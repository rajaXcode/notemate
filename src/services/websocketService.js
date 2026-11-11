import { WebSocketServer, WebSocket } from 'ws';
import Redis from 'ioredis';
import { prisma } from '../lib/prisma.js';
import { authenticateWs } from '../middleware/auth.js';
import {
    generateDynamicNotes,
    segmentAndRouteNotes,
    generateGeminiEmbedding
} from '../lib/gemini.js';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

export function initWebSocketServer(server) {
    const wss = new WebSocketServer({ noServer: true });

    server.on('upgrade', (request, socket, head) => {
        const user = authenticateWs(request);
        if (!user) {
            socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
            socket.destroy();
            return;
        }

        wss.handleUpgrade(request, socket, head, (ws) => {
            wss.emit('connection', ws, request, user);
        });
    });

    wss.on('connection', (clientWs, req, user) => {
        const sessionId = `session_${Date.now()}_${user.id}`;
        console.log(`User ${user.id} connected. Session: ${sessionId}`);

        // Connect to Sarvam STT WebSocket
        const sarvamWs = new WebSocket('wss://api.sarvam.ai/speech-to-text/ws', {
            headers: { 'Authorization': `Bearer ${process.env.SARVAM_API_KEY}` }
        });

        sarvamWs.on('open', () => {
            clientWs.on('message', (audioChunk) => {
                if (sarvamWs.readyState === WebSocket.OPEN) {
                    sarvamWs.send(audioChunk);
                }
            });
        });

        sarvamWs.on('message', async (data) => {
            const response = JSON.parse(data.toString());
            clientWs.send(JSON.stringify({ type: 'transcript', text: response.text }));

            if (response.is_final && response.text) {
                await redis.rpush(`transcripts:${sessionId}`, response.text);
            }
        });

        // 60-second rolling notes update loop
        const noteInterval = setInterval(async () => {
            const newTranscripts = await redis.lrange(`transcripts:${sessionId}`, 0, -1);
            if (newTranscripts.length > 0) {
                await redis.del(`transcripts:${sessionId}`);
                const transcriptText = newTranscripts.join(' ');

                const currentNotes = (await redis.get(`master_notes:${sessionId}`)) || '';
                const updatedNotes = await generateDynamicNotes(currentNotes, transcriptText);

                await redis.set(`master_notes:${sessionId}`, updatedNotes);
                clientWs.send(JSON.stringify({ type: 'notes_update', payload: updatedNotes }));
            }
        }, 60000);

        // Stop recording & post-process notes into Prisma pgvector
        clientWs.on('close', async () => {
            sarvamWs.close();
            clearInterval(noteInterval);

            const finalDocument = await redis.get(`master_notes:${sessionId}`);
            if (finalDocument) {
                try {
                    const sections = await segmentAndRouteNotes(finalDocument);

                    for (const section of sections) {
                        const vectorArray = await generateGeminiEmbedding(
                            `${section.topicName}: ${section.contentMarkdown.slice(0, 300)}`
                        );
                        const vectorString = `[${vectorArray.join(',')}]`;

                        // Query existing notebooks using cosine similarity
                        const matches = await prisma.$queryRaw`
              SELECT id, "topicName", 1 - ("topicEmbedding" <=> ${vectorString}::vector) AS similarity
              FROM "Notebook"
              WHERE "userId" = ${user.id}
                AND 1 - ("topicEmbedding" <=> ${vectorString}::vector) > 0.82
              ORDER BY "topicEmbedding" <=> ${vectorString}::vector
              LIMIT 1;
            `;

                        let targetNotebookId;

                        if (matches && matches.length > 0 && section.isReviewContent) {
                            targetNotebookId = matches[0].id;
                        } else {
                            const newNotebook = await prisma.notebook.create({
                                data: {
                                    userId: user.id,
                                    subject: 'Physics',
                                    topicName: section.topicName,
                                }
                            });

                            await prisma.$executeRaw`
                UPDATE "Notebook"
                SET "topicEmbedding" = ${vectorString}::vector
                WHERE id = ${newNotebook.id};
              `;

                            targetNotebookId = newNotebook.id;
                        }

                        const chunkVectorArray = await generateGeminiEmbedding(section.contentMarkdown);
                        const chunkVectorString = `[${chunkVectorArray.join(',')}]`;

                        const createdChunk = await prisma.noteChunk.create({
                            data: {
                                notebookId: targetNotebookId,
                                sessionId: sessionId,
                                content: section.contentMarkdown
                            }
                        });

                        await prisma.$executeRaw`
              UPDATE "NoteChunk"
              SET embedding = ${chunkVectorString}::vector
              WHERE id = ${createdChunk.id};
            `;
                    }
                } catch (err) {
                    console.error('Error during post-session processing:', err);
                } finally {
                    await redis.del(`master_notes:${sessionId}`);
                }
            }
        });
    });
}