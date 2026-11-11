import { WebSocketServer, WebSocket } from 'ws';
import { prisma } from '../lib/prisma.js';
import { redis } from '../lib/redis.js';
import { authenticateWs } from '../middleware/auth.js';
import {
    generateDynamicNotes,
    segmentAndRouteNotes,
    generateGeminiEmbedding
} from '../lib/gemini.js';

const SARVAM_URL =
    'wss://api.sarvam.ai/speech-to-text-realtime/ws?language_code=en-IN&mode=codemix&stream_type=balanced';

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

        const transcriptKey = `transcripts:${sessionId}`;
        const notesKey = `master_notes:${sessionId}`;

        const sarvamWs = new WebSocket(SARVAM_URL, {
            headers: { 'Api-Subscription-Key': process.env.SARVAM_API_KEY }
        });

        sarvamWs.on('error', (err) => console.error('Sarvam WS error:', err.message));
        clientWs.on('error', (err) => console.error('Client WS error:', err.message));

        // Accept raw audio buffer chunks and relay as base64 JSON to Sarvam
        clientWs.on('message', (data) => {
            if (sarvamWs.readyState === WebSocket.OPEN) {
                const base64Audio = Buffer.isBuffer(data)
                    ? data.toString('base64')
                    : Buffer.from(data).toString('base64');

                sarvamWs.send(JSON.stringify({
                    event: 'audio_input',
                    audio: base64Audio
                }));
            }
        });

        sarvamWs.on('message', (data) => {
            try {
                const msg = JSON.parse(data.toString());

                if (msg.event === 'transcript.partial' || msg.event === 'transcript.final') {
                    const isFinal = msg.event === 'transcript.final';

                    if (clientWs.readyState === WebSocket.OPEN) {
                        clientWs.send(JSON.stringify({ type: 'transcript', text: msg.text, isFinal }));
                    }

                    // Only push final transcripts to Redis
                    if (isFinal && msg.text) {
                        redis.rpush(transcriptKey, msg.text);
                    }
                } else if (msg.event === 'error') {
                    console.error(`Sarvam API error (${msg.code}): ${msg.message}`);
                }
            } catch (err) {
                console.error('Error parsing Sarvam payload:', err.message);
            }
        });

        // Prevent connection timeout with keep-alive pings
        const pingInterval = setInterval(() => {
            if (sarvamWs.readyState === WebSocket.OPEN) {
                sarvamWs.send(JSON.stringify({ event: 'ping' }));
            }
        }, 15000);

        // Atomic transcript read & note batch generation
        async function updateNotes() {
            // Atomic LRANGE + DEL via pipeline to prevent losing concurrent transcripts
            const pipeline = redis.pipeline();
            pipeline.lrange(transcriptKey, 0, -1);
            pipeline.del(transcriptKey);
            const results = await pipeline.exec();

            const newTranscripts = results?.[0]?.[1] || [];
            if (newTranscripts.length === 0) return;

            const currentNotes = (await redis.get(notesKey)) || '';
            const updatedNotes = await generateDynamicNotes(currentNotes, newTranscripts.join(' '));

            await redis.set(notesKey, updatedNotes);

            if (clientWs.readyState === WebSocket.OPEN) {
                clientWs.send(JSON.stringify({ type: 'notes_update', payload: updatedNotes }));
            }
        }

        let chain = Promise.resolve();
        const enqueueUpdate = () =>
            (chain = chain.then(updateNotes).catch((err) => console.error('Notes update error:', err)));

        // 60-second rolling notes update loop
        const noteInterval = setInterval(enqueueUpdate, 60000);

        function closeSarvam() {
            return new Promise((resolve) => {
                if (sarvamWs.readyState === WebSocket.CLOSED) return resolve();
                sarvamWs.once('close', resolve);
                setTimeout(resolve, 5000);

                if (sarvamWs.readyState === WebSocket.OPEN) {
                    sarvamWs.send(JSON.stringify({ event: 'end' }));
                } else {
                    sarvamWs.terminate();
                }
            }).then(() => sarvamWs.terminate());
        }

        // Post-session cleanup and PgVector sync
        clientWs.on('close', async () => {
            clearInterval(noteInterval);
            clearInterval(pingInterval);

            await closeSarvam();
            await enqueueUpdate(); // Flush final remaining transcripts into notes

            try {
                const finalDocument = await redis.get(notesKey);
                if (finalDocument) {
                    const sections = await segmentAndRouteNotes(finalDocument);

                    for (const section of sections) {
                        const vectorArray = await generateGeminiEmbedding(
                            `${section.topicName}: ${section.contentMarkdown.slice(0, 300)}`
                        );
                        const vectorString = `[${vectorArray.join(',')}]`;

                        // Fixed SQL syntax for PGVector cosine similarity matching
                        const matches = await prisma.$queryRaw`
                            SELECT id, "topicName", (1 - ("topicEmbedding" <=> ${vectorString}::vector)) AS similarity
                            FROM "Notebook"
                            WHERE "userId" = ${user.id}
                              AND (1 - ("topicEmbedding" <=> ${vectorString}::vector)) > 0.82
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
                                    subject: 'General',
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
                }
            } catch (err) {
                console.error('Error during post-session processing:', err);
            } finally {
                await redis.del(notesKey);
                await redis.del(transcriptKey);
            }
        });
    });
}