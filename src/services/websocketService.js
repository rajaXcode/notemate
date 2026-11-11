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
    'wss://api.sarvam.ai/speech-to-text-realtime/ws?language_code=hi-IN&mode=codemix&stream_type=balanced';

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

        // Browser sends raw PCM chunks -> Sarvam wants base64 inside a JSON event
        clientWs.on('message', (audioChunk) => {
            if (sarvamWs.readyState === WebSocket.OPEN) {
                sarvamWs.send(JSON.stringify({
                    event: 'audio_input',
                    audio: audioChunk.toString('base64')
                }));
            }
        });

        sarvamWs.on('message', (data) => {
            const msg = JSON.parse(data.toString());

            if (msg.event === 'transcript.partial' || msg.event === 'transcript.final') {
                const isFinal = msg.event === 'transcript.final';
                clientWs.send(JSON.stringify({ type: 'transcript', text: msg.text, isFinal }));

                // only final transcripts feed the notes (redis commands stay ordered on one connection)
                if (isFinal && msg.text) redis.rpush(transcriptKey, msg.text);
            } else if (msg.event === 'error') {
                console.error(`Sarvam error (${msg.code}): ${msg.message}`);
            }
        });

        // keep Sarvam from closing on inactivity
        const pingInterval = setInterval(() => {
            if (sarvamWs.readyState === WebSocket.OPEN) {
                sarvamWs.send(JSON.stringify({ event: 'ping' }));
            }
        }, 15000);

        // Fold pending transcripts into the master notes.
        // Calls are chained so the 60s tick and the final flush on close never overlap.
        async function updateNotes() {
            const newTranscripts = await redis.lrange(transcriptKey, 0, -1);
            if (newTranscripts.length === 0) return;

            await redis.ltrim(transcriptKey, newTranscripts.length, -1); // remove only what we read
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

        // Ask Sarvam to finish (so the last utterance's final transcript still arrives), then close
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

        // Stop recording & post-process notes into Prisma pgvector
        clientWs.on('close', async () => {
            clearInterval(noteInterval);
            clearInterval(pingInterval);

            await closeSarvam();
            await enqueueUpdate(); // flush transcripts not yet in the notes

            const finalDocument = await redis.get(notesKey);
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
                    await redis.del(notesKey);
                }
            }
        });
    });
}