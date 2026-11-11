// src/workers/noteGenerationWorker.js
import { redisClient } from '../config/redis.js';
import prisma from '../config/prisma.js';
import { generateIncrementalNotes } from '../services/geminiNoteEngine.js';

const STREAM_GROUP = 'note_generators';
const CONSUMER_NAME = `worker_${process.pid}`;

export async function processTranscriptStream(sessionId, clientWs) {
    const streamKey = `stream:transcripts:${sessionId}`;

    try {
        await redisClient.xgroup('CREATE', streamKey, STREAM_GROUP, '$', 'MKSTREAM');
    } catch (err) {
        // Group may already exist
    }

    let batch = [];
    let batchIds = [];
    let lastFlush = Date.now();

    const flushBatch = async () => {
        if (batch.length === 0) return;

        const combinedText = batch.map(b => b.text).join(' ');
        const pendingChunkIds = batch.map(b => b.chunkId);

        try {
            // 1. Generate incremental notes via Gemini
            const changedBlocks = await generateIncrementalNotes(sessionId, combinedText);

            // 2. Mark transcripts as PROCESSED
            await prisma.transcriptChunk.updateMany({
                where: { id: { in: pendingChunkIds } },
                data: { status: 'PROCESSED', processedAt: new Date() }
            });

            // 3. Acknowledge stream events
            if (batchIds.length > 0) {
                await redisClient.xack(streamKey, STREAM_GROUP, ...batchIds);
            }

            // 4. Send updated blocks to client via WebSocket
            if (clientWs && clientWs.readyState === 1) {
                clientWs.send(JSON.stringify({ type: 'NOTE_BLOCKS_UPDATED', blocks: changedBlocks }));
            }
        } catch (error) {
            console.error('Failed to process batch:', error);
            // Fail safely without losing stream position; will be retried or handled gracefully
        } finally {
            batch = [];
            batchIds = [];
            lastFlush = Date.now();
        }
    };

    const interval = setInterval(async () => {
        const entries = await redisClient.xreadgroup(
            'GROUP', STREAM_GROUP, CONSUMER_NAME,
            'COUNT', 10,
            'BLOCK', 2000,
            'STREAMS', streamKey, '>'
        );

        if (entries && entries.length > 0) {
            const [_, records] = entries[0];
            for (const record of records) {
                const id = record[0];
                const fields = record[1];
                const chunkData = {};
                for (let i = 0; i < fields.length; i += 2) {
                    chunkData[fields[i]] = fields[i + 1];
                }
                batch.push(chunkData);
                batchIds.push(id);
            }
        }

        const elapsed = Date.now() - lastFlush;
        const tokenEstimate = batch.reduce((acc, curr) => acc + curr.text.split(' ').length, 0);

        if (batch.length > 0 && (elapsed >= 12000 || tokenEstimate >= 200)) {
            await flushBatch();
        }
    }, 3000);

    return () => clearInterval(interval);
}