// src/services/transcriptionService.js
import WebSocket from 'ws';
import { redisClient } from '../config/redis.js';
import prisma from '../config/prisma.js';

export class LiveTranscriptionManager {
    constructor(sessionId, clientWs) {
        this.sessionId = sessionId;
        this.clientWs = clientWs;
        this.sarvamWs = null;
        this.sequenceNumber = 0;
    }

    async initialize() {
        // 1. Establish Sarvam Real-time STT WebSocket Connection
        this.sarvamWs = new WebSocket('wss://api.sarvam.ai/v1/live-transcript', {
            headers: { 'api-subscription-key': process.env.SARVAM_API_KEY }
        });

        this.sarvamWs.on('open', () => {
            this.sarvamWs.send(JSON.stringify({
                config: {
                    language_code: "hi-IN", // Code-mix / Hinglish support
                    model: "saarika:v2",
                    sample_rate: 16000
                }
            }));
        });

        this.sarvamWs.on('message', async (data) => {
            const response = JSON.parse(data.toString());

            if (response.type === 'partial') {
                // Forward partial transcript to frontend for instant feedback
                this.clientWs.send(JSON.stringify({ type: 'TRANSCRIPT_PARTIAL', text: response.transcript }));
            } else if (response.type === 'final' && response.transcript.trim().length > 0) {
                this.sequenceNumber++;
                const text = response.transcript.trim();

                // 2. Persist TranscriptChunk as PENDING
                const chunk = await prisma.transcriptChunk.create({
                    data: {
                        sessionId: this.sessionId,
                        sequence: this.sequenceNumber,
                        text,
                        status: 'PENDING'
                    }
                });

                // 3. Emit final transcript to frontend
                this.clientWs.send(JSON.stringify({ type: 'TRANSCRIPT_FINAL', chunk }));

                // 4. Buffer into Redis Stream for batch note generation
                await redisClient.xadd(
                    `stream:transcripts:${this.sessionId}`,
                    '*',
                    'chunkId', chunk.id,
                    'text', text,
                    'sequence', this.sequenceNumber.toString()
                );
            }
        });
    }

    handleAudioChunk(buffer) {
        if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN) {
            this.sarvamWs.send(buffer);
        }
    }

    async close() {
        if (this.sarvamWs) this.sarvamWs.close();
    }
}