// src/services/sessionFinalizer.js
import prisma from '../config/prisma.js';
import { GoogleGenerativeAI } from '@google/generative-ai';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

export async function finalizeLectureSession(sessionId) {
    // 1. Set Status to PROCESSING
    await prisma.lectureSession.update({
        where: { id: sessionId },
        data: { status: 'PROCESSING' }
    });

    // 2. Fetch all chunks needing embedding generation/refresh
    const chunksToEmbed = await prisma.noteChunk.findMany({
        where: {
            sessionId,
            OR: [
                { embeddingVersion: 0 },
                { embeddingVersion: { lt: prisma.noteChunk.fields.version } }
            ]
        }
    });

    const embedModel = genAI.getGenerativeModel({ model: "text-embedding-004" });

    for (const chunk of chunksToEmbed) {
        const textToEmbed = `${chunk.heading}\n${chunk.content}`;
        const result = await embedModel.embedContent(textToEmbed);
        const vector = result.embedding.values; // 768 float array

        // Execute raw SQL update to write vector data into pgvector column
        await prisma.$executeRaw`
      UPDATE "NoteChunk"
      SET 
        embedding = ${JSON.stringify(vector)}::vector,
        "embeddingVersion" = version,
        "updatedAt" = NOW()
      WHERE id = ${chunk.id};
    `;
    }

    // 3. Mark Session COMPLETED
    const finalizedSession = await prisma.lectureSession.update({
        where: { id: sessionId },
        data: {
            status: 'COMPLETED',
            endedAt: new Date()
        }
    });

    return finalizedSession;
}