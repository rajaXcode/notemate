// src/services/geminiNoteEngine.js
import { GoogleGenerativeAI, SchemaType } from '@google/generative-ai';
import prisma from '../config/prisma.js';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const noteResponseSchema = {
    type: SchemaType.ARRAY,
    description: "Operations to maintain lecture notes incrementally.",
    items: {
        type: SchemaType.OBJECT,
        properties: {
            action: { type: SchemaType.STRING, description: "CREATE, UPDATE, or IGNORE" },
            noteChunkId: { type: SchemaType.STRING, description: "Existing block ID if UPDATE, null if CREATE" },
            heading: { type: SchemaType.STRING },
            content: { type: SchemaType.STRING, description: "Markdown text for the note block" },
            sortOrder: { type: SchemaType.INTEGER },
            expectedVersion: { type: SchemaType.INTEGER, description: "Version of noteChunkId when UPDATE" }
        },
        required: ["action", "heading", "content"]
    }
};

export async function generateIncrementalNotes(sessionId, newTranscriptText) {
    // 1. Load lightweight outline of existing note blocks for this session
    const existingChunks = await prisma.noteChunk.findMany({
        where: { sessionId },
        select: { id: true, heading: true, content: true, sortOrder: true, version: true },
        orderBy: { sortOrder: 'asc' }
    });

    const outline = existingChunks.map(c => ({
        id: c.id,
        heading: c.heading,
        summary: c.content.substring(0, 150),
        version: c.version,
        sortOrder: c.sortOrder
    }));

    const model = genAI.getGenerativeModel({
        model: 'gemini-3.5-flash',
        generationConfig: {
            responseMimeType: 'application/json',
            responseSchema: noteResponseSchema,
            temperature: 0.2
        }
    });

    const prompt = `
You are an academic note generator. Update structured Markdown notes for a live lecture session.

CURRENT NOTE OUTLINE:
${JSON.stringify(outline, null, 2)}

NEW TRANSCRIPT BATCH:
"${newTranscriptText}"

INSTRUCTIONS:
1. Review the new transcript batch.
2. Decide whether to:
   - "CREATE": Add a new concept as a new NoteChunk block.
   - "UPDATE": Append or improve an existing NoteChunk identified by its ID. Preserve all previously written information; integrate new insights cleanly.
   - "IGNORE": Skip non-academic conversation, repetition, or administrative banter.
3. Return ONLY structured operations matching the required JSON schema.
  `;

    const result = await model.generateContent(prompt);
    const operations = JSON.parse(result.response.text());
    const changedBlocks = [];

    for (const op of operations) {
        if (op.action === 'CREATE') {
            const maxOrder = existingChunks.length > 0 ? Math.max(...existingChunks.map(e => e.sortOrder)) : 0;
            const created = await prisma.noteChunk.create({
                data: {
                    sessionId,
                    heading: op.heading,
                    content: op.content,
                    sortOrder: op.sortOrder ?? maxOrder + 1,
                    version: 1,
                    embeddingVersion: 0
                }
            });
            changedBlocks.push(created);
        } else if (op.action === 'UPDATE' && op.noteChunkId) {
            // Version check to prevent race conditions
            const target = existingChunks.find(e => e.id === op.noteChunkId);
            if (target) {
                const updated = await prisma.noteChunk.update({
                    where: {
                        id: op.noteChunkId,
                        version: op.expectedVersion ?? target.version
                    },
                    data: {
                        heading: op.heading,
                        content: op.content,
                        version: { increment: 1 }
                    }
                });
                changedBlocks.push(updated);
            }
        }
    }

    return changedBlocks;
}