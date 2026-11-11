// src/services/questionAnsweringService.js
import prisma from '../config/prisma.js';
import { GoogleGenerativeAI } from '@google/generative-ai';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

export async function answerStudentQuestion({ sessionId, subjectId, question, clientWs }) {
    // 1. Generate Question Embedding
    const embedModel = genAI.getGenerativeModel({ model: "text-embedding-004" });
    const embedResult = await embedModel.embedContent(question);
    const qVector = JSON.stringify(embedResult.embedding.values);

    // 2. Search A: Current Session Notes (Top 3)
    const currentSessionChunks = await prisma.$queryRaw`
    SELECT id, heading, content, 'CURRENT_LECTURE' as source
    FROM "NoteChunk"
    WHERE "sessionId" = ${sessionId}
      AND embedding IS NOT NULL
    ORDER BY embedding <=> ${qVector}::vector
    LIMIT 3;
  `;

    // 3. Search B: Historical Completed Sessions under same Subject (Top 3)
    const historicalChunks = await prisma.$queryRaw`
    SELECT nc.id, nc.heading, nc.content, ls.title as "sessionTitle", 'HISTORICAL_SUBJECT' as source
    FROM "NoteChunk" nc
    JOIN "LectureSession" ls ON nc."sessionId" = ls.id
    WHERE ls."subjectId" = ${subjectId}
      AND ls.id != ${sessionId}
      AND ls.status = 'COMPLETED'
      AND nc.embedding IS NOT NULL
      AND (1 - (nc.embedding <=> ${qVector}::vector)) > 0.65
    ORDER BY nc.embedding <=> ${qVector}::vector
    LIMIT 3;
  `;

    // 4. Combine and Deduplicate
    const contextList = [
        ...currentSessionChunks.map(c => `[Current Lecture Context - ${c.heading}]: ${c.content}`),
        ...historicalChunks.map(c => `[Past Session "${c.sessionTitle}" - ${c.heading}]: ${c.content}`)
    ];

    const assembledContext = contextList.join('\n\n');

    // 5. Stream Gemini Response
    const model = genAI.getGenerativeModel({ model: 'gemini-1.5-pro' });
    const prompt = `
You are NoteMate AI assistant. Answer the student's question accurately based on the provided current and historical context.

CONTEXT:
${assembledContext || "No relevant prior context found."}

STUDENT QUESTION:
"${question}"

Provide a clear, formatted explanation. Distinctly clarify if a reference comes from previous sessions vs. the current lecture.
  `;

    const streamResult = await model.generateContentStream(prompt);

    for await (const chunk of streamResult.stream) {
        const chunkText = chunk.text();
        if (clientWs && clientWs.readyState === 1) {
            clientWs.send(JSON.stringify({ type: 'QA_ANSWER_CHUNK', text: chunkText }));
        }
    }

    if (clientWs && clientWs.readyState === 1) {
        clientWs.send(JSON.stringify({ type: 'QA_ANSWER_END' }));
    }
}