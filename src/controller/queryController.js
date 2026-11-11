import { prisma } from '../lib/prisma.js';
import { generateGeminiEmbedding, answerQueryWithContextStream } from '../lib/gemini.js';

export async function handleQuery(req, res) {
    const { question, notebookId } = req.body;
    const userId = req.user.id;

    try {
        const queryVector = await generateGeminiEmbedding(question);
        const vectorString = `[${queryVector.join(',')}]`;

        // Retrieve matching note chunks via vector similarity
        const matchedChunks = await prisma.$queryRaw`
      SELECT nc.content, 1 - (nc.embedding <=> ${vectorString}::vector) AS similarity
      FROM "NoteChunk" nc
      JOIN "Notebook" n ON nc."notebookId" = n.id
      WHERE n."userId" = ${userId}
        ${notebookId ? prisma.raw(`AND n.id = '${notebookId}'`) : prisma.empty}
        AND 1 - (nc.embedding <=> ${vectorString}::vector) > 0.5
      ORDER BY nc.embedding <=> ${vectorString}::vector
      LIMIT 3;
    `;

        if (!matchedChunks || matchedChunks.length === 0) {
            return res.json({ answer: 'No relevant information found in your lecture notes.' });
        }

        const context = matchedChunks.map(c => c.content).join('\n\n---\n\n');
        const answer = await answerQueryWithContextStream(context, question);

        res.json({ answer, retrievedChunks: matchedChunks.length });
    } catch (error) {
        console.error('RAG Query Error:', error);
        res.status(500).json({ error: 'Failed to process lecture question.' });
    }
}