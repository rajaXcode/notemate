import { prisma } from '../lib/prisma.js';
import { Prisma } from '@prisma/client';
import {
    generateGeminiEmbedding,
    answerQueryWithContextStream
} from '../lib/gemini.js';

export async function handleQuery(req, res) {
    const { question, notebookId } = req.body;
    const userId = req.user.id;

    try {
        if (!question || typeof question !== 'string' || !question.trim()) {
            return res.status(400).json({
                error: 'Question is required.'
            });
        }

        // Generate vector embedding for the student's question
        const queryVector = await generateGeminiEmbedding(question);
        if (!queryVector || !Array.isArray(queryVector)) {
            return res.status(500).json({
                error: 'Failed to generate embedding for the question.'
            });
        }

        const vectorString = `[${queryVector.join(',')}]`;

        let currentNotebookChunks = [];
        let previousKnowledgeChunks = [];

        if (notebookId) {
            // 1. Primary Retrieval: Chunks from the active target notebook
            currentNotebookChunks = await prisma.$queryRaw(
                Prisma.sql`
                    SELECT
                        nc.content,
                        n."topicName",
                        'CURRENT' AS source,
                        (1 - (nc.embedding <=> ${vectorString}::vector)) AS similarity
                    FROM "NoteChunk" nc
                    JOIN "Notebook" n ON nc."notebookId" = n.id
                    WHERE
                        n.id = ${notebookId}
                        AND n."userId" = ${userId}
                        AND nc.embedding IS NOT NULL
                        AND (1 - (nc.embedding <=> ${vectorString}::vector)) > 0.45
                    ORDER BY nc.embedding <=> ${vectorString}::vector
                    LIMIT 3;
                `
            );

            // 2. Previous Knowledge Retrieval: Top matching chunks from past notebooks
            previousKnowledgeChunks = await prisma.$queryRaw(
                Prisma.sql`
                    SELECT
                        nc.content,
                        n."topicName",
                        'PREVIOUS_KNOWLEDGE' AS source,
                        (1 - (nc.embedding <=> ${vectorString}::vector)) AS similarity
                    FROM "NoteChunk" nc
                    JOIN "Notebook" n ON nc."notebookId" = n.id
                    WHERE
                        n."userId" = ${userId}
                        AND n.id != ${notebookId}
                        AND nc.embedding IS NOT NULL
                        AND (1 - (nc.embedding <=> ${vectorString}::vector)) > 0.50
                    ORDER BY nc.embedding <=> ${vectorString}::vector
                    LIMIT 3;
                `
            );
        } else {
            // Global Search: No notebookId specified, query all past & present notebooks
            currentNotebookChunks = await prisma.$queryRaw(
                Prisma.sql`
                    SELECT
                        nc.content,
                        n."topicName",
                        'GLOBAL' AS source,
                        (1 - (nc.embedding <=> ${vectorString}::vector)) AS similarity
                    FROM "NoteChunk" nc
                    JOIN "Notebook" n ON nc."notebookId" = n.id
                    WHERE
                        n."userId" = ${userId}
                        AND nc.embedding IS NOT NULL
                        AND (1 - (nc.embedding <=> ${vectorString}::vector)) > 0.50
                    ORDER BY nc.embedding <=> ${vectorString}::vector
                    LIMIT 5;
                `
            );
        }

        const totalRetrieved = currentNotebookChunks.length + previousKnowledgeChunks.length;

        if (totalRetrieved === 0) {
            return res.json({
                answer: 'No relevant information found in your current or previous lecture notes.',
                retrievedChunks: 0
            });
        }

        // Context assembly with explicit tags so Gemini knows the source
        const formattedContext = [
            ...currentNotebookChunks.map(
                c => `[Current Lecture Topic: ${c.topicName}]\n${c.content}`
            ),
            ...previousKnowledgeChunks.map(
                c => `[Previous Knowledge Topic: ${c.topicName}]\n${c.content}`
            )
        ].join('\n\n---\n\n');

        // Stream or generate final response with Gemini
        const answer = await answerQueryWithContextStream(
            formattedContext,
            question
        );

        return res.json({
            answer,
            retrievedChunks: {
                currentNotebook: currentNotebookChunks.length,
                previousKnowledge: previousKnowledgeChunks.length,
                total: totalRetrieved
            }
        });

    } catch (error) {
        console.error('RAG Query Error:', error);

        return res.status(500).json({
            error: 'Failed to process lecture question.'
        });
    }
}