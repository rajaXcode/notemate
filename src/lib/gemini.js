import { GoogleGenAI, Type } from '@google/genai';

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
const MODEL_NAME = 'gemini-3.5-flash';
const EMBEDDING_MODEL = 'text-embedding-004';

export async function generateDynamicNotes(currentNotes, newTranscript) {
    const prompt = `
        You are an expert academic note-taker maintaining a living markdown document.

        CURRENT NOTES:
        ${currentNotes || "(Document is empty. Start a new set of notes.)"}

        NEW RAW TRANSCRIPT (Hinglish):
        ${newTranscript}

        INSTRUCTIONS:
        1. Seamlessly integrate the NEW RAW TRANSCRIPT into the CURRENT NOTES.
        2. Do not just append to the bottom. Update existing section headers or add new ones logically.
        3. Remove filler words, correct grammar, and translate Hinglish concepts into clean, professional academic English.
        4. Output ONLY the updated complete Markdown document.
`;

    const response = await ai.models.generateContent({
        model: MODEL_NAME,
        contents: prompt,
    });

    return response.text;
}

export async function generateGeminiEmbedding(text) {
    const response = await ai.models.embedContent({
        model: EMBEDDING_MODEL,
        contents: text,
    });
    return response.embedding.values;
}

export async function segmentAndRouteNotes(masterDocumentText) {
    const response = await ai.models.generateContent({
        model: MODEL_NAME,
        contents: `Analyze this classroom document. Split it into distinct sections based on the core topic taught (e.g., past review topics vs today's main lesson).\n\nDOCUMENT:\n${masterDocumentText}`,
        config: {
            responseMimeType: 'application/json',
            responseSchema: {
                type: Type.ARRAY,
                description: 'List of distinct topic sections extracted from the document',
                items: {
                    type: Type.OBJECT,
                    properties: {
                        topicName: { type: Type.STRING, description: 'Topic name (e.g. Gravitation, Newton Laws)' },
                        isReviewContent: { type: Type.BOOLEAN, description: 'True if this was a review/recap of a past chapter' },
                        contentMarkdown: { type: Type.STRING, description: 'The Markdown notes for this topic' }
                    },
                    required: ['topicName', 'isReviewContent', 'contentMarkdown']
                }
            }
        }
    });

    return JSON.parse(response.text);
}

export async function* answerQueryWithContextStream(context, question) {
    const prompt = `
    You are a helpful teaching assistant. Use ONLY the provided classroom notes to answer the student's question. 
    If the answer cannot be inferred from the notes, answer based on your knowledge.

    CONTEXT (Classroom Notes):
    ${context}

    STUDENT QUESTION:
    ${question}
  `;

    const responseStream = await ai.models.generateContentStream({
        model: MODEL_NAME,
        contents: prompt,
    });

    for await (const chunk of responseStream) {
        yield chunk.text;
    }
}