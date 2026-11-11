import prisma from '../config/prisma.js';
import { finalizeLectureSession } from '../services/sessionFinalizer.js';

export async function createSession(req, res) {
    const { subjectId } = req.params;
    const { title } = req.body;

    const subject = await prisma.subject.findFirst({
        where: { id: subjectId, userId: req.user.id }
    });

    if (!subject) {
        return res.status(404).json({ error: 'Subject not found or unauthorized' });
    }

    const session = await prisma.lectureSession.create({
        data: {
            subjectId,
            title: title || `Lecture - ${new Date().toLocaleDateString()}`,
            status: 'LIVE'
        }
    });

    res.status(201).json({ success: true, data: session });
}

export async function endSession(req, res) {
    const { sessionId } = req.params;

    const session = await prisma.lectureSession.findFirst({
        where: { id: sessionId, subject: { userId: req.user.id } }
    });

    if (!session) {
        return res.status(404).json({ error: 'Session not found or unauthorized' });
    }

    // Trigger background finalization and vector embedding pipeline
    finalizeLectureSession(sessionId).catch((err) =>
        console.error(`Finalization error for session ${sessionId}:`, err)
    );

    res.json({ success: true, message: 'Session status set to ending; background embedding queued' });
}