// src/middleware/ownership.js
import prisma from '../config/prisma.js';

export async function validateSessionAccess(userId, sessionId) {
  const session = await prisma.lectureSession.findFirst({
    where: {
      id: sessionId,
      subject: { userId }
    },
    select: { id: true, subjectId: true, status: true }
  });

  return session; // Returns session if authorized, null otherwise
}