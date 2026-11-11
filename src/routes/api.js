import { Router } from 'express';
import { authenticateToken } from '../middleware/auth.js';
import { getSubjects, createSubject, deleteSubject } from '../controllers/subjectController.js';
import { createSession, endSession } from '../controllers/sessionController.js';

const router = Router();

router.use(authenticateToken);

// Subject Routes
router.get('/subjects', getSubjects);
router.post('/subjects', createSubject);
router.delete('/subjects/:subjectId', deleteSubject);

// Session Routes
router.post('/subjects/:subjectId/sessions', createSession);
router.post('/sessions/:sessionId/end', endSession);

export default router;