import http from 'http';
import express from 'express';
import cors from 'cors';
import { handleQuery } from './controller/queryController.js';
import { login, register } from './controller/authcontroller.js';
import { authenticateToken } from './middleware/auth.js';
import { initWebSocketServer } from './services/websocketService.js';
import dotenv from 'dotenv'
import morgan from 'morgan';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());
app.use(morgan('dev'));

//auth
app.post('/api/auth/register', register);
app.post('/api/auth/login', login);

// RAG Endpoint
app.post('/api/query', authenticateToken, handleQuery);

const server = http.createServer(app);

// Attach WebSockets
initWebSocketServer(server);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
    console.log(`Backend Server running on port ${PORT}`);
});