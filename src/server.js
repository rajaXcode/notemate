import 'dotenv/config';
import http from 'http';
import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import { handleQuery } from './controllers/queryController.js';
import { login, register } from './controllers/authcontroller.js';
import { authenticateToken } from './middleware/auth.js';
import { initWebSocketServer } from './services/websocketService.js';

const app = express();

app.use(cors({ origin: '*' }));
app.use(express.json());
app.use(morgan('dev'));

// Health Check
app.get('/', (req, res) => {
    res.send("APIs running health");
});

// Authentication Routes
app.post('/api/auth/register', register);
app.post('/api/auth/login', login);

// RAG Query Endpoint
app.post('/api/query', authenticateToken, handleQuery);

const server = http.createServer(app);

// Attach WebSocket Handling
initWebSocketServer(server);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
    console.log(`Backend Server running on port ${PORT}`);
});