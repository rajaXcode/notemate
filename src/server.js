import 'dotenv/config';
import http from 'http';
import express from 'express';
import cors from 'cors';
import { handleQuery } from './controller/queryController.js';
import { login, register } from './controller/authcontroller.js';
import { authenticateToken } from './middleware/auth.js';
import { initWebSocketServer } from './services/websocketService.js';
import morgan from 'morgan';


const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json());
app.use(morgan('dev'));

//health
app.get('/', (req, res) => {
    res.send("APIs running health");
})
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