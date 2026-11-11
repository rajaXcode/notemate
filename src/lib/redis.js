import Redis from 'ioredis';

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

const isSecure = redisUrl.startsWith('rediss://');

export const redis = new Redis(redisUrl, {
    tls: isSecure ? { rejectUnauthorized: false } : undefined,
    maxRetriesPerRequest: 3,
    retryStrategy(times) {
        const delay = Math.min(times * 50, 2000);
        return delay;
    }
});

redis.on('connect', () => {
    console.log('[Upstash Redis] Successfully connected to cloud instance.');
});

redis.on('error', (err) => {
    console.error('[Upstash Redis Error]', err.message);
});