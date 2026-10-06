const Redis = require('ioredis');
const { redisUrl } = require('./env');

// Same pattern as the Kafka client: one shared connection object, reused
// everywhere via require() caching, instead of opening a new connection
// per request.
const redisClient = new Redis(redisUrl);

redisClient.on('error', (err) => console.error('Redis error:', err.message));
redisClient.on('connect', () => console.log('Redis connected'));

module.exports = { redisClient };
