const Redis = require('ioredis');
const { redisUrl } = require('./env');
const logger = require('../utils/logger');

let client;

function getRedisClient() {
  if (!client) {
    client = new Redis(redisUrl, {
      maxRetriesPerRequest: 3,
      retryStrategy: (times) => Math.min(times * 200, 5000),
    });
    client.on('error', (err) => logger.error('Redis error', { error: err.message }));
    client.on('connect', () => logger.info('Redis connected'));
  }
  return client;
}

module.exports = { getRedisClient };
