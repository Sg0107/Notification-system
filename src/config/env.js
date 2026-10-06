require('dotenv').config();

module.exports = {
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9093').split(','),
    clientId: process.env.KAFKA_CLIENT_ID || 'notification-system',
  },
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6380',
  idempotencyTtlSeconds: Number(process.env.IDEMPOTENCY_TTL_SECONDS) || 86400, // 24h default
};
