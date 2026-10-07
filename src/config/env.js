require('dotenv').config();

module.exports = {
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9093').split(','),
    clientId: process.env.KAFKA_CLIENT_ID || 'notification-system',
  },
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6380',
  idempotencyTtlSeconds: Number(process.env.IDEMPOTENCY_TTL_SECONDS) || 86400, // 24h default
  // Token bucket capacity/refill rate per channel, in requests per second.
  // Deliberately low defaults for push since that's usually the
  // highest-volume, least rate-sensitive channel in real systems; SMS is
  // kept lowest since SMS providers are typically the strictest/costliest.
  rateLimits: {
    email: Number(process.env.RATE_LIMIT_EMAIL_PER_SEC) || 10,
    sms: Number(process.env.RATE_LIMIT_SMS_PER_SEC) || 5,
    push: Number(process.env.RATE_LIMIT_PUSH_PER_SEC) || 20,
  },
};
