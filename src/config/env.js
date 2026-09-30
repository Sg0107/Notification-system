require('dotenv').config();

function num(val, fallback) {
  const n = Number(val);
  return Number.isFinite(n) ? n : fallback;
}

module.exports = {
  port: num(process.env.PORT, 3000),
  nodeEnv: process.env.NODE_ENV || 'development',

  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    clientId: process.env.KAFKA_CLIENT_ID || 'notification-system',
  },

  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/notifications',

  rateLimits: {
    email: num(process.env.RATE_LIMIT_EMAIL_PER_SEC, 50),
    sms: num(process.env.RATE_LIMIT_SMS_PER_SEC, 10),
    push: num(process.env.RATE_LIMIT_PUSH_PER_SEC, 100),
  },

  retry: {
    maxRetries: num(process.env.MAX_RETRIES, 5),
    baseBackoffMs: num(process.env.BASE_BACKOFF_MS, 1000),
    maxBackoffMs: num(process.env.MAX_BACKOFF_MS, 60000),
  },

  idempotencyTtlSeconds: num(process.env.IDEMPOTENCY_TTL_SECONDS, 86400),

  providerFailureRates: {
    email: num(process.env.EMAIL_PROVIDER_FAILURE_RATE, 0.1),
    sms: num(process.env.SMS_PROVIDER_FAILURE_RATE, 0.15),
    push: num(process.env.PUSH_PROVIDER_FAILURE_RATE, 0.05),
  },

  circuitBreaker: {
    failureThreshold: num(process.env.CIRCUIT_BREAKER_FAILURE_THRESHOLD, 5),
    resetTimeoutMs: num(process.env.CIRCUIT_BREAKER_RESET_TIMEOUT_MS, 30000),
  },
};
