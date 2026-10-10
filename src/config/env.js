require('dotenv').config();

module.exports = {
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9093').split(','),
    clientId: process.env.KAFKA_CLIENT_ID || 'notification-system',
  },
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6380',
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27018/notifications',
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
  // Mock providers randomly fail at this rate (0-1) so the retry/circuit
  // breaker logic we build in later steps has something real to react to.
  providerFailureRates: {
    email: Number(process.env.EMAIL_PROVIDER_FAILURE_RATE) || 0.1,
    sms: Number(process.env.SMS_PROVIDER_FAILURE_RATE) || 0.15,
    push: Number(process.env.PUSH_PROVIDER_FAILURE_RATE) || 0.05,
  },
  retry: {
    maxRetries: Number(process.env.MAX_RETRIES) || 5,
    baseDelayMs: Number(process.env.RETRY_BASE_DELAY_MS) || 1000,
    maxDelayMs: Number(process.env.RETRY_MAX_DELAY_MS) || 30000,
    // Rate-limited (not failed) messages retry quickly on a short fixed
    // delay - being over budget isn't a real failure, it doesn't count
    // against maxRetries, and it should resolve itself within a second
    // or two once the token bucket refills.
    rateLimitRetryDelayMs: Number(process.env.RATE_LIMIT_RETRY_DELAY_MS) || 250,
  },
  circuitBreaker: {
    // Consecutive failures before the breaker trips from closed -> open.
    failureThreshold: Number(process.env.CIRCUIT_BREAKER_THRESHOLD) || 5,
    // How long the breaker stays open before allowing one half-open trial
    // call through, in ms.
    breakTimeoutMs: Number(process.env.CIRCUIT_BREAKER_TIMEOUT_MS) || 5000,
  },
};
