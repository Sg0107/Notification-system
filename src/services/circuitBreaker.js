const { getRedisClient } = require('../config/redis');
const { circuitBreaker } = require('../config/env');
const logger = require('../utils/logger');

/**
 * Redis-backed circuit breaker, one circuit per provider, shared across all
 * worker processes for that channel. This is what gives "provider failure
 * isolation": if the email provider starts erroring, the circuit opens and
 * email jobs fail fast (straight to retry/DLQ) instead of piling up threads
 * waiting on a dying dependency, while SMS/push circuits are unaffected.
 *
 * States: CLOSED (normal) -> OPEN (fast-fail) -> HALF_OPEN (single probe)
 */
const STATE_KEY = (provider) => `circuit:${provider}:state`;
const FAILURES_KEY = (provider) => `circuit:${provider}:failures`;
const OPENED_AT_KEY = (provider) => `circuit:${provider}:opened_at`;

async function getState(provider) {
  const redis = getRedisClient();
  const state = await redis.get(STATE_KEY(provider));
  if (!state) return 'CLOSED';

  if (state === 'OPEN') {
    const openedAt = Number(await redis.get(OPENED_AT_KEY(provider))) || 0;
    if (Date.now() - openedAt >= circuitBreaker.resetTimeoutMs) {
      return 'HALF_OPEN';
    }
  }
  return state;
}

async function recordSuccess(provider) {
  const redis = getRedisClient();
  await redis.del(FAILURES_KEY(provider));
  await redis.set(STATE_KEY(provider), 'CLOSED');
}

async function recordFailure(provider) {
  const redis = getRedisClient();
  const failures = await redis.incr(FAILURES_KEY(provider));
  await redis.expire(FAILURES_KEY(provider), 300);

  if (failures >= circuitBreaker.failureThreshold) {
    await redis.set(STATE_KEY(provider), 'OPEN');
    await redis.set(OPENED_AT_KEY(provider), Date.now());
    logger.warn(`Circuit breaker OPEN for provider ${provider}`, { failures });
  }
}

/**
 * Wraps a provider call. Throws CircuitOpenError immediately without
 * invoking `fn` if the circuit is open, isolating failures of one provider
 * from consuming worker capacity meant for others.
 */
async function withCircuitBreaker(provider, fn) {
  const state = await getState(provider);

  if (state === 'OPEN') {
    const err = new Error(`Circuit open for provider ${provider}`);
    err.code = 'CIRCUIT_OPEN';
    throw err;
  }

  try {
    const result = await fn();
    await recordSuccess(provider);
    return result;
  } catch (err) {
    await recordFailure(provider);
    throw err;
  }
}

module.exports = { withCircuitBreaker, getState };
