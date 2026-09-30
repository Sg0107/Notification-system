const crypto = require('crypto');
const { getRedisClient } = require('../config/redis');
const { idempotencyTtlSeconds } = require('../config/env');

/**
 * Deterministic idempotency key: caller-supplied key if present, otherwise a
 * hash of (channel + recipient + payload). This guarantees retried API calls
 * or duplicate Kafka deliveries never produce two real-world sends.
 */
function buildIdempotencyKey({ idempotencyKey, channel, recipient, payload }) {
  if (idempotencyKey) return idempotencyKey;
  const hash = crypto
    .createHash('sha256')
    .update(JSON.stringify({ channel, recipient, payload }))
    .digest('hex');
  return `auto:${hash}`;
}

/**
 * Atomically reserve an idempotency key. Returns true if this call is the
 * first to claim the key (i.e. safe to proceed), false if it's a duplicate.
 * Uses SET ... NX so the check-and-set is a single atomic Redis operation.
 */
async function reserveIdempotencyKey(key) {
  const redis = getRedisClient();
  const result = await redis.set(
    `idem:${key}`,
    '1',
    'EX',
    idempotencyTtlSeconds,
    'NX'
  );
  return result === 'OK';
}

/**
 * Idempotency guard specifically around the *send* step (as opposed to
 * ingestion). Delivery workers use this so that a message redelivered by
 * Kafka (at-least-once) after a crash-before-commit never gets sent twice
 * to the provider.
 */
async function reserveSendAttempt(notificationId, attempt) {
  const redis = getRedisClient();
  const key = `send-lock:${notificationId}:${attempt}`;
  const result = await redis.set(key, '1', 'EX', 3600, 'NX');
  return result === 'OK';
}

async function releaseIdempotencyKey(key) {
  const redis = getRedisClient();
  await redis.del(`idem:${key}`);
}

module.exports = {
  buildIdempotencyKey,
  reserveIdempotencyKey,
  releaseIdempotencyKey,
  reserveSendAttempt,
};
