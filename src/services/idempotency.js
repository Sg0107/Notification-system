const crypto = require('crypto');
const { redisClient } = require('../config/redis');
const { idempotencyTtlSeconds } = require('../config/env');

function buildIdempotencyKey({ idempotencyKey, channel, recipient, payload }) {
  if (idempotencyKey) {
    return idempotencyKey;
  }
  const hash = crypto
    .createHash('sha256')
    .update(JSON.stringify({ channel, recipient, payload }))
    .digest('hex');
  return `auto:${hash}`;
}

// Returns true if this call successfully claimed the key (caller may
// proceed - this is a new request). Returns false if the key was already
// claimed by an earlier call (this is a duplicate - caller should NOT
// proceed again).
async function claimIdempotencyKey(key) {
  // SET key value EX <ttl> NX, all as one atomic Redis command.
  // - EX <ttl>: expire this key automatically after idempotencyTtlSeconds
  // - NX: only set the key if it does NOT already exist
  // ioredis returns the string 'OK' if the SET actually happened, or
  // `null` if NX blocked it because the key was already present.
  const result = await redisClient.set(key, '1', 'EX', idempotencyTtlSeconds, 'NX');
  return result === 'OK';
}

// Used to roll back a claim when the thing we claimed it *for* (publishing
// to Kafka) ends up failing - so a legitimate retry with the same key
// isn't permanently blocked for the rest of the TTL window.
async function releaseIdempotencyKey(key) {
  await redisClient.del(key);
}

module.exports = {
  buildIdempotencyKey,
  claimIdempotencyKey,
  releaseIdempotencyKey,
};
