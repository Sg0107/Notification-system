const { redisClient } = require('../config/redis');
const { idempotencyTtlSeconds } = require('../config/env');

// idempotencyKey is REQUIRED from the caller (enforced in the route, not
// here) - there used to be a fallback that auto-derived a key by hashing
// channel+recipient+payload, but that can only ever express "this content
// looks the same as before," not "this is the same intended event as
// before." Those are genuinely different things: a caller might legitimately
// send identical content twice on purpose (e.g. the same reminder on two
// different days), and a hash of content alone can't tell that apart from
// an accidental duplicate retry - only the CALLER knows which one it is,
// since that's a question about intent, not content. So the caller is now
// required to generate their own key once per real event (an order ID, a
// cron run ID, a UUID minted when the decision to send is made) and reuse
// it only when retrying that same event.

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
  claimIdempotencyKey,
  releaseIdempotencyKey,
};
