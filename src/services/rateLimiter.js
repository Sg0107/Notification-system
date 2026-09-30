const { getRedisClient } = require('../config/redis');
const { rateLimits } = require('../config/env');

/**
 * Token-bucket rate limiter implemented with a single atomic Lua script so
 * check-and-decrement never races across worker processes. One bucket per
 * channel (and optionally per provider) refills continuously at
 * `ratePerSec` and holds at most `ratePerSec` tokens (burst == 1s of quota).
 */
const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])

if tokens == nil then
  tokens = rate
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(rate, tokens + elapsed * rate)

local allowed = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, 60)

return allowed
`;

function ratePerSecFor(channel) {
  return rateLimits[channel] || 10;
}

/**
 * Returns true if a slot was consumed (caller may proceed), false if the
 * channel is currently over its rate budget (caller should re-queue/delay).
 */
async function tryConsume(channel, tokens = 1) {
  const redis = getRedisClient();
  const rate = ratePerSecFor(channel);
  const now = Date.now() / 1000;
  const key = `ratelimit:${channel}`;
  const allowed = await redis.eval(TOKEN_BUCKET_LUA, 1, key, rate, now, tokens);
  return allowed === 1;
}

module.exports = { tryConsume, ratePerSecFor };
