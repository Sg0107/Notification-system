const { redisClient } = require('../config/redis');
const { rateLimits } = require('../config/env');

function capacityFor(channel) {
  // The bucket's max size equals the channel's configured rate - i.e. we
  // allow up to one full second's worth of requests as a burst.
  return rateLimits[channel];
}

// This entire script runs as ONE atomic operation inside Redis - Redis
// executes commands one at a time on a single thread, and a Lua script
// counts as a single command for that purpose. No other client's request
// can be interleaved partway through this logic, which is exactly the
// guarantee the plain-JS version was missing.
//
// KEYS[1] = the bucket's Redis key (e.g. "ratelimit:email")
// ARGV[1] = rate (tokens per second, also used as bucket capacity)
//
// We ask Redis itself for the current time (via the TIME command) rather
// than passing in Date.now() from Node - this avoids relying on the
// clock of whichever app server happens to run this, which matters once
// there are multiple worker processes/machines calling this.
const TOKEN_BUCKET_SCRIPT = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])

local time = redis.call('TIME')
local nowMs = tonumber(time[1]) * 1000 + tonumber(time[2]) / 1000

local bucket = redis.call('HMGET', key, 'tokens', 'lastRefill')
local tokens
local lastRefill

if bucket[1] == false then
  -- HMGET returns Lua "false" for a field that doesn't exist (first time
  -- we've ever seen this channel) - start the bucket full.
  tokens = rate
  lastRefill = nowMs
else
  tokens = tonumber(bucket[1])
  lastRefill = tonumber(bucket[2])
  local elapsedSeconds = (nowMs - lastRefill) / 1000
  local refillAmount = elapsedSeconds * rate
  tokens = math.min(rate, tokens + refillAmount)
  lastRefill = nowMs
end

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'lastRefill', lastRefill)

return allowed
`;

async function tryConsume(channel) {
  const rate = capacityFor(channel);
  const key = `ratelimit:${channel}`;

  // ioredis's eval signature: eval(script, numberOfKeys, key1, ..., arg1, ...)
  // "1" here means "the next 1 argument is a KEY", so key -> KEYS[1],
  // and everything after that is ARGV.
  const allowed = await redisClient.eval(TOKEN_BUCKET_SCRIPT, 1, key, rate);

  return allowed === 1;
}

module.exports = { tryConsume };
