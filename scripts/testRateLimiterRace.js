/**
 * Demonstrates the race condition in the non-atomic rate limiter: fires 20
 * truly CONCURRENT requests at a bucket with capacity 5 (using Promise.all,
 * not a loop with await) and counts how many got ALLOWED.
 *
 * If this were race-free, exactly 5 should be allowed. Run it a few times -
 * watch the allowed count creep above 5.
 *
 * Run with: node scripts/testRateLimiterRace.js
 */
const { redisClient } = require('../src/config/redis');
const { tryConsume } = require('../src/services/rateLimiter');

async function main() {
  await redisClient.del('ratelimit:sms'); // sms has the lowest configured rate (5/sec) - easiest to see overflow

  const results = await Promise.all(
    Array.from({ length: 20 }, () => tryConsume('sms'))
  );

  const allowedCount = results.filter(Boolean).length;
  console.log('Allowed:', allowedCount, '/ 20 (expected: 5, if race-free)');
  process.exit(0);
}

main();
