/**
 * Throwaway manual test: fires 15 rapid calls at the email bucket
 * (capacity 10), then waits 300ms and fires 5 more, to see the burst +
 * refill behavior directly rather than trusting the code by inspection.
 *
 * Run with: node scripts/testRateLimiter.js
 */
const { tryConsume } = require('../src/services/rateLimiter');

async function burst(label, count) {
  for (let i = 0; i < count; i++) {
    const allowed = await tryConsume('email');
    console.log(`${label} #${i + 1}:`, allowed ? 'ALLOWED' : 'BLOCKED');
  }
}

async function main() {
  await burst('initial burst', 15);
  console.log('--- waiting 300ms for refill ---');
  await new Promise((r) => setTimeout(r, 300));
  await burst('after wait', 5);
  process.exit(0);
}

main();
