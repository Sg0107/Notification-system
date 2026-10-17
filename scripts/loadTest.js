// Full-matrix load test for the notification pipeline.
//
// Prints the machine's capacity (CPU, RAM, disk, Node/OS) as a header so
// any throughput numbers below are read next to what hardware produced
// them - the same test run on a laptop and a beefy server will naturally
// hit different ceilings, and that header is what makes the numbers mean
// something later.
//
// Fires notifications at EVERY channel (email/sms/push) x EVERY priority
// (high/normal/low) combination at once, as fast as the API will accept
// them - the highest-concurrency shape this system can actually be asked
// to handle. Reports ingestion throughput (how fast the API accepts the
// burst) and, separately, per-channel end-to-end drain time (how long
// each channel's worker actually takes to finish processing everything
// sent to it, via its own rate limiter/retry/circuit-breaker behavior -
// each channel has its own token bucket shared across all 3 of its
// priority topics, so the 3 priorities for one channel compete with each
// other, not with the other channels).
//
// Safe to run with SEND_REAL_EMAILS=false (the default): the email
// provider mocks the AWS call (including a simulated failure rate, same
// as the sms/push mocks), so retries and the circuit breaker get
// exercised without ever touching AWS.
//
// Usage: node scripts/loadTest.js [countPerCombo]
//   countPerCombo: how many notifications to send per (channel, priority)
//   pair. There are 9 pairs, so total volume = countPerCombo * 9.

const os = require('node:os');
const { execSync } = require('node:child_process');
const crypto = require('node:crypto');
require('dotenv').config();

const API_URL = 'http://localhost:3000/api/notifications';
const STATS_URL = 'http://localhost:3000/api/dashboard/stats';

const CHANNELS = ['email', 'sms', 'push'];
const PRIORITIES = ['high', 'normal', 'low'];

const countPerCombo = Number(process.argv[2]) || 50;
const totalCount = countPerCombo * CHANNELS.length * PRIORITIES.length;

const DRAIN_POLL_MS = 1000;
// Scales with volume: 3s per notification-per-pair, never under 2 minutes.
const DRAIN_TIMEOUT_MS = Math.max(120000, countPerCombo * 3000);

function diskFreeGb() {
  try {
    // df -k gives 1024-byte blocks; the mount for "." is enough context
    // for a local dev-machine capacity check, no need to inspect every
    // volume on the system.
    const out = execSync('df -k .', { encoding: 'utf8' });
    const line = out.trim().split('\n').pop();
    const cols = line.trim().split(/\s+/);
    const availableKb = Number(cols[3]);
    return (availableKb / 1024 / 1024).toFixed(1);
  } catch {
    return 'unknown';
  }
}

function printSystemHeader() {
  const cpus = os.cpus();
  const totalMemGb = (os.totalmem() / 1024 ** 3).toFixed(1);
  const freeMemGb = (os.freemem() / 1024 ** 3).toFixed(1);

  console.log('='.repeat(70));
  console.log('SYSTEM CAPACITY (this is the machine the numbers below came from)');
  console.log('='.repeat(70));
  console.log(`Host:        ${os.hostname()}`);
  console.log(`OS:          ${os.platform()} ${os.release()} (${os.arch()})`);
  console.log(`Node:        ${process.version}`);
  console.log(`CPU:         ${cpus[0]?.model || 'unknown'} - ${cpus.length} logical core(s)`);
  console.log(`RAM:         ${totalMemGb} GB total, ${freeMemGb} GB free right now`);
  console.log(`Disk free:   ${diskFreeGb()} GB (on the volume running this process)`);
  console.log('='.repeat(70));
}

function printTestConfig() {
  const { rateLimits, retry, circuitBreaker, sendRealEmails, providerFailureRates } = require('../src/config/env');
  console.log('TEST PARAMETERS (full matrix: every channel x every priority)');
  console.log('-'.repeat(70));
  console.log(`Per (channel, priority) pair: ${countPerCombo}`);
  console.log(`Total notifications:          ${totalCount}`);
  console.log(`SEND_REAL_EMAILS:              ${sendRealEmails} ${sendRealEmails ? '(REAL SES CALLS WILL HAPPEN)' : '(mocked - no real sends)'}`);
  for (const ch of CHANNELS) {
    console.log(`  ${ch.padEnd(6)} rate limit: ${String(rateLimits[ch]).padEnd(3)}/sec   simulated failure rate: ${(providerFailureRates[ch] * 100).toFixed(0)}%   volume: ${countPerCombo * PRIORITIES.length}`);
  }
  console.log(`Max retries:                   ${retry.maxRetries}`);
  console.log(`Circuit breaker:               trips after ${circuitBreaker.failureThreshold} consecutive failures, ${circuitBreaker.breakTimeoutMs}ms cooldown`);
  console.log('='.repeat(70));
  console.log('');
}

function percentile(sortedArr, p) {
  if (sortedArr.length === 0) return 0;
  const idx = Math.min(sortedArr.length - 1, Math.floor((p / 100) * sortedArr.length));
  return sortedArr[idx];
}

function recipientFor(channel, n) {
  if (channel === 'email') return `loadtest+${n}@example.com`;
  if (channel === 'sms') return `+1555${String(n).padStart(7, '0')}`;
  return `device-loadtest-${n}`;
}

function payloadFor(channel, n) {
  if (channel === 'email') return { subject: `Load test #${n}`, body: `Load test message ${n}` };
  if (channel === 'push') return { title: `Load test #${n}`, body: `Load test message ${n}` };
  return { body: `Load test message ${n}` }; // sms has no subject slot
}

async function sendOne(channel, priority, n) {
  const idempotencyKey = `loadtest-${Date.now()}-${channel}-${priority}-${n}-${crypto.randomUUID()}`;
  const body = {
    channel,
    priority,
    recipient: recipientFor(channel, n),
    payload: payloadFor(channel, n),
    idempotencyKey,
  };

  const start = Date.now();
  try {
    const res = await fetch(API_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return { channel, priority, status: res.status, ms: Date.now() - start };
  } catch (err) {
    return { channel, priority, status: 'ERR', ms: Date.now() - start, error: err.message };
  }
}

async function getStats() {
  const res = await fetch(STATS_URL);
  return res.json();
}

async function runIngestionBurst() {
  const tasks = [];
  let n = 0;
  for (const channel of CHANNELS) {
    for (const priority of PRIORITIES) {
      for (let i = 0; i < countPerCombo; i++) {
        n++;
        tasks.push({ channel, priority, n });
      }
    }
  }

  console.log(`Firing ${tasks.length} concurrent requests across ${CHANNELS.length} channels x ${PRIORITIES.length} priorities ...`);
  const start = Date.now();
  const results = await Promise.all(tasks.map((t) => sendOne(t.channel, t.priority, t.n)));
  const elapsedMs = Date.now() - start;

  const accepted = results.filter((r) => r.status === 202).length;
  const duplicates = results.filter((r) => r.status === 409).length;
  const failed = results.length - accepted - duplicates;
  const latencies = results.map((r) => r.ms).sort((a, b) => a - b);

  console.log('');
  console.log('INGESTION (how fast the API accepted the full burst)');
  console.log('-'.repeat(70));
  console.log(`Accepted (202):        ${accepted}/${tasks.length}`);
  console.log(`Duplicates (409):      ${duplicates}`);
  console.log(`Failed/other:          ${failed}`);
  console.log(`Wall-clock time:       ${elapsedMs}ms`);
  console.log(`Ingestion throughput:  ${(accepted / (elapsedMs / 1000)).toFixed(1)} requests/sec`);
  console.log(`Request latency:       p50=${percentile(latencies, 50)}ms  p95=${percentile(latencies, 95)}ms  p99=${percentile(latencies, 99)}ms`);

  // Per-channel x per-priority breakdown, so you can see if any one
  // combination got rejected more than others (e.g. idempotency
  // collisions, validation errors).
  console.log('');
  console.log('Per (channel, priority) accepted / sent:');
  for (const channel of CHANNELS) {
    const row = PRIORITIES.map((p) => {
      const subset = results.filter((r) => r.channel === channel && r.priority === p);
      const ok = subset.filter((r) => r.status === 202).length;
      return `${p}=${ok}/${subset.length}`;
    }).join('   ');
    console.log(`  ${channel.padEnd(6)} ${row}`);
  }
  console.log('');

  // Accepted counts grouped by channel - this is what each channel's
  // worker actually has to drain, regardless of which priority it came
  // in on, since the rate limiter bucket is per-channel, shared across
  // all 3 priorities.
  const acceptedByChannel = {};
  for (const channel of CHANNELS) {
    acceptedByChannel[channel] = results.filter((r) => r.channel === channel && r.status === 202).length;
  }

  return acceptedByChannel;
}

// Polls /api/dashboard/stats until every channel's (sent + dead_lettered)
// has grown by at least its target since the baseline taken before the
// burst - i.e. until each channel's worker has actually finished WORKING
// on everything this test queued for it, not just accepted it. Channels
// finish at different times since they have different rate limits/retry
// behavior, so each is tracked and reported independently.
async function watchDrain(targetsByChannel, baseline) {
  const totalTarget = Object.values(targetsByChannel).reduce((a, b) => a + b, 0);
  if (totalTarget === 0) return;

  console.log(`Watching the pipeline drain ${totalTarget} queued notifications across ${CHANNELS.length} channels (polling every ${DRAIN_POLL_MS}ms)...`);
  const start = Date.now();

  const baselineByChannel = {};
  for (const channel of CHANNELS) {
    const b = baseline.byChannel?.[channel] || {};
    baselineByChannel[channel] = (b.sent || 0) + (b.dead_lettered || 0);
  }

  const finishedAtMs = {};

  while (Date.now() - start < DRAIN_TIMEOUT_MS) {
    await new Promise((r) => setTimeout(r, DRAIN_POLL_MS));
    const stats = await getStats();

    let totalProcessed = 0;
    for (const channel of CHANNELS) {
      const c = stats.byChannel?.[channel] || {};
      const doneNow = (c.sent || 0) + (c.dead_lettered || 0);
      const processed = Math.min(doneNow - baselineByChannel[channel], targetsByChannel[channel]);
      totalProcessed += processed;

      if (!finishedAtMs[channel] && processed >= targetsByChannel[channel] && targetsByChannel[channel] > 0) {
        finishedAtMs[channel] = Date.now() - start;
      }
    }

    process.stdout.write(`\r  processed ${totalProcessed}/${totalTarget} total...   `);

    if (totalProcessed >= totalTarget) break;
  }

  const finalStats = await getStats();
  console.log('\n');
  console.log('END-TO-END DRAIN (per channel - rate limit + retries + circuit breaker)');
  console.log('-'.repeat(70));

  for (const channel of CHANNELS) {
    const target = targetsByChannel[channel];
    if (target === 0) continue;
    const c = finalStats.byChannel?.[channel] || {};
    const sentDelta = (c.sent || 0) - (baseline.byChannel?.[channel]?.sent || 0);
    const dlqDelta = (c.dead_lettered || 0) - (baseline.byChannel?.[channel]?.dead_lettered || 0);
    const finishMs = finishedAtMs[channel];

    if (finishMs != null) {
      const throughput = (target / (finishMs / 1000)).toFixed(2);
      console.log(`${channel.padEnd(6)} drained ${target} in ${(finishMs / 1000).toFixed(1)}s  (${throughput}/sec effective)  sent=${sentDelta}  dead_lettered=${dlqDelta}`);
    } else {
      console.log(`${channel.padEnd(6)} did NOT finish within ${DRAIN_TIMEOUT_MS / 1000}s  sent=${sentDelta}  dead_lettered=${dlqDelta}  (target was ${target})`);
    }
  }

  console.log('='.repeat(70));
  console.log('Full breakdown any time at http://localhost:3000/dashboard.html');
}

async function main() {
  printSystemHeader();
  printTestConfig();

  const baseline = await getStats().catch(() => ({ byStatus: {}, byChannel: {} }));
  const acceptedByChannel = await runIngestionBurst();

  await watchDrain(acceptedByChannel, baseline);
}

main().catch((err) => {
  console.error('Load test failed:', err);
  process.exit(1);
});
