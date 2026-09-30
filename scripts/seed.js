/**
 * Fires a batch of sample notifications at the running API so you can watch
 * priority processing, retries, and (occasionally) dead-lettering happen in
 * real time across the worker logs.
 *
 * Usage: node scripts/seed.js [count] [baseUrl]
 */
const count = Number(process.argv[2]) || 30;
const baseUrl = process.argv[3] || 'http://localhost:3000';

const CHANNELS = ['email', 'sms', 'push'];
const PRIORITIES = ['high', 'normal', 'low'];

function samplePayload(channel, i) {
  if (channel === 'email') {
    return { recipient: `user${i}@example.com`, payload: { subject: `Order #${i} confirmed`, body: 'Thanks for your order!' } };
  }
  if (channel === 'sms') {
    return { recipient: '+15550001234', payload: { body: `Your verification code is ${1000 + i}` } };
  }
  return { recipient: `device-token-${i}`, payload: { title: 'New message', body: `You have a new message (${i})` } };
}

async function main() {
  for (let i = 0; i < count; i++) {
    const channel = CHANNELS[i % CHANNELS.length];
    const priority = PRIORITIES[i % PRIORITIES.length];
    const { recipient, payload } = samplePayload(channel, i);

    const res = await fetch(`${baseUrl}/api/notifications`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ channel, priority, recipient, payload }),
    });
    const body = await res.json();
    console.log(res.status, channel, priority, body.notificationId || body.error);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
