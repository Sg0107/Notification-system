/**
 * Inspects a channel's dead-letter topic: prints every message currently
 * sitting there, then exits. This is deliberately a one-shot read, not a
 * long-running consumer - the DLQ is meant to be checked on demand (e.g.
 * "did anything end up here today?"), not continuously processed. There's
 * no automatic handling of DLQ messages yet (no alerting, no admin "retry
 * from DLQ" action) - this script is just visibility into what's there.
 *
 * Run with: node scripts/viewDlq.js email
 */
const { kafkaClient } = require('../src/kafka/client');
const { channels, dlqTopicFor } = require('../src/kafka/topics');

async function main() {
  const channel = process.argv[2];
  if (!channel || !Object.values(channels).includes(channel)) {
    console.error(`Usage: node scripts/viewDlq.js <${Object.values(channels).join('|')}>`);
    process.exit(1);
  }

  const topic = dlqTopicFor(channel);
  const consumer = kafkaClient.consumer({
    // A fresh, randomly-suffixed group ID every run, so this script always
    // reads the DLQ from the beginning instead of resuming from wherever a
    // previous run left off - we want "show me everything that's there,"
    // not "show me only what's new since last time."
    groupId: `dlq-viewer-${channel}-${Date.now()}`,
  });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  console.log(`Reading ${topic}... (will exit after 3s of no new messages)`);

  let count = 0;
  let idleTimer;

  const resetIdleTimer = () => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(async () => {
      console.log(`\n${count} message(s) in ${topic}.`);
      await consumer.disconnect();
      process.exit(0);
    }, 3000);
  };

  await consumer.run({
    eachMessage: async ({ message }) => {
      count++;
      const notification = JSON.parse(message.value.toString());
      console.log(`\n--- #${count} ---`);
      console.log(JSON.stringify(notification, null, 2));
      resetIdleTimer();
    },
  });

  resetIdleTimer();
}

main().catch((err) => {
  console.error('Failed to read DLQ:', err);
  process.exit(1);
});
