const { startConsumer } = require('../kafka/consumer');
const { topicsFor, priorities } = require('../kafka/topics');
const { tryConsume } = require('../services/rateLimiter');

// Higher priority = more concurrent partition processing, so a backlog of
// low-priority work can never delay high-priority work from being picked
// up. These numbers are small because we only configured 3 partitions per
// topic back in createTopics.js - concurrency can't exceed partition count
// anyway (extra "concurrent slots" beyond the partition count just sit idle).
const CONCURRENCY_BY_PRIORITY = {
  [priorities.HIGH]: 3,
  [priorities.NORMAL]: 2,
  [priorities.LOW]: 1,
};

// Builds and starts one worker for a given channel ("email"/"sms"/"push").
// Internally, this is actually THREE separate Kafka consumers (one per
// priority topic), each in its own consumer group, each with its own
// concurrency - not one consumer subscribed to all three topics. kafkajs
// applies `partitionsConsumedConcurrently` uniformly across everything one
// consumer is subscribed to, so getting different concurrency per
// priority requires genuinely separate consumer instances.
//
// `sendFn` is the channel-specific provider function: async ({recipient,
// payload}) => result, expected to throw on failure.
async function startWorker({ channel, sendFn }) {
  const consumers = [];

  for (const priority of Object.values(priorities)) {
    const topic = topicsFor(channel, priority);
    const groupId = `${channel}-worker-${priority}`;

    const consumer = await startConsumer({
      groupId,
      topic,
      concurrency: CONCURRENCY_BY_PRIORITY[priority],
      onMessage: async (notification) => makeHandler(channel, sendFn)(notification),
    });

    consumers.push(consumer);
    console.log(`[${channel}] listening on ${topic} (concurrency ${CONCURRENCY_BY_PRIORITY[priority]})`);
  }

  return consumers;
}

function makeHandler(channel, sendFn) {
  return async function handleNotification(notification) {
    const { recipient, payload, priority } = notification;

    // TEMPORARY for this step: if we're over budget, just skip and log.
    // Step 5 (retry/backoff) replaces this with properly re-queuing the
    // message onto a delayed retry topic instead of dropping it.
    const allowed = await tryConsume(channel);
    if (!allowed) {
      console.warn(`[${channel}] rate limited, skipping for now (priority=${priority})`, { recipient });
      return;
    }

    try {
      const result = await sendFn({ recipient, payload });
      console.log(`[${channel}] sent OK (priority=${priority})`, {
        recipient,
        provider: result.provider,
        messageId: result.messageId,
      });
    } catch (err) {
      // TEMPORARY for this step: just log. Step 5/6 add real retry with
      // backoff and circuit-breaker-based provider failure isolation.
      console.error(`[${channel}] send FAILED (priority=${priority})`, {
        recipient,
        error: err.message,
      });
    }
  };
}

module.exports = { startWorker };
