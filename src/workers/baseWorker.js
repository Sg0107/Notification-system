const { startConsumer } = require('../kafka/consumer');
const { connectProducer, publish } = require('../kafka/producer');
const { topicsFor, retryTopicFor, dlqTopicFor, priorities } = require('../kafka/topics');
const { tryConsume } = require('../services/rateLimiter');
const { computeBackoffMs } = require('../utils/backoff');
const { callWithBreaker, CircuitOpenError } = require('../services/circuitBreaker');
const { retry: retryConfig, circuitBreaker: circuitBreakerConfig } = require('../config/env');

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
// Internally, this is actually FOUR separate Kafka consumers: one per
// priority topic (each its own consumer group, each its own concurrency -
// kafkajs applies partitionsConsumedConcurrently uniformly across
// everything one consumer is subscribed to, so different concurrency per
// priority requires genuinely separate consumer instances), plus one more
// for this channel's retry topic.
//
// `sendFn` is the channel-specific provider function: async ({recipient,
// payload}) => result, expected to throw on failure.
async function startWorker({ channel, sendFn }) {
  // Workers only ever CONSUMED messages before this step. Now that a
  // failed/rate-limited message gets republished to the retry topic, this
  // process is also a producer, and needs its own explicit connection -
  // same as the API server does in server.js. Without this, kafkajs
  // lazily auto-connects on the first publish() call instead, which is
  // less robust and was the actual cause of the "producer is disconnected"
  // crash we hit while testing.
  await connectProducer();

  // One breaker per channel, created once and shared across every message
  // this worker handles (not a fresh breaker per message) - its whole job
  // is to remember failure history across calls, so it has to be the same
  // instance every time.
  const guardedSend = callWithBreaker(channel, sendFn, circuitBreakerConfig);
  const handleNotification = makeHandler(channel, guardedSend);

  const consumers = [];

  for (const priority of Object.values(priorities)) {
    const topic = topicsFor(channel, priority);
    const groupId = `${channel}-worker-${priority}`;

    const consumer = await startConsumer({
      groupId,
      topic,
      concurrency: CONCURRENCY_BY_PRIORITY[priority],
      onMessage: handleNotification,
    });

    consumers.push(consumer);
    console.log(`[${channel}] listening on ${topic} (concurrency ${CONCURRENCY_BY_PRIORITY[priority]})`);
  }

  // The retry scheduler: holds a failed/rate-limited message until its
  // deliverAt time, then republishes it onto its ORIGINAL priority topic
  // (not back onto the retry topic) so it re-enters the normal flow and
  // gets picked up by one of the consumers above, exactly like a fresh
  // message - the only difference is its `attempt` count is already > 0.
  const retryTopic = retryTopicFor(channel);
  const retryConsumer = await startConsumer({
    groupId: `${channel}-worker-retry`,
    topic: retryTopic,
    concurrency: 2,
    onMessage: async (notification) => {
      const waitMs = Math.max(0, (notification.deliverAt || 0) - Date.now());
      if (waitMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }
      await publish(topicsFor(notification.channel, notification.priority), notification);
    },
  });
  consumers.push(retryConsumer);
  console.log(`[${channel}] listening on ${retryTopic} (retry scheduler)`);

  return consumers;
}

// `send` is already wrapped with the circuit breaker (see startWorker) -
// from here, a breaker rejection and a genuine provider failure both just
// arrive as a thrown error, so the retry/backoff path below treats them
// identically. We only distinguish them in the log line, so it's obvious
// from the logs whether we actually hit the provider or short-circuited.
function makeHandler(channel, send) {
  return async function handleNotification(notification) {
    const { recipient, payload, priority, attempt = 0 } = notification;

    const allowed = await tryConsume(channel);
    if (!allowed) {
      // Being over budget isn't a failure - retry quickly on a short
      // fixed delay, and don't touch the attempt count (doesn't count
      // against maxRetries).
      console.warn(`[${channel}] rate limited, scheduling quick retry (priority=${priority})`, { recipient });
      await publish(retryTopicFor(channel), {
        ...notification,
        deliverAt: Date.now() + retryConfig.rateLimitRetryDelayMs,
      });
      return;
    }

    try {
      const result = await send({ recipient, payload });
      console.log(`[${channel}] sent OK (priority=${priority}, attempt=${attempt})`, {
        recipient,
        provider: result.provider,
        messageId: result.messageId,
      });
    } catch (err) {
      const nextAttempt = attempt + 1;
      const reason = err instanceof CircuitOpenError ? 'circuit open' : 'send FAILED';

      if (nextAttempt >= retryConfig.maxRetries) {
        // Out of retries - instead of just dropping the notification,
        // publish it to the channel's dead-letter topic with enough
        // context to debug later without needing to cross-reference logs:
        // the original notification body, how many attempts were actually
        // made, the final error, and when it was given up on.
        console.error(
          `[${channel}] ${reason}, max retries (${retryConfig.maxRetries}) exhausted - sending to DLQ (priority=${priority})`,
          { recipient, error: err.message }
        );
        await publish(dlqTopicFor(channel), {
          ...notification,
          attempts: nextAttempt,
          finalError: err.message,
          deadLetteredAt: Date.now(),
        });
        return;
      }

      const backoffMs = computeBackoffMs(nextAttempt, retryConfig.baseDelayMs, retryConfig.maxDelayMs);
      console.error(
        `[${channel}] ${reason}, scheduling retry ${nextAttempt}/${retryConfig.maxRetries} in ${backoffMs}ms (priority=${priority})`,
        { recipient, error: err.message }
      );

      await publish(retryTopicFor(channel), {
        ...notification,
        attempt: nextAttempt,
        deliverAt: Date.now() + backoffMs,
      });
    }
  };
}

module.exports = { startWorker };
