const { getKafkaClient } = require('../kafka/client');
const { publish } = require('../kafka/producer');
const { CHANNELS, retryTopicFor, topicFor } = require('../kafka/topics');
const { connectMongo } = require('../config/mongo');
const logger = require('../utils/logger');

/**
 * Holds delayed-retry jobs until their backoff window elapses, then
 * re-publishes them onto the original priority topic for that channel.
 *
 * Kafka has no native delayed-delivery, so this "poor man's delay queue"
 * pattern (dedicated retry topic + a consumer that sleeps out the
 * remaining delay per message) is what actually implements exponential
 * backoff here. It intentionally blocks its own partition while waiting -
 * that's fine because retry topics are low-throughput compared to the
 * primary work queues, and it never blocks primary-topic consumers.
 */
async function start() {
  await connectMongo();
  const kafka = getKafkaClient();

  const consumers = await Promise.all(
    CHANNELS.map(async (channel) => {
      const consumer = kafka.consumer({ groupId: `retry-scheduler-${channel}` });
      await consumer.connect();
      await consumer.subscribe({ topic: retryTopicFor(channel), fromBeginning: false });
      await consumer.run({
        partitionsConsumedConcurrently: 3,
        eachMessage: async ({ message }) => {
          const job = JSON.parse(message.value.toString());
          const waitMs = Math.max(0, (job.deliverAt || 0) - Date.now());
          if (waitMs > 0) {
            await new Promise((r) => setTimeout(r, waitMs));
          }
          await publish(topicFor(channel, job.priority || 'normal'), job);
          logger.info('Retry released back to primary queue', {
            notificationId: job.notificationId,
            channel,
            attempt: job.attempt,
            waitedMs: waitMs,
          });
        },
      });
      return consumer;
    })
  );

  logger.info('Retry scheduler started', { channels: CHANNELS });

  const shutdown = async () => {
    logger.info('Shutting down retry scheduler');
    await Promise.all(consumers.map((c) => c.disconnect()));
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) {
  start().catch((err) => {
    logger.error('Retry scheduler failed to start', { error: err.message });
    process.exit(1);
  });
}

module.exports = { start };
