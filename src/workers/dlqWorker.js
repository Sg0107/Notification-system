const { getKafkaClient } = require('../kafka/client');
const { CHANNELS, dlqTopicFor } = require('../kafka/topics');
const { connectMongo } = require('../config/mongo');
const logger = require('../utils/logger');

/**
 * Dead-letter processing worker. In this reference implementation it just
 * logs and leaves the durable record in MongoDB (via DeadLetter, written
 * by baseWorker at the moment of dead-lettering) for operators to inspect
 * through the API and decide whether to manually replay or discard. In a
 * production system this is where you'd wire paging/alerting.
 */
async function start() {
  await connectMongo();
  const kafka = getKafkaClient();

  const consumers = await Promise.all(
    CHANNELS.map(async (channel) => {
      const consumer = kafka.consumer({ groupId: `dlq-observer-${channel}` });
      await consumer.connect();
      await consumer.subscribe({ topic: dlqTopicFor(channel), fromBeginning: false });
      await consumer.run({
        eachMessage: async ({ message }) => {
          const job = JSON.parse(message.value.toString());
          logger.error('DLQ item observed', {
            channel,
            notificationId: job.notificationId,
            finalError: job.finalError,
          });
        },
      });
      return consumer;
    })
  );

  logger.info('DLQ worker started', { channels: CHANNELS });

  const shutdown = async () => {
    await Promise.all(consumers.map((c) => c.disconnect()));
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) {
  start().catch((err) => {
    logger.error('DLQ worker failed to start', { error: err.message });
    process.exit(1);
  });
}

module.exports = { start };
