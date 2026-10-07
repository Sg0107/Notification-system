const { kafkaClient } = require('./client');

// Creates and starts a Kafka consumer in its own consumer group, subscribed
// to a single topic, calling `onMessage` for each message received.
//
// `concurrency` maps to kafkajs's partitionsConsumedConcurrently - how many
// of this topic's partitions this consumer will process in parallel. This
// is what gives us real priority handling: we create one of these per
// priority topic, with a higher concurrency number for "high" than "low".
async function startConsumer({ groupId, topic, concurrency, onMessage }) {
  const consumer = kafkaClient.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  await consumer.run({
    partitionsConsumedConcurrently: concurrency,
    eachMessage: async ({ message }) => {
      const parsed = JSON.parse(message.value.toString());
      await onMessage(parsed);
      // No explicit "commit offset" call needed - kafkajs auto-commits
      // after eachMessage resolves successfully. If onMessage throws,
      // kafkajs will NOT advance the offset, and will retry this same
      // message - this is the "at-least-once" behavior we mentioned.
    },
  });

  return consumer;
}

module.exports = { startConsumer };
