const { getKafkaClient } = require('./client');
const logger = require('../utils/logger');

let producer;
let connected = false;

async function getProducer() {
  if (!producer) {
    producer = getKafkaClient().producer({
      allowAutoTopicCreation: false,
      idempotent: true, // exactly-once semantics at the producer/broker level
      maxInFlightRequests: 1,
    });
  }
  if (!connected) {
    await producer.connect();
    connected = true;
    logger.info('Kafka producer connected');
  }
  return producer;
}

/**
 * Publish a notification job. The key is set to notificationId so that all
 * retries/events for the same notification land on the same partition,
 * preserving order for that entity.
 */
async function publish(topic, message) {
  const p = await getProducer();
  await p.send({
    topic,
    messages: [
      {
        key: message.notificationId,
        value: JSON.stringify(message),
        headers: {
          'x-attempt': String(message.attempt || 0),
        },
      },
    ],
  });
}

async function disconnectProducer() {
  if (producer && connected) {
    await producer.disconnect();
    connected = false;
  }
}

module.exports = { getProducer, publish, disconnectProducer };
