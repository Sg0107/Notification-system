const { kafkaClient } = require('./client');

// A Kafka producer needs an explicit connect() call before it can send
// anything - unlike, say, a typical HTTP client. We create one producer
// and reuse it for the whole life of the process rather than creating a
// new one per request (connecting is relatively slow).
const producer = kafkaClient.producer();
let isConnected = false;

async function connectProducer() {
  if (!isConnected) {
    await producer.connect();
    isConnected = true;
    console.log('Kafka producer connected');
  }
}

// `topic` - which Kafka topic to publish to (we'll pass in the result of
// topicsFor(channel, priority) from the route handler).
// `message` - a plain JS object; Kafka only stores bytes/strings, so we
// JSON.stringify it ourselves.
async function publish(topic, message) {
  await producer.send({
    topic,
    messages: [{ value: JSON.stringify(message) }],
  });
}

module.exports = { connectProducer, publish };
