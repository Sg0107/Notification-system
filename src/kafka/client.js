const { Kafka } = require('kafkajs');
const { kafka } = require('../config/env');

// A single shared Kafka client. Both producers (things that send messages)
// and consumers (things that read messages) are created *from* this client,
// but the client itself doesn't connect to anything by itself - think of it
// as "the settings for how to reach the cluster," not a connection.
const kafkaClient = new Kafka({
  clientId: kafka.clientId,
  brokers: kafka.brokers,
});

module.exports = { kafkaClient };
