const { Kafka, logLevel } = require('kafkajs');
const { kafka } = require('../config/env');

let kafkaClient;

function getKafkaClient() {
  if (!kafkaClient) {
    kafkaClient = new Kafka({
      clientId: kafka.clientId,
      brokers: kafka.brokers,
      logLevel: logLevel.NOTHING,
      retry: {
        initialRetryTime: 300,
        retries: 8,
      },
    });
  }
  return kafkaClient;
}

module.exports = { getKafkaClient };
