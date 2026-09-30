const { getKafkaClient } = require('./client');
const { allPrimaryTopics, allRetryTopics, allDlqTopics } = require('./topics');
const logger = require('../utils/logger');

async function ensureTopics() {
  const admin = getKafkaClient().admin();
  await admin.connect();
  try {
    const existing = await admin.listTopics();
    const desired = [
      ...allPrimaryTopics(),
      ...allRetryTopics(),
      ...allDlqTopics(),
    ];
    const toCreate = desired
      .filter((t) => !existing.includes(t))
      .map((topic) => ({
        topic,
        numPartitions: topic.endsWith('.dlq') ? 3 : 6,
        replicationFactor: 1,
      }));

    if (toCreate.length) {
      await admin.createTopics({ topics: toCreate, waitForLeaders: true });
      logger.info(`Created ${toCreate.length} Kafka topics`, {
        topics: toCreate.map((t) => t.topic),
      });
    } else {
      logger.info('All required Kafka topics already exist');
    }
  } finally {
    await admin.disconnect();
  }
}

module.exports = { ensureTopics };
