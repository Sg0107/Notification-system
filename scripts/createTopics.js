/**
 * One-time (well, idempotent - safe to re-run) setup script: connects to
 * Kafka as an "admin" client and creates every (channel x priority) topic
 * we defined in src/kafka/topics.js, if it doesn't already exist.
 *
 * Why explicit creation instead of letting Kafka auto-create topics on
 * first use? Auto-created topics get a default partition count (often 1),
 * which is usually wrong for what we want. Creating them ourselves means
 * we control partition count deliberately.
 *
 * Run with: node scripts/createTopics.js
 */
const { kafkaClient } = require('../src/kafka/client');
const { channels, priorities, topicsFor, retryTopicFor } = require('../src/kafka/topics');

async function main() {
  const admin = kafkaClient.admin();
  await admin.connect();

  const existingTopics = await admin.listTopics();

  const desiredTopics = [];
  for (const channel of Object.values(channels)) {
    for (const priority of Object.values(priorities)) {
      desiredTopics.push(topicsFor(channel, priority));
    }
    desiredTopics.push(retryTopicFor(channel));
  }

  const topicsToCreate = desiredTopics
    .filter((topic) => !existingTopics.includes(topic))
    .map((topic) => ({
      topic,
      numPartitions: 3,
      replicationFactor: 1, // we only have 1 Kafka broker running locally
    }));

  if (topicsToCreate.length === 0) {
    console.log('All topics already exist, nothing to do.');
  } else {
    await admin.createTopics({ topics: topicsToCreate, waitForLeaders: true });
    console.log(
      `Created ${topicsToCreate.length} topic(s):`,
      topicsToCreate.map((t) => t.topic)
    );
  }

  await admin.disconnect();
}

main().catch((err) => {
  console.error('Failed to create topics:', err);
  process.exit(1);
});
