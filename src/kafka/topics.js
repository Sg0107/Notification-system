/**
 * Topic layout
 * ------------
 * Priority is modeled as separate topics per channel so that high-priority
 * consumers can be scaled independently and never wait behind a backlog of
 * low priority messages (a single topic + header priority would still be
 * processed in offset order by one consumer group).
 *
 * notifications.<channel>.<priority>   -> primary work queues
 * notifications.<channel>.retry        -> delayed-retry holding topic
 * notifications.<channel>.dlq          -> dead letter queue after max retries
 */

const CHANNELS = ['email', 'sms', 'push'];
const PRIORITIES = ['high', 'normal', 'low'];

function topicFor(channel, priority) {
  return `notifications.${channel}.${priority}`;
}

function retryTopicFor(channel) {
  return `notifications.${channel}.retry`;
}

function dlqTopicFor(channel) {
  return `notifications.${channel}.dlq`;
}

function allPrimaryTopics() {
  const topics = [];
  for (const channel of CHANNELS) {
    for (const priority of PRIORITIES) {
      topics.push(topicFor(channel, priority));
    }
  }
  return topics;
}

function allRetryTopics() {
  return CHANNELS.map(retryTopicFor);
}

function allDlqTopics() {
  return CHANNELS.map(dlqTopicFor);
}

module.exports = {
  CHANNELS,
  PRIORITIES,
  topicFor,
  retryTopicFor,
  dlqTopicFor,
  allPrimaryTopics,
  allRetryTopics,
  allDlqTopics,
};
