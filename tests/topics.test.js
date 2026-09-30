const {
  topicFor,
  retryTopicFor,
  dlqTopicFor,
  allPrimaryTopics,
  CHANNELS,
  PRIORITIES,
} = require('../src/kafka/topics');

describe('topic naming', () => {
  it('builds primary topic names per channel/priority', () => {
    expect(topicFor('email', 'high')).toBe('notifications.email.high');
    expect(topicFor('sms', 'low')).toBe('notifications.sms.low');
  });

  it('builds retry and dlq topic names per channel', () => {
    expect(retryTopicFor('push')).toBe('notifications.push.retry');
    expect(dlqTopicFor('push')).toBe('notifications.push.dlq');
  });

  it('enumerates every channel x priority combination exactly once', () => {
    const topics = allPrimaryTopics();
    expect(topics.length).toBe(CHANNELS.length * PRIORITIES.length);
    expect(new Set(topics).size).toBe(topics.length);
  });
});
