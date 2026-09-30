const { v4: uuidv4 } = require('uuid');
const Notification = require('../models/Notification');
const { publish } = require('../kafka/producer');
const { topicFor } = require('../kafka/topics');
const {
  buildIdempotencyKey,
  reserveIdempotencyKey,
} = require('./idempotencyService');
const { retry } = require('../config/env');
const logger = require('../utils/logger');

class DuplicateNotificationError extends Error {
  constructor(idempotencyKey, existing) {
    super(`Duplicate notification for idempotency key ${idempotencyKey}`);
    this.code = 'DUPLICATE';
    this.idempotencyKey = idempotencyKey;
    this.existing = existing;
  }
}

/**
 * Ingest a notification request: dedupe, persist, and publish onto the
 * priority-specific Kafka topic for its channel. This is the only place
 * that decides *whether* a notification is new work; workers downstream
 * assume everything they see is legitimate, unique work.
 */
async function submitNotification({
  channel,
  priority = 'normal',
  recipient,
  payload,
  idempotencyKey: callerKey,
}) {
  const idempotencyKey = buildIdempotencyKey({
    idempotencyKey: callerKey,
    channel,
    recipient,
    payload,
  });

  const isNew = await reserveIdempotencyKey(idempotencyKey);
  if (!isNew) {
    const existing = await Notification.findOne({ idempotencyKey }).lean();
    throw new DuplicateNotificationError(idempotencyKey, existing);
  }

  const notificationId = uuidv4();

  const notification = await Notification.create({
    notificationId,
    idempotencyKey,
    channel,
    priority,
    recipient,
    payload,
    status: 'QUEUED',
    maxRetries: retry.maxRetries,
  });

  const topic = topicFor(channel, priority);
  await publish(topic, {
    notificationId,
    idempotencyKey,
    channel,
    priority,
    recipient,
    payload,
    attempt: 0,
  });

  logger.info('Notification queued', { notificationId, channel, priority, topic });

  return notification;
}

module.exports = { submitNotification, DuplicateNotificationError };
