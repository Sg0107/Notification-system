const express = require('express');
const router = express.Router();
const { channels, priorities, topicsFor } = require('../../kafka/topics');
const { publish } = require('../../kafka/producer');
const {
  claimIdempotencyKey,
  releaseIdempotencyKey,
} = require('../../services/idempotency');
const { createQueuedNotification } = require('../../dao/notificationDao');
const logger = require('../../utils/logger');

router.post('/', async (req, res) => {
  const { channel, priority = 'normal', recipient, payload, idempotencyKey } = req.body;

  logger.info('Notification request received', { channel, priority, recipient, idempotencyKey });

  // validate channel/priority against the enums from kafka/topics.js
  if (!Object.values(channels).includes(channel)) {
    logger.warn('Rejected notification request: invalid channel', { channel });
    return res.status(400).json({ error: `Invalid channel: ${channel}` });
  }
  if (!Object.values(priorities).includes(priority)) {
    logger.warn('Rejected notification request: invalid priority', { priority });
    return res.status(400).json({ error: `Invalid priority: ${priority}` });
  }
  if (!recipient) {
    logger.warn('Rejected notification request: missing recipient', { channel });
    return res.status(400).json({ error: `Recipient is required` });
  }
  if (!payload) {
    logger.warn('Rejected notification request: missing payload', { channel, recipient });
    return res.status(400).json({ error: `Payload is required` });
  }
  // idempotencyKey is required, not auto-derived from content - a hash of
  // channel+recipient+payload can only tell you "this looks the same as
  // before," not "this is the same intended event as before," and only the
  // CALLER knows which one is true. The caller should generate this once
  // per real event (an order ID, a cron run ID, a UUID minted when the
  // decision to send is made) and reuse it only when retrying that event.
  if (!idempotencyKey) {
    logger.warn('Rejected notification request: missing idempotencyKey', { channel, recipient });
    return res.status(400).json({ error: 'idempotencyKey is required' });
  }

  const topic = topicsFor(channel, priority);

  // Outer try/catch: covers claimIdempotencyKey() itself failing (e.g.
  // Redis is down). Nothing was claimed in that case, so there's nothing
  // to roll back - just report the failure.
  try {
    const claimed = await claimIdempotencyKey(idempotencyKey);
    if (!claimed) {
      logger.warn('Duplicate notification rejected', { channel, recipient, idempotencyKey });
      return res.status(409).json({
        error: 'duplicate notification',
        idempotencyKey,
      });
    }

    // Inner try/catch: covers publish() failing *after* we've already
    // claimed the key. Here we DO need to roll back, so a legitimate
    // retry with the same idempotency key isn't wrongly blocked for the
    // rest of the TTL window.
    try {
      // idempotencyKey now rides along in the message body - the worker
      // needs it to find this same notification's Mongo document later
      // and update it (markSent/markAttemptFailed/markDeadLettered).
      await publish(topic, { channel, priority, recipient, payload, idempotencyKey });
    } catch (publishError) {
      await releaseIdempotencyKey(idempotencyKey);
      throw publishError;
    }

    // Best-effort: record this notification for the dashboard. Kafka is
    // already the source of truth for delivery at this point (the message
    // is published), so a Mongo hiccup here shouldn't fail a request that
    // has, in fact, been successfully queued.
    try {
      await createQueuedNotification({
        idempotencyKey,
        channel,
        priority,
        recipient,
        payload,
      });
    } catch (storeError) {
      logger.error('Failed to record notification in MongoDB (non-fatal):', storeError.message);
    }

    logger.info('Notification queued', { channel, priority, recipient, idempotencyKey, topic });

    res.status(202).json({
      message: 'Notification queued',
      channel,
      priority,
      idempotencyKey,
    });
  } catch (error) {
    logger.error('Error queuing notification:', error);
    res.status(500).json({ error: 'Failed to queue notification' });
  }
});

module.exports = router;
