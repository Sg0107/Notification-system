const express = require('express');
const router = express.Router();
const { channels, priorities, topicsFor } = require('../../kafka/topics');
const { publish } = require('../../kafka/producer');
const {
  claimIdempotencyKey,
  releaseIdempotencyKey,
} = require('../../services/idempotency');
const { createQueuedNotification, markPublishFailed } = require('../../dao/notificationDao');
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

    // Record the notification BEFORE publishing. A worker can pick up the
    // message and call markSent within milliseconds of publish(), and that
    // update needs the document to already exist. Best-effort: if Mongo is
    // down the notification is still delivered, it just won't appear on the
    // dashboard, so this is logged, not thrown.
    let recorded = true;
    try {
      await createQueuedNotification({
        idempotencyKey,
        channel,
        priority,
        recipient,
        payload,
      });
    } catch (storeError) {
      recorded = false;
      logger.error('Failed to record notification in MongoDB (non-fatal):', storeError.message);
    }

    // Inner try/catch: covers publish() failing *after* we've already
    // claimed the key. Here we DO need to roll back, so a legitimate
    // retry with the same idempotency key isn't wrongly blocked for the
    // rest of the TTL window - and the document created above is marked
    // "failed" (not deleted) since nothing will ever process it.
    try {
      // idempotencyKey rides along in the message body - the worker
      // needs it to find this same notification's Mongo document later
      // and update it (markSent/markAttemptFailed/markDeadLettered).
      await publish(topic, { channel, priority, recipient, payload, idempotencyKey });
    } catch (publishError) {
      await releaseIdempotencyKey(idempotencyKey);
      if (recorded) {
        try {
          await markPublishFailed(idempotencyKey, { error: publishError.message });
        } catch (markError) {
          logger.error('Failed to mark notification as failed in MongoDB (non-fatal):', markError.message);
        }
      }
      throw publishError;
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
