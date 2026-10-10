const express = require('express');
const router = express.Router();
const { channels, priorities, topicsFor } = require('../../kafka/topics');
const { publish } = require('../../kafka/producer');
const {
  buildIdempotencyKey,
  claimIdempotencyKey,
  releaseIdempotencyKey,
} = require('../../services/idempotency');
const { createQueuedNotification } = require('../../dao/notificationDao');

router.post('/', async (req, res) => {
  const { channel, priority = 'normal', recipient, payload, idempotencyKey } = req.body;

  // validate channel/priority against the enums from kafka/topics.js
  if (!Object.values(channels).includes(channel)) {
    return res.status(400).json({ error: `Invalid channel: ${channel}` });
  }
  if (!Object.values(priorities).includes(priority)) {
    return res.status(400).json({ error: `Invalid priority: ${priority}` });
  }
  if (!recipient) {
    return res.status(400).json({ error: `Recipient is required` });
  }
  if (!payload) {
    return res.status(400).json({ error: `Payload is required` });
  }

  const topic = topicsFor(channel, priority);
  const builtIdempotencyKey = buildIdempotencyKey({ idempotencyKey, channel, recipient, payload });

  // Outer try/catch: covers claimIdempotencyKey() itself failing (e.g.
  // Redis is down). Nothing was claimed in that case, so there's nothing
  // to roll back - just report the failure.
  try {
    const claimed = await claimIdempotencyKey(builtIdempotencyKey);
    if (!claimed) {
      return res.status(409).json({
        error: 'duplicate notification',
        idempotencyKey: builtIdempotencyKey,
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
      await publish(topic, { channel, priority, recipient, payload, idempotencyKey: builtIdempotencyKey });
    } catch (publishError) {
      await releaseIdempotencyKey(builtIdempotencyKey);
      throw publishError;
    }

    // Best-effort: record this notification for the dashboard. Kafka is
    // already the source of truth for delivery at this point (the message
    // is published), so a Mongo hiccup here shouldn't fail a request that
    // has, in fact, been successfully queued.
    try {
      await createQueuedNotification({
        idempotencyKey: builtIdempotencyKey,
        channel,
        priority,
        recipient,
        payload,
      });
    } catch (storeError) {
      console.error('Failed to record notification in MongoDB (non-fatal):', storeError.message);
    }

    res.status(202).json({
      message: 'Notification queued',
      channel,
      priority,
      idempotencyKey: builtIdempotencyKey,
    });
  } catch (error) {
    console.error('Error queuing notification:', error);
    res.status(500).json({ error: 'Failed to queue notification' });
  }
});

module.exports = router;
