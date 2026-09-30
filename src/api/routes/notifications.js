const express = require('express');
const Notification = require('../../models/Notification');
const DeadLetter = require('../../models/DeadLetter');
const { submitNotification } = require('../../services/notificationService');
const { submitNotificationSchema } = require('../validators/notificationSchema');

const router = express.Router();

// POST /api/notifications - enqueue a new notification
router.post('/', async (req, res, next) => {
  try {
    const { error, value } = submitNotificationSchema.validate(req.body);
    if (error) {
      return res.status(400).json({ error: 'validation_error', message: error.message });
    }

    const notification = await submitNotification(value);
    return res.status(202).json({
      notificationId: notification.notificationId,
      status: notification.status,
      idempotencyKey: notification.idempotencyKey,
    });
  } catch (err) {
    return next(err);
  }
});

// GET /api/notifications - list/filter
router.get('/', async (req, res, next) => {
  try {
    const { status, channel, limit = 50 } = req.query;
    const filter = {};
    if (status) filter.status = status;
    if (channel) filter.channel = channel;

    const notifications = await Notification.find(filter)
      .sort({ createdAt: -1 })
      .limit(Math.min(Number(limit) || 50, 200))
      .lean();

    return res.json({ count: notifications.length, notifications });
  } catch (err) {
    return next(err);
  }
});

// GET /api/notifications/dlq/list - inspect dead-lettered jobs
// NOTE: must be registered before the /:id route below, otherwise Express
// would match "dlq" as an :id param.
router.get('/dlq/list', async (req, res, next) => {
  try {
    const items = await DeadLetter.find({ resolved: false })
      .sort({ createdAt: -1 })
      .limit(100)
      .lean();
    return res.json({ count: items.length, items });
  } catch (err) {
    return next(err);
  }
});

// GET /api/notifications/:id - check delivery status
router.get('/:id', async (req, res, next) => {
  try {
    const notification = await Notification.findOne({
      notificationId: req.params.id,
    }).lean();
    if (!notification) {
      return res.status(404).json({ error: 'not_found' });
    }
    return res.json(notification);
  } catch (err) {
    return next(err);
  }
});

module.exports = router;
