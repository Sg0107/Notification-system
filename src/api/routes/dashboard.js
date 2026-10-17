const express = require('express');
const router = express.Router();
const { listNotifications, getStats } = require('../../dao/notificationDao');
const { channels } = require('../../kafka/topics');
const logger = require('../../utils/logger');

// The statuses notificationDao actually writes.
const STATUSES = ['queued', 'retrying', 'sent', 'dead_lettered'];

// GET /api/dashboard/stats - the summary numbers: total count, counts per
// status, and counts per channel+status. Cheap enough to call on every
// dashboard page load/refresh (one aggregation query).
router.get('/stats', async (req, res) => {
  try {
    const stats = await getStats();
    res.json(stats);
  } catch (error) {
    logger.error('Failed to load dashboard stats:', error);
    res.status(500).json({ error: 'Failed to load stats' });
  }
});

// GET /api/dashboard/notifications - paginated, newest-first, optionally
// filtered by ?channel= and/or ?status=. Query params arrive as strings,
// so limit/skip are parsed and clamped here rather than trusting the client.
router.get('/notifications', async (req, res) => {
  const { channel, status } = req.query;

  if (channel && !Object.values(channels).includes(channel)) {
    return res.status(400).json({ error: `Invalid channel: ${channel}` });
  }
  // Express parses ?status[$ne]=sent into an object, which would reach the
  // Mongo filter as a query operator. Only accept one of the known status
  // strings - the includes() check also rejects objects and arrays.
  if (status && !STATUSES.includes(status)) {
    return res.status(400).json({ error: `Invalid status: ${status}` });
  }

  const limit = Math.min(Math.max(Number(req.query.limit) || 20, 1), 100);
  const skip = Math.max(Number(req.query.skip) || 0, 0);

  try {
    const { items, total } = await listNotifications({ channel, status, limit, skip });
    res.json({ items, total, limit, skip });
  } catch (error) {
    logger.error('Failed to list notifications:', error);
    res.status(500).json({ error: 'Failed to list notifications' });
  }
});

module.exports = router;
