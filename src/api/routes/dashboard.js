const express = require('express');
const router = express.Router();
const { listNotifications, getStats } = require('../../dao/notificationDao');
const { channels } = require('../../kafka/topics');
const logger = require('../../utils/logger');

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
