const path = require('node:path');
const express = require('express');
const { connectProducer } = require('../kafka/producer');
const { connectMongo } = require('../config/mongo');
const { ensureIndexes } = require('../dao/notificationDao');
const notificationsRouter = require('./routes/notifications');
const dashboardRouter = require('./routes/dashboard');
const logger = require('../utils/logger');

const app = express();
app.use(express.json());

app.get('/health', (req, res) => res.json({ status: 'ok' }));
app.use('/api/notifications', notificationsRouter);
app.use('/api/dashboard', dashboardRouter);

// The dashboard's static HTML/JS/CSS - plain files, no build step. Served
// from the same API process so there's no separate server or CORS to deal
// with; the dashboard's JS just calls /api/dashboard/* on its own origin.
app.use(express.static(path.join(__dirname, '../../public')));

async function start() {
  // Connect to Kafka once, before accepting any HTTP traffic - if Kafka is
  // unreachable, we want the whole process to fail loudly at startup,
  // rather than accepting requests we can't actually fulfil.
  await connectProducer();

  // Same reasoning for Mongo - fail loudly at startup if it's unreachable,
  // rather than discovering it mid-request.
  await connectMongo();
  await ensureIndexes();

  const port = 3000;
  app.listen(port, () => {
    logger.info(`Notification API listening on port ${port}`);
  });
}

start().catch(async (err) => {
  logger.error('Failed to start API:', err);
  await logger.flush();
  process.exit(1);
});
