const express = require('express');
const helmet = require('helmet');
const { connectMongo } = require('../config/mongo');
const { ensureTopics } = require('../kafka/adminSetup');
const { disconnectProducer } = require('../kafka/producer');
const notificationsRouter = require('./routes/notifications');
const errorHandler = require('./middleware/errorHandler');
const { port } = require('../config/env');
const logger = require('../utils/logger');

const app = express();
app.use(helmet());
app.use(express.json({ limit: '1mb' }));

app.get('/health', (req, res) => res.json({ status: 'ok' }));
app.use('/api/notifications', notificationsRouter);
app.use(errorHandler);

async function start() {
  await connectMongo();
  await ensureTopics();

  const server = app.listen(port, () => {
    logger.info(`Notification API listening on port ${port}`);
  });

  const shutdown = async (signal) => {
    logger.info(`Received ${signal}, shutting down API`);
    server.close();
    await disconnectProducer();
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

if (require.main === module) {
  start().catch((err) => {
    logger.error('Failed to start API', { error: err.message, stack: err.stack });
    process.exit(1);
  });
}

module.exports = app;
