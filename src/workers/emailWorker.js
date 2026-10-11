const { startWorker } = require('./baseWorker');
const emailProvider = require('../providers/emailProvider');
const logger = require('../utils/logger');

startWorker({ channel: 'email', sendFn: emailProvider.send }).catch(async (err) => {
  logger.error('Email worker failed to start:', err);
  await logger.flush();
  process.exit(1);
});
