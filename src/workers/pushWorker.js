const { startWorker } = require('./baseWorker');
const pushProvider = require('../providers/pushProvider');
const logger = require('../utils/logger');

startWorker({ channel: 'push', sendFn: pushProvider.send }).catch(async (err) => {
  logger.error('Push worker failed to start:', err);
  await logger.flush();
  process.exit(1);
});
