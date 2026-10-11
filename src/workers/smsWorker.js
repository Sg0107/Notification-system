const { startWorker } = require('./baseWorker');
const smsProvider = require('../providers/smsProvider');
const logger = require('../utils/logger');

startWorker({ channel: 'sms', sendFn: smsProvider.send }).catch(async (err) => {
  logger.error('SMS worker failed to start:', err);
  await logger.flush();
  process.exit(1);
});
