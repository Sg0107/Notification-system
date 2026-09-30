const { createChannelWorker } = require('./baseWorker');
const pushProvider = require('../providers/push/mockPushProvider');
const logger = require('../utils/logger');

const worker = createChannelWorker({
  channel: 'push',
  providerName: pushProvider.PROVIDER_NAME,
  sendFn: pushProvider.send,
});

if (require.main === module) {
  worker.start().catch((err) => {
    logger.error('Push worker failed to start', { error: err.message });
    process.exit(1);
  });
}

module.exports = worker;
