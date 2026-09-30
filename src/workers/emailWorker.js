const { createChannelWorker } = require('./baseWorker');
const emailProvider = require('../providers/email/mockEmailProvider');
const logger = require('../utils/logger');

const worker = createChannelWorker({
  channel: 'email',
  providerName: emailProvider.PROVIDER_NAME,
  sendFn: emailProvider.send,
});

if (require.main === module) {
  worker.start().catch((err) => {
    logger.error('Email worker failed to start', { error: err.message });
    process.exit(1);
  });
}

module.exports = worker;
