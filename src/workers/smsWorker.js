const { createChannelWorker } = require('./baseWorker');
const smsProvider = require('../providers/sms/mockSmsProvider');
const logger = require('../utils/logger');

const worker = createChannelWorker({
  channel: 'sms',
  providerName: smsProvider.PROVIDER_NAME,
  sendFn: smsProvider.send,
});

if (require.main === module) {
  worker.start().catch((err) => {
    logger.error('SMS worker failed to start', { error: err.message });
    process.exit(1);
  });
}

module.exports = worker;
