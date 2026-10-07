const { startWorker } = require('./baseWorker');
const smsProvider = require('../providers/smsProvider');

startWorker({ channel: 'sms', sendFn: smsProvider.send }).catch((err) => {
  console.error('SMS worker failed to start:', err);
  process.exit(1);
});
