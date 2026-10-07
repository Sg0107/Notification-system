const { startWorker } = require('./baseWorker');
const pushProvider = require('../providers/pushProvider');

startWorker({ channel: 'push', sendFn: pushProvider.send }).catch((err) => {
  console.error('Push worker failed to start:', err);
  process.exit(1);
});
