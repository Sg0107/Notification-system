const { startWorker } = require('./baseWorker');
const emailProvider = require('../providers/emailProvider');

startWorker({ channel: 'email', sendFn: emailProvider.send }).catch((err) => {
  console.error('Email worker failed to start:', err);
  process.exit(1);
});
