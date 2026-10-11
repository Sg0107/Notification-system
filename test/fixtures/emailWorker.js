// Behaves like a worker whose startup failed: logs, flushes, exits with 1.
// Named emailWorker.js on purpose: the logger names its source after the script.
const logger = require('../../src/utils/logger');

logger.info('worker starting');
logger.error('Email worker failed to start:', new Error('boom'));
logger.flush().then(() => process.exit(1));
