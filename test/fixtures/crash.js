// Throws an exception nobody catches, a moment after logging.
const logger = require('../../src/utils/logger');

logger.info('about to crash');
setTimeout(() => {
  throw new Error('kaboom');
}, 50);
setInterval(() => {}, 1000); // keep the process alive until the crash
