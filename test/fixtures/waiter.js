// Runs until it is told to stop, like the API server does.
const logger = require('../../src/utils/logger');

logger.info('server is up');
console.log('READY');
setInterval(() => {}, 1000);
