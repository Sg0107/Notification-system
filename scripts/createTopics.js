const { ensureTopics } = require('../src/kafka/adminSetup');
const logger = require('../src/utils/logger');

ensureTopics()
  .then(() => {
    logger.info('Topic setup complete');
    process.exit(0);
  })
  .catch((err) => {
    logger.error('Topic setup failed', { error: err.message });
    process.exit(1);
  });
