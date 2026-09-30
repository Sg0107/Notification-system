const logger = require('../../utils/logger');
const { DuplicateNotificationError } = require('../../services/notificationService');

function errorHandler(err, req, res, next) { // eslint-disable-line no-unused-vars
  if (err instanceof DuplicateNotificationError) {
    return res.status(409).json({
      error: 'duplicate_notification',
      message: err.message,
      idempotencyKey: err.idempotencyKey,
      existing: err.existing
        ? {
            notificationId: err.existing.notificationId,
            status: err.existing.status,
          }
        : null,
    });
  }

  logger.error('Unhandled API error', { error: err.message, stack: err.stack });
  return res.status(err.status || 500).json({
    error: 'internal_error',
    message: err.message,
  });
}

module.exports = errorHandler;
