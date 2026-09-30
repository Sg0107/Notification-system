const Joi = require('joi');

const submitNotificationSchema = Joi.object({
  channel: Joi.string().valid('email', 'sms', 'push').required(),
  priority: Joi.string().valid('high', 'normal', 'low').default('normal'),
  recipient: Joi.string().required(),
  idempotencyKey: Joi.string().optional(),
  payload: Joi.object().required(),
});

module.exports = { submitNotificationSchema };
