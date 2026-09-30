const mongoose = require('mongoose');

const STATUSES = [
  'PENDING',
  'QUEUED',
  'PROCESSING',
  'DELIVERED',
  'FAILED_RETRYABLE',
  'DEAD_LETTERED',
];

const attemptSchema = new mongoose.Schema(
  {
    attempt: Number,
    startedAt: Date,
    finishedAt: Date,
    success: Boolean,
    provider: String,
    error: String,
  },
  { _id: false }
);

const notificationSchema = new mongoose.Schema(
  {
    notificationId: { type: String, required: true, unique: true, index: true },
    idempotencyKey: { type: String, required: true, index: true },
    channel: { type: String, enum: ['email', 'sms', 'push'], required: true },
    priority: { type: String, enum: ['high', 'normal', 'low'], default: 'normal' },
    recipient: { type: String, required: true },
    payload: { type: mongoose.Schema.Types.Mixed, required: true },
    status: { type: String, enum: STATUSES, default: 'PENDING', index: true },
    attemptCount: { type: Number, default: 0 },
    maxRetries: { type: Number, default: 5 },
    attempts: [attemptSchema],
    lastError: String,
    deliveredAt: Date,
    deadLetteredAt: Date,
  },
  { timestamps: true }
);

notificationSchema.index({ idempotencyKey: 1 }, { unique: true });

module.exports = mongoose.model('Notification', notificationSchema);
module.exports.STATUSES = STATUSES;
