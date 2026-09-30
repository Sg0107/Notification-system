const mongoose = require('mongoose');

const deadLetterSchema = new mongoose.Schema(
  {
    notificationId: { type: String, required: true, index: true },
    channel: String,
    payload: mongoose.Schema.Types.Mixed,
    reason: String,
    attemptCount: Number,
    lastError: String,
    rawMessage: mongoose.Schema.Types.Mixed,
    resolved: { type: Boolean, default: false },
    resolvedAt: Date,
  },
  { timestamps: true }
);

module.exports = mongoose.model('DeadLetter', deadLetterSchema);
