/**
 * Exponential backoff with full jitter.
 * delay = random(0, min(maxBackoffMs, baseBackoffMs * 2^attempt))
 */
function computeBackoffMs(attempt, baseBackoffMs, maxBackoffMs) {
  const exp = Math.min(maxBackoffMs, baseBackoffMs * Math.pow(2, attempt));
  return Math.floor(Math.random() * exp);
}

module.exports = { computeBackoffMs };
