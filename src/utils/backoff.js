const { randomInt } = require('crypto');

const computeBackoffMs = (attempt = 0, baseDelay = 1000, maxDelay = 30000) => {
  const delay = randomInt(0, Math.min(baseDelay * Math.pow(2, attempt), maxDelay));
  return delay;
}

module.exports = {computeBackoffMs};