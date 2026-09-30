const { providerFailureRates } = require('../../config/env');

/**
 * Mock provider standing in for a real integration (e.g. SendGrid/SES).
 * Randomly fails at a configurable rate so the retry/backoff/circuit-breaker
 * machinery has something real to exercise end to end.
 */
const PROVIDER_NAME = 'mock-email-provider';

async function send({ recipient, payload }) {
  await new Promise((r) => setTimeout(r, 20 + Math.random() * 80));

  if (Math.random() < providerFailureRates.email) {
    const err = new Error('Simulated email provider timeout');
    err.transient = true;
    throw err;
  }

  return {
    provider: PROVIDER_NAME,
    messageId: `email-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    to: recipient,
    subject: payload.subject,
  };
}

module.exports = { send, PROVIDER_NAME };
