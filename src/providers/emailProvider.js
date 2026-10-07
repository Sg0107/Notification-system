const { providerFailureRates } = require('../config/env');

const PROVIDER_NAME = 'mock-email-provider';

async function send({ recipient, payload }) {
  await new Promise((resolve) => setTimeout(resolve, 20 + Math.random() * 80));

  if (Math.random() < providerFailureRates.email) {
    throw new Error('Simulated email provider timeout');
  }

  return {
    provider: PROVIDER_NAME,
    messageId: `email-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    to: recipient,
    subject: payload.subject,
  };
}

module.exports = { send, PROVIDER_NAME };
