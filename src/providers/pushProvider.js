const { providerFailureRates } = require('../config/env');

const PROVIDER_NAME = 'mock-push-provider';

async function send({ recipient, payload }) {
  await new Promise((resolve) => setTimeout(resolve, 10 + Math.random() * 40));

  if (Math.random() < providerFailureRates.push) {
    throw new Error('Simulated push gateway 5xx');
  }

  return {
    provider: PROVIDER_NAME,
    messageId: `push-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    deviceToken: recipient,
    title: payload.title,
  };
}

module.exports = { send, PROVIDER_NAME };
