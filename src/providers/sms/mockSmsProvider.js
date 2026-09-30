const { providerFailureRates } = require('../../config/env');

const PROVIDER_NAME = 'mock-sms-provider';

async function send({ recipient, payload }) {
  await new Promise((r) => setTimeout(r, 15 + Math.random() * 60));

  if (Math.random() < providerFailureRates.sms) {
    const err = new Error('Simulated SMS carrier rejection');
    err.transient = true;
    throw err;
  }

  return {
    provider: PROVIDER_NAME,
    messageId: `sms-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    to: recipient,
    body: payload.body,
  };
}

module.exports = { send, PROVIDER_NAME };
