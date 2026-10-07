const { providerFailureRates } = require('../config/env');

const PROVIDER_NAME = 'mock-sms-provider';

// Fake "send an SMS" - simulates network latency and a configurable random
// failure rate, so later steps (retry, circuit breaker) have real failures
// to react to. Swap this out for an actual Twilio call later without
// touching any worker code - that's the whole point of this interface.
async function send({ recipient, payload }) {
  await new Promise((resolve) => setTimeout(resolve, 20 + Math.random() * 60));

  if (Math.random() < providerFailureRates.sms) {
    throw new Error('Simulated SMS carrier rejection');
  }

  return {
    provider: PROVIDER_NAME,
    messageId: `sms-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    to: recipient,
    body: payload.body,
  };
}

module.exports = { send, PROVIDER_NAME };
