const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');
const { aws, sendRealEmails, providerFailureRates } = require('../config/env');
const logger = require('../utils/logger');

const PROVIDER_NAME = 'aws-ses';
const PROVIDER_NAME_MOCKED = 'aws-ses-mocked';

// One shared client, same "create once, reuse everywhere" pattern as the
// Kafka/Redis/Mongo clients - connecting/configuring a client per send
// would be wasteful and isn't how any of these SDKs are meant to be used.
const sesClient = new SESClient({
  region: aws.region,
  credentials: {
    accessKeyId: aws.accessKeyId,
    secretAccessKey: aws.secretAccessKey,
  },
});

// Same signature and same "throws on failure" contract as the mock this
// replaces - baseWorker.js, the circuit breaker, and retry/backoff logic
// don't know or care that this now calls a real AWS API instead of
// simulating one. SES rejects the promise on real failures (unverified
// recipient while in sandbox mode, throttling, invalid address, etc.),
// which is exactly the "expected to throw on failure" contract makeHandler
// already handles - no special-casing needed here.
async function send({ recipient, payload }) {
  if (!sendRealEmails) {
    // SEND_REAL_EMAILS is not "true" - skip the actual AWS call entirely.
    // Everything downstream (worker, retry/backoff, circuit breaker, Mongo
    // status) behaves exactly as if a real send succeeded; only the actual
    // network call to SES is skipped, which is what makes it safe to fire
    // load-test volume without burning into the SES quota/rate limit or
    // emailing anyone for real. A small artificial delay keeps timing
    // realistic rather than suspiciously instant.
    await new Promise((resolve) => setTimeout(resolve, 20 + Math.random() * 60));

    // Same "random failure rate" simulation as smsProvider/pushProvider,
    // using the same EMAIL_PROVIDER_FAILURE_RATE config - without this,
    // the mocked path never throws, so load testing with it never
    // exercises retry/backoff or the circuit breaker for email at all.
    if (Math.random() < providerFailureRates.email) {
      logger.debug('[email] SEND_REAL_EMAILS is false - mocked send FAILED (simulated)', { recipient });
      throw new Error('Simulated SES rejection (mocked)');
    }

    logger.debug('[email] SEND_REAL_EMAILS is false - mocked send, no SES call made', { recipient });
    return {
      provider: PROVIDER_NAME_MOCKED,
      messageId: `mock-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      to: recipient,
      subject: payload.subject,
    };
  }

  const command = new SendEmailCommand({
    Source: aws.sesFromEmail,
    Destination: { ToAddresses: [recipient] },
    Message: {
      Subject: { Data: payload.subject || '(no subject)' },
      Body: {
        Text: { Data: payload.body || '' },
      },
    },
  });

  const result = await sesClient.send(command);

  return {
    provider: PROVIDER_NAME,
    messageId: result.MessageId,
    to: recipient,
    subject: payload.subject,
  };
}

module.exports = { send, PROVIDER_NAME };
