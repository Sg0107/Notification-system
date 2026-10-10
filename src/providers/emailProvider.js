const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');
const { aws } = require('../config/env');

const PROVIDER_NAME = 'aws-ses';

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
