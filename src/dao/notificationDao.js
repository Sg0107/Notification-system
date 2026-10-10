// DAO (Data Access Object) for the `notifications` collection - every raw
// Mongo query for notification documents lives here, and nowhere else.
// Callers (the API route, the worker) describe WHAT they want recorded
// ("this notification was sent", "this attempt failed") without knowing
// HOW it's stored - collection name, field names, index choices are all
// private to this file and could change without touching any caller.
const { getDb } = require('../config/mongo');

const COLLECTION = 'notifications';

// Call once at startup (API and workers both call this independently -
// createIndex is a no-op if the index already exists, so that's safe).
// Two indexes, chosen for the two kinds of queries we actually need:
// - idempotencyKey: unique, and it's how the API's "queued" write and the
//   worker's later "sent"/"dead_lettered" updates find the SAME document.
// - {channel, status, createdAt}: the shape a dashboard actually queries -
//   "how many email notifications are dead_lettered today", "status
//   breakdown per channel over time".
async function ensureIndexes() {
  const collection = getDb().collection(COLLECTION);
  await collection.createIndex({ idempotencyKey: 1 }, { unique: true });
  await collection.createIndex({ channel: 1, status: 1, createdAt: 1 });
}

// Called once, from the API, right after a notification is successfully
// published to Kafka. This is the document's birth - everything after this
// is an update keyed by idempotencyKey, not a new insert.
async function createQueuedNotification({ idempotencyKey, channel, priority, recipient, payload }) {
  const now = new Date();
  await getDb().collection(COLLECTION).insertOne({
    idempotencyKey,
    channel,
    priority,
    recipient,
    payload,
    status: 'queued',
    attempts: 0,
    lastError: null,
    provider: null,
    messageId: null,
    createdAt: now,
    updatedAt: now,
    sentAt: null,
    deadLetteredAt: null,
  });
}

// Called from the worker on every failed send that's going to be retried
// (not on the final exhausted attempt - that's markDeadLettered instead).
// Updates attempts/lastError in place rather than inserting a new document
// per attempt, so the dashboard always sees one row per notification with
// its current state, not a growing history table.
async function markAttemptFailed(idempotencyKey, { attempt, error }) {
  await getDb().collection(COLLECTION).updateOne(
    { idempotencyKey },
    {
      $set: {
        status: 'retrying',
        attempts: attempt,
        lastError: error,
        updatedAt: new Date(),
      },
    }
  );
}

// Called from the worker once a send actually succeeds.
async function markSent(idempotencyKey, { provider, messageId }) {
  const now = new Date();
  await getDb().collection(COLLECTION).updateOne(
    { idempotencyKey },
    {
      $set: {
        status: 'sent',
        provider,
        messageId,
        sentAt: now,
        updatedAt: now,
      },
    }
  );
}

// Called from the worker once a notification exhausts maxRetries and is
// published to the DLQ topic - this is the Mongo-side mirror of that event.
async function markDeadLettered(idempotencyKey, { attempts, finalError }) {
  const now = new Date();
  await getDb().collection(COLLECTION).updateOne(
    { idempotencyKey },
    {
      $set: {
        status: 'dead_lettered',
        attempts,
        lastError: finalError,
        deadLetteredAt: now,
        updatedAt: now,
      },
    }
  );
}

module.exports = {
  ensureIndexes,
  createQueuedNotification,
  markAttemptFailed,
  markSent,
  markDeadLettered,
};
