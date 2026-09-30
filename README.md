# Distributed Notification System

Asynchronous, multi-channel (email / SMS / push) notification delivery system built on **Node.js**, **Kafka**, **Redis**, and **MongoDB**. Notifications are ingested through a small HTTP API, queued by priority, and delivered by independent per-channel workers with idempotency, exponential backoff retry, provider failure isolation (circuit breakers), and dead-letter processing.

## Architecture

```
                 ┌──────────────┐
  HTTP clients → │  API (Express)│ → dedupe (Redis) → persist (Mongo) → publish
                 └──────────────┘
                                            │
                       notifications.<channel>.<priority>
                        (email|sms|push) x (high|normal|low)
                                            │
                 ┌──────────────────────────────────────────┐
                 │   Channel workers (email / sms / push)    │
                 │   - per-priority consumer, priority=more   │
                 │     concurrency                            │
                 │   - Redis token-bucket rate limit           │
                 │   - idempotent send lock per attempt         │
                 │   - circuit breaker around provider call      │
                 └──────────────────────────────────────────┘
                          │ success                │ failure
                          ▼                        ▼
                     DELIVERED            notifications.<channel>.retry
                    (Mongo status)                 │
                                          retryScheduler waits out
                                          exponential backoff, then
                                          republishes to primary topic
                                                     │
                                          max retries exceeded
                                                     ▼
                                          notifications.<channel>.dlq
                                          + DeadLetter record (Mongo)
```

### Why this shape

- **Priority processing** — each channel has three physical Kafka topics (`high`/`normal`/`low`), each consumed by its own consumer with priority-proportional concurrency, so a flood of low-priority notifications can never starve high-priority ones (a single topic with a "priority" field would still serialize behind FIFO offset order for one consumer group).
- **Rate limiting** — a Redis-backed token bucket (atomic Lua script) per channel throttles how fast workers call out to providers, independent of how fast Kafka can hand them messages.
- **Independent delivery workers** — email/SMS/push each run as separate consumer groups and separate Node processes, so scaling or restarting one channel never touches the others.
- **Idempotency** — two layers: (1) an ingestion-time idempotency key (caller-supplied or derived from channel+recipient+payload) guarantees the same logical request is only ever queued once; (2) a per-attempt send lock guarantees Kafka's at-least-once redelivery (e.g. after a worker crash before offset commit) never causes a duplicate provider call.
- **Exponential backoff** — failed sends are republished to a per-channel `retry` topic carrying a `deliverAt` timestamp; a dedicated retry-scheduler worker holds each message until its backoff window elapses (full-jitter exponential backoff) before releasing it back to the primary topic.
- **Provider failure isolation** — a Redis-backed circuit breaker per provider trips after N consecutive failures and fails fast for a cooldown window, so a struggling provider can't burn worker capacity meant for healthy channels.
- **Dead-letter processing** — after `MAX_RETRIES` attempts, the notification is marked `DEAD_LETTERED`, written to a `DeadLetter` collection for inspection/replay, and published to `notifications.<channel>.dlq` for any downstream alerting.

## Project layout

```
src/
  config/        env, Redis, Mongo config/clients
  kafka/         Kafka client, topic names, producer, admin/topic setup
  models/        Notification, DeadLetter (Mongoose)
  api/           Express API (routes, validators, error handling)
  services/      notificationService, idempotencyService, rateLimiter, circuitBreaker
  providers/     mock email/SMS/push providers (swap for SendGrid/Twilio/FCM later)
  workers/       baseWorker (shared logic) + email/sms/push/retry/dlq entrypoints
  utils/         logger, backoff
scripts/         createTopics.js, seed.js (demo load generator)
tests/           Jest unit tests (backoff, idempotency key, topic naming)
```

## Running it

```bash
# 1. Start infra
docker compose up -d

# 2. Install deps
npm install

# 3. Create Kafka topics
npm run setup:topics

# 4. Start the API + all workers (one process each)
npm run dev
# or individually:
npm run start:api
npm run start:worker:email
npm run start:worker:sms
npm run start:worker:push
npm run start:worker:retry
npm run start:worker:dlq
```

Kafka UI is available at http://localhost:8080 to watch topics/consumer groups live.

### Send a notification

```bash
curl -X POST localhost:3000/api/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"email","priority":"high","recipient":"a@example.com","payload":{"subject":"Hi","body":"Hello!"}}'
```

### Check status / DLQ

```bash
curl localhost:3000/api/notifications/<notificationId>
curl localhost:3000/api/notifications/dlq/list
```

### Generate demo load

```bash
node scripts/seed.js 50
```

Mock providers randomly fail (rates configurable in `.env`) so you can watch retries, backoff, and occasional dead-lettering happen against real Kafka/Redis/Mongo.

## Testing

```bash
npm test
```

## Configuration

See `.env.example` for all tunables: rate limits per channel, retry/backoff bounds, idempotency TTL, mock provider failure rates, and circuit breaker thresholds.

## Swapping in real providers

Replace the three files under `src/providers/*` with real SDK calls (SendGrid/SES for email, Twilio for SMS, FCM/APNs for push) that throw on failure and resolve `{ provider, messageId, ... }` on success — everything else (rate limiting, retries, circuit breaking, DLQ) is provider-agnostic and needs no changes.
