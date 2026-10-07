# Distributed Notification System (learning build)

An asynchronous, multi-channel (email / SMS / push) notification system built step-by-step with Node.js, Kafka, Redis, and MongoDB — as a deliberate learning exercise, not a one-shot generated project. Every file here was written (and debugged) line by line, with the reasoning and alternatives considered documented as we went.

**This README reflects the actual current state of the project, not the end goal.** See the status table below for what's really done vs. still to come.

## Why this exists

A naive "call the email/SMS provider directly from the request handler" approach breaks down fast: slow providers slow down your whole API, failures have no retry story, duplicate retries can send the same notification twice, and a flood of traffic can get you rate-limited or banned by the provider. The fix is to decouple *accepting* a notification request from *delivering* it: an API quickly queues the work, and independent background workers do the actual sending, with proper handling for retries, duplicates, and failures.

## Architecture (current + planned)

```mermaid
flowchart LR
    Client -->|POST /api/notifications| API[Express API]
    API -->|1. build idempotency key| Idem[(Redis:\nidempotency keys)]
    API -->|2. claim key atomically| Idem
    API -->|3. publish message| Kafka{{Kafka}}

    subgraph Kafka Topics
        T1[notifications.email.high/normal/low]
        T2[notifications.sms.high/normal/low]
        T3[notifications.push.high/normal/low]
    end

    Kafka --> T1
    Kafka --> T2
    Kafka --> T3

    T1 --> W1[Email Worker]
    T2 --> W2[SMS Worker]
    T3 --> W3[Push Worker]

    W1 -->|rate limit check| RL[(Redis:\nrate limiter)]
    W2 -->|rate limit check| RL
    W3 -->|rate limit check| RL

    W1 -. circuit breaker, not built yet .-> P1[Email Provider mock]
    W2 -. circuit breaker, not built yet .-> P2[SMS Provider mock]
    W3 -. circuit breaker, not built yet .-> P3[Push Provider mock]

    W1 -. on failure .-> Retry[notifications.*.retry]
    Retry -. exhausted .-> DLQ[notifications.*.dlq]
```

Solid lines/boxes = built and verified. Dashed lines/boxes = designed, not yet implemented.

## Status

| # | Piece | Status |
|---|---|---|
| 1 | Local infra (Kafka, Redis, MongoDB via Docker Compose) | ✅ Done |
| 1 | Priority modeling (separate topic per channel × priority) | ✅ Done |
| 2 | Ingestion API (`POST /api/notifications`) | ✅ Done |
| 2 | Idempotency (claim-before-publish, release-on-failure) | ✅ Done |
| 3 | Rate limiting (token bucket, atomic via Redis Lua script) | ✅ Done |
| 4 | Delivery workers (email/SMS/push consumers) | ✅ Done |
| 5 | Retry with exponential backoff | ⬜ Not started |
| 6 | Circuit breaker for provider failure isolation | ⬜ Not started |
| 7 | Dead-letter queue handling | ⬜ Not started |
| 8 | MongoDB persistence (durable notification status) | ⬜ Not started |

## What's actually implemented right now

- `docker-compose.yml` — Kafka (+ Zookeeper), Redis, MongoDB, on non-default ports (`9093`, `6380`, `27018`) to avoid colliding with other local projects.
- `src/kafka/topics.js` — topic naming: `notifications.<channel>.<priority>`, channels = `email|sms|push`, priorities = `low|normal|high`.
- `src/kafka/client.js`, `src/kafka/producer.js` — Kafka connection + a minimal `publish(topic, message)` helper.
- `src/config/env.js`, `src/config/redis.js` — centralized env config, shared Redis client.
- `src/services/idempotency.js` — `buildIdempotencyKey` (caller-supplied key or a derived SHA-256 hash of channel+recipient+payload), `claimIdempotencyKey` (atomic Redis `SET ... NX EX`), `releaseIdempotencyKey` (rollback on publish failure).
- `src/api/routes/notifications.js`, `src/api/server.js` — `POST /api/notifications` validates input, claims the idempotency key, publishes to the right topic, rolls back the claim if publishing fails. Returns `202` (queued), `400` (bad input), `409` (duplicate), or `500`.
- `src/services/rateLimiter.js` — `tryConsume(channel)`, a token bucket rate limiter per channel. The refill math (elapsed time × rate, capped at capacity) runs entirely inside a Redis Lua script via `EVAL`, so the whole "read bucket state, compute refill, decide, write back" sequence is one atomic operation - verified by `scripts/testRateLimiterRace.js`, which fires 20 concurrent requests at a bucket with capacity 5 and confirms exactly 5 get through.
- `src/kafka/consumer.js`, `src/workers/baseWorker.js` — generic Kafka consumer wrapper + per-channel worker factory. Each channel runs **three** separate consumers (one per priority), each its own consumer group, each with a different `partitionsConsumedConcurrently` (high=3, normal=2, low=1) - real priority-based concurrency, not just a field on the message.
- `src/providers/{email,sms,push}Provider.js` — mock providers simulating network latency and a configurable random failure rate, so later steps have real failures to react to. Swappable for real Twilio/SendGrid/FCM calls later without touching worker code.
- `src/workers/{email,sms,push}Worker.js` — entrypoints, run via `npm run start:worker:<channel>`.
- **Known gap, by design, to be fixed in Step 5:** right now a rate-limited or failed send is just logged and dropped - nothing retries it yet.

## Key design decisions made so far (and why)

- **Separate Kafka topics per priority**, not a single topic with a priority field — Kafka only guarantees order within a partition by write order, so a priority field on a shared topic can't actually jump the queue without extra machinery. Separate topics + separate consumers per priority gives real prioritization.
- **Idempotency uses one atomic Redis `SET NX` call**, not a separate `GET` then `SET` — two round trips re-open a race condition where two concurrent duplicate requests could both see "not claimed yet" before either writes.
- **Claim the idempotency key *before* publishing, with an explicit rollback if publishing fails** (rather than claiming after success) — this closes the window where two concurrent duplicate requests could both slip through, at the cost of needing explicit cleanup on failure. See the idempotency section of our build log for the full tradeoff discussion.
- **Rate limiting's token-bucket math runs inside a Redis Lua script (`EVAL`), not as separate JS-side `GET`/`SET` calls** — same reasoning as idempotency: two round trips re-open a race condition (proven empirically: a non-atomic first draft let all 20 of 20 concurrent requests through against a bucket of capacity 5). The Lua script also asks Redis for the current time via `TIME` rather than using each caller's own clock, so results stay consistent across multiple worker processes/machines.

## Running it locally

```bash
docker compose up -d
npm install
cp .env.example .env   # then edit if your ports differ
npm run setup:topics
node src/api/server.js
```

Test it:

```bash
curl -i -X POST localhost:3000/api/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"email","priority":"high","recipient":"you@example.com","payload":{"subject":"test"},"idempotencyKey":"demo-1"}'
```

Send the exact same request again and you should get `409 duplicate notification` instead of a second `202`.

## Next up

Retry with exponential backoff (so a failed or rate-limited send gets re-queued instead of dropped), then circuit breakers and dead-letter handling.
