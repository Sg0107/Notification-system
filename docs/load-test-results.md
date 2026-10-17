# Load test results

Produced with `node scripts/loadTest.js <countPerCombo>`. Each run fires
`countPerCombo` notifications at every channel x priority pair (9 pairs) at
once, then watches the dashboard stats until each channel's worker has
processed everything it was sent.

## Environment

- Apple M5, 10 logical cores, 16 GB RAM, macOS (arm64), Node v24.15.0
- Kafka, Redis and MongoDB in Docker on the same machine; API and the three
  workers each run as a single process (`npm run dev`)
- `SEND_REAL_EMAILS=false`: all three providers are mocked. Simulated
  failure rates: email 10%, SMS 15%, push 5%. Max 5 retries.
- Requests are all fired concurrently (no concurrency cap).

## Run set 1: original rate limits (email 10/s, SMS 5/s, push 20/s)

| Per pair | Total | Accepted | Ingestion | Latency p50 / p99 | Email | SMS | Push |
|---|---|---|---|---|---|---|---|
| 50 | 450 | 450 | 1,059/s | 242 ms / 395 ms | 150 in 26.2 s | 150 in 38.3 s | 150 in 9.1 s |
| 500 | 4,500 | 4,473 | 498/s | 4.1 s / 7.3 s | not captured | not captured | not captured |
| 1000 | 9,000 | 7,737 | 663/s | 5.7 s / 11.0 s | 2,209 in 182 s | 2,820 in 678 s | 2,708 in 159 s |

- 50 per pair: no failures, nothing dead-lettered.
- 500 per pair: 27 requests failed (all email, low priority). The run was
  stopped during the drain, so there are no drain numbers.
- 1000 per pair: 1,263 of 9,000 requests failed. Firing 9,000 requests in
  one instant is far beyond a realistic burst for a single Node process on a
  laptop; the failure cause was not diagnosed. The per-channel sent counts
  in that run do not reconcile cleanly with the accepted counts.
- Drain time was set by the rate limit: SMS at 5/s needs about 10 minutes
  for 3,000 messages.

## Run set 2: all rate limits 20/s (data wiped first)

| Per pair | Total | Accepted | Ingestion | Latency p99 | Email | SMS | Push |
|---|---|---|---|---|---|---|---|
| 10 | 90 | 90 | n/a | n/a | 30 in 29.2 s | 30 in 32.2 s | 30 in 27.2 s |
| 50 | 450 | 450 | n/a | n/a | 150 in 10.1 s | 150 in 13.1 s | 150 in 8.0 s |
| 100 | 900 | 900 | 1,278/s | 674 ms | 298 of 300, timed out | 300 in 36.3 s | 300 in 16.1 s |
| 200 | 1,800 | 1,800 | 1,018/s | 1,685 ms | 599 of 600, timed out | 600 in 38.8 s | 600 in 36.8 s |

- No dead-lettered notifications in any step.
- SMS and push drain at roughly 15-19/s, close to the 20/s limit.
- The 10-per-pair numbers (about 1/s) mostly reflect the retry backoff on
  the few simulated failures, not throughput.

## Run set 3: after the Mongo-before-publish fix (all rate limits 20/s)

Same setup as run set 2, run against a clean dashboard (all earlier documents
had `active: false`). Steps ran back to back.

| Per pair | Total | Accepted | Ingestion | Latency p50 / p99 | Email | SMS | Push |
|---|---|---|---|---|---|---|---|
| 10 | 90 | 90 | 616/s | 66 ms / 136 ms | 30 in 4.0 s | 30 in 3.0 s | 30 in 1.0 s |
| 20 | 180 | 180 | 698/s | 109 ms / 244 ms | 60 in 4.0 s | 60 in 4.0 s | 60 in 5.1 s |
| 50 | 450 | 450 | 1,289/s | 176 ms / 324 ms | 150 in 10.1 s | 150 in 9.1 s | 150 in 7.1 s |
| 100 | 900 | 900 | 1,318/s | 309 ms / 648 ms | 300 in 17.2 s | 300 in 29.3 s | 300 in 16.1 s |
| 200 | 1,800 | 1,800 | 1,041/s | 676 ms / 1.6 s | 600 in 40.5 s | 600 in 39.5 s | 600 in 31.4 s |
| 300 | 2,700 | 2,700 | 934/s | 1.2 s / 2.8 s | 900 in 63.1 s | 900 in 71.3 s | 900 in 46.8 s |
| 400 | 3,600 | 3,600 | 690/s | 2.0 s / 4.9 s | 1,200 in 66.8 s | 1,200 in 92.5 s | 1,200 in 64.7 s |
| 500 | 4,500 | 4,500 | 900/s | 2.0 s / 4.8 s | 1,500 in 95.4 s | 1,500 in 103.7 s | 1,500 in 78.9 s |
| 1000 | 9,000 | 8,812 | 902/s | 4.0 s / 9.4 s | 3,000 in 177 s | 2,812 in 195 s | 3,000 in 152 s |

- Every step drained fully and nothing was left `queued`. Final totals:
  23,031 sent, 1 dead-lettered (an SMS at the 400 step, retries exhausted
  against the 15% simulated failure rate).
- 188 of the 9,000 requests at the 1000 step failed (SMS only); every other
  step accepted all of its requests. The cause of those failures was not
  investigated.
- Push runs at 18-20/s; email and SMS at about 14-17/s, a little under the
  20/s limit.
- Latency grows with burst size because every request arrives at once on a
  single API process.

## Emails left as `queued` (found in run set 2, fixed)

Three emails (normal/low priority, `attempts: 0`) stayed `queued` in Mongo
after the 100- and 200-per-pair steps of run set 2, which is why the email
drain timed out in both. The API used to publish to Kafka before inserting
the Mongo document, so a fast worker could call `markSent` before the insert
existed; that update matched nothing and the document stayed `queued`. They
were most likely delivered, with a stale status in the dashboard.

Fixed in `ccea05f`: the API now creates the document before publishing, and
marks it `failed` (never deletes it) if the publish fails. Run set 3 shows no
stuck documents across 23,000 notifications.

## Caveats

- One machine, one process per service, mocked providers: these numbers
  show the pipeline and rate-limit behavior, not production capacity.
- The drain timeout in the script scales with volume (3 s per
  notification-per-pair, minimum 2 minutes).
