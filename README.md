# Distributed Job Processing Engine

A job queue built from scratch to understand the failure modes of asynchronous systems — durability, retries, dead-lettering, distributed fan-in, and what saturation actually looks like on a dashboard.

**Node.js · Express · BullMQ · Redis · Docker · k6**

---

## Why this exists

HTTP requests are a bad place to do slow work. A request that triggers a 30-second task holds a connection open, times out at the proxy, and loses all progress if anything restarts.

This engine accepts work over HTTP, acknowledges instantly, executes it in a separate worker pool, and survives crashes without losing jobs. It was built in six weekly slices, each shipped against an explicit definition of done before the next began — not because the world needs another queue (BullMQ already exists and is excellent), but because **the semantics of failure are the part you only understand after implementing them yourself.**

---

## Architecture

```
   client
     │  POST /jobs · POST /jobs/batch     ← returns 202 + id immediately
     ▼
┌──────────────┐    enqueue     ┌────────────────────────┐
│  API         │ ─────────────► │        Redis           │
│  (Express)   │                │  jobs · jobs-dead      │
│  never does  │ ◄───────────── │  batch results         │
│  the work    │  status/stats  │  metrics               │
└──────────────┘                └────────────────────────┘
                                    ▲     ▲     ▲
                            consume │     │     │
                              ┌─────┴──┬──┴──┬──┴─────┐
                              │ worker │ ... │ worker │  N independent processes
                              └────────┴─────┴────────┘
```

The API and the workers never talk to each other. **Redis is the only coupling point** — which is what makes workers disposable: add them, kill them, restart them, and queued work is unaffected.

| File | Responsibility |
|---|---|
| `api.js` | HTTP surface. Enqueues and reports status. Does no work. |
| `worker.js` | Job executor. Runs as N independent OS processes. |
| `queue.js` | Primary work queue (`jobs`). |
| `deadQueue.js` | Dead-letter queue (`jobs-dead`). Written to, never consumed. |
| `store.js` | Batch coordination state and metrics in Redis. |
| `redis.js` | Connection configuration. |
| `load-test.js` | k6 load profile. |

---

## Quick start

```bash
docker compose up --build                  # 1 API + 1 worker + Redis
docker compose up --build --scale worker=8 # 8 workers
```

Submit a job:

```bash
curl -X POST localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d '{"ms":3000}'
# → {"id":"1"}

curl localhost:3000/jobs/1
# → {"id":"1","state":"completed","result":{"ok":true,"ranFor":3000},"error":null}
```

Submit a chunked batch (sums 1..400 across 4 parallel chunks):

```bash
curl -X POST localhost:3000/jobs/batch \
  -H "Content-Type: application/json" \
  -d '{"total":400,"chunkSize":100,"ms":2000}'
# → {"parentId":"p_...","chunks":4}

curl localhost:3000/jobs/batch/p_...
# → {"state":"completed","total":80200,"okChunks":4,"failedChunks":[]}
```

Watch it saturate:

```bash
k6 run load-test.js
watch -n 2 curl -s localhost:3000/stats
```

### Running without Docker

Requires Node 20+ and a Redis instance on `localhost:6379`.

```bash
npm install
node api.js        # terminal 1
node worker.js     # terminals 2..N — one process per terminal
```

---

## API

| Endpoint | Description |
|---|---|
| `POST /jobs` | `{ms, failFor?, attempts?}` → `202 {id}` |
| `GET /jobs/:id` | `waiting \| active \| completed \| failed` + result or error |
| `POST /jobs/batch` | `{total, chunkSize, ms, failChunkIndex?}` → `202 {parentId, chunks}` |
| `GET /jobs/batch/:id` | Batch progress, or aggregate + `failedChunks[]` |
| `GET /stats` | Throughput, p50/p95 service time, p50/p95 **queue wait**, queue depth, DLQ count |
| `GET /dlq` | Inspect permanently-failed jobs |

The fake job is a `sleep(ms)` — deliberately, so the engine could be built and tested without codec or I/O noise. `failFor` and `failChunkIndex` force deterministic failures for testing retry and partial-failure paths.

---

## Design decisions

**Workers are separate processes, not `concurrency: N`.** Process isolation means one crash doesn't take down the pool, and it mirrors how you'd scale containers. Worth being honest: for purely I/O-bound work like this, in-process concurrency is the cheaper lever and would give the same parallelism. Processes earn their cost when work is CPU-bound, since Node is single-threaded.

**Exponential backoff, not fixed delay.** Retries at ~1s, 2s, 4s. A fixed one-second retry is just polling a broken service at high frequency — it converts your error handling into load, which is how a brief outage becomes a long one.

**A dedicated dead-letter queue, not the built-in failed set.** "Failed once, will retry" and "gave up for good" are operationally different states. Mixing them makes triage guesswork. A DLQ isn't a graveyard — it's an inbox that turns invisible failures into a countable number you can alert on.

**Fan-in counts distinct chunks, not increments.**

```js
async function recordChunkOutcome(parentId, index, outcome) {
  await redis.hset(key(parentId, 'results'), String(index), JSON.stringify(outcome));
  return redis.hlen(key(parentId, 'results'));   // redelivery-proof
}

const setFinal = (parentId, obj) =>
  redis.set(key(parentId, 'final'), JSON.stringify(obj), 'NX');  // first writer wins
```

An earlier version used `INCR`. It worked until a worker died mid-chunk: BullMQ redelivered the job, the increment fired twice for the same chunk, the counter overshot the total, and the batch hung in `processing` forever. `HSET` on the same index is idempotent, so `HLEN` doesn't grow on redelivery. `SET NX` guarantees a single finalization. Verified by `kill -9`ing a worker mid-batch and confirming the batch still completes with the correct total.

**Partial success is a first-class outcome.** The counter tracks *resolutions*, not successes; a permanently-failed chunk is recorded too, so the parent always completes. Count only successes and one bad chunk deadlocks the batch forever. A batch with 3 failures out of 500 reports exactly that, rather than discarding 497 good results.

**Metrics live in Redis.** In-process counters would fragment across worker processes and die on restart.

**Bounded graceful shutdown.** SIGTERM stops job intake and drains in-flight work, with a force-exit timer so a hung job can't block a deploy indefinitely.

---

## Load test results

Uniform ~500ms jobs, k6 ramping arrival rate to ~15/s, 497 samples per run. One variable changed: worker count.

| | 2 workers | 8 workers |
|---|---|---|
| p50 **queue wait** | 24,999 ms | 3 ms |
| p95 **queue wait** | 64,804 ms | 6 ms |
| p50 / p95 **service time** | 501 / 502 ms | 501 / 502 ms |
| API p95 (`http_req_duration`) | 5.52 ms | ~same |
| Failed requests | 0% | 0% |

The rows that *didn't* move matter more than the one that did.

**Service time never budged.** 501ms saturated, 501ms idle. The work never got slower — there was simply nobody free to start it.

**The API stayed green.** Every request answered in under 6ms with zero errors, while the median job waited 25 seconds in the queue. Health checks passing, latency graphs flat.

> Under saturation, latency doesn't degrade — it queues. A system can be failing badly while every response-time graph looks perfect.

The ceiling is arithmetic, not tuning: `throughput = workers × (1 / service time)`. Two workers at 500ms is 4 jobs/sec. Push arrivals past that and the backlog isn't slow — it's unbounded. You need queue depth and time-in-queue on a graph or you won't see it coming.

---

## Known limitations

Stated deliberately. These are understood trade-offs, not oversights:

- **At-least-once delivery.** A worker that crashes after doing the work but before acknowledging causes a re-run. Fan-in coordination is idempotent; the *work itself* isn't. Anything with real side effects would need an idempotency key.
- **No jitter on backoff.** Mass failure means retries in lockstep — a politer storm is still a storm. First thing I'd add.
- **No reaper for stalled batches.** BullMQ's stall detection covers worker death, but a batch whose chunks never reach a terminal state has no deadline.
- **No input validation.** Request bodies are trusted; `attempts` is client-controlled and should be capped server-side.
- **Unbounded state growth.** Completed jobs and `parent:*` keys are never expired. Needs retention policies at volume.
- **No auth.** Every endpoint is open, including `/stats` and `/dlq`.
- **Redis is a single point of failure.** AOF is enabled in compose, but there's no replication.
- **No automated tests.** Verified manually and via load and chaos testing.

---

## What I'd do next

Input validation and an `attempts` cap · jitter on backoff · integration tests for the retry→DLQ and partial-batch paths · structured JSON logging with a job correlation ID · `/health` and `/ready` endpoints · job retention policies.
