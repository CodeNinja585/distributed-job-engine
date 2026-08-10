# Distributed Job Engine — Staff Engineer Audit, Interview Prep & Gap Analysis

---

## 0. The verdict first

**What a CTO actually thinks:** this is well above a typical portfolio project, and the reason has nothing to do with the queue. Plenty of people can wire up BullMQ from a tutorial. Very few can say *"here's the bottleneck I hypothesised, here's the load test, here's the before/after number, and here's why response time stayed green the whole time the system was failing."* That measurement discipline is the asset. Lead with it.

**What it is not:** a production system. It's a single-node demo with no auth, no validation, no tests, and unbounded state growth. That's *fine* — nobody expects a 6-week learning project to be production-ready — but do not present it as one. Present it as *"a systems study where I built each failure-handling primitive by hand instead of inheriting it."* That framing is honest and it's stronger.

**The one thing that would embarrass you in a code review:** there is a real, non-theoretical bug in the fan-in path that causes batches to hang forever (§1.1). Fix it before anyone reads the code.

**Honest calibration:** this project demonstrates *systems reasoning*, not *production engineering*. Those are different hiring signals. If the role is backend/platform, you're in good shape. If the role is "own a service on-call," you'll need to show the operational layer too — but adding it to *this* project is lower value than being fluent about why it's absent.

---

## 1. Staff Engineer audit checklist

### 1.1 Correctness — the highest-severity findings

**🔴 CRITICAL — the fan-in counter is not idempotent, and the equality check turns that into a permanent hang.**

Two defects compound:

1. `recordChunkOutcome` does `HSET` then `INCR` as two separate round-trips. A crash between them leaves the result recorded but uncounted.
2. If a worker dies mid-chunk, BullMQ marks the job *stalled* and re-runs it. The re-run calls `recordChunkOutcome` again → `INCR` fires a **second time** for the same chunk → the counter reaches `total + 1`.

And the aggregation trigger is strict equality:

```js
if (!p || done < p.total) return;
```

`total + 1 !== total`, so the aggregation **never fires**. The batch reports `processing` forever and the client polls into the void. The retry doesn't heal the batch — it *guarantees* the hang.

- **Immediate mitigation:** change `!==` to `<`. Converts a permanent hang into eventual completion. ~30 seconds of work.
- **Correct fix:** move record-and-count into a single Lua script so `HSET` + `INCR` + the completion check execute atomically, and key the count off *distinct chunk indices* (`HLEN` of the results hash) rather than a blind increment. `HSET` on the same index twice is naturally idempotent; `INCR` is not.
- **Belt and braces:** a reaper that force-fails parents past a deadline, so no batch can hang indefinitely regardless of cause.

**🟠 Aggregation itself is not idempotent.** Once the trigger can fire more than once (which the `<` mitigation permits), two workers could both aggregate. `setFinal` would be written twice. Harmless today because the computation is deterministic — genuinely dangerous the moment aggregation has a side effect (notification, payment, downstream write). Guard it with `SET ... NX` so the first writer wins.

**🟠 Chunk boundary arithmetic is untested.** `start = index * chunkSize + 1`, `end = min((index+1) * chunkSize, total)`. This is exactly the shape of code that's off by one when `total % chunkSize !== 0`. It happens to be right; nothing proves it stays right. This is the single best candidate for a unit test (§1.6).

**🟡 Metrics only record successes.** `recordCompletion` is called after the processor returns, so failed jobs contribute no duration sample. Your latency percentiles describe *the jobs that worked*. In a real incident, the slow path is usually the failing path — meaning your metrics would be blindest exactly when you need them most.

### 1.2 Fault tolerance

| Check | Status |
|---|---|
| Job survives worker death | ✅ Verified — jobs persist in Redis, resume on restart |
| Transient failure → retry with backoff | ✅ Exponential, configurable attempts |
| Permanent failure → isolated for triage | ✅ Dedicated DLQ with reason, attempt count, timestamp |
| Partial batch failure tolerated | ✅ Counter tracks resolutions, not successes — correct design |
| In-flight work survives shutdown | ✅ Drain + bounded force-exit |
| Stalled job recovery | ⚠️ Works, but triggers the §1.1 bug |
| Orphaned parent detection | ❌ None — a chunk that never reaches a terminal state strands its parent forever |
| Poison-pill handling | ❌ Input that can never succeed still burns full retries with growing delays. No classification of retryable vs. non-retryable errors — a malformed payload gets the same patient treatment as a network blip |
| Retry storm protection | ❌ No jitter. Mass failure → lockstep retries → the recovering dependency gets flattened again |
| Delivery semantics documented | ⚠️ At-least-once in practice; not stated in code or README, and no idempotency keys, so a crash after side-effect-but-before-ack causes duplicate work |

**The gap that matters:** everything here is about *transient* failure. There is no handling for *permanent* failure of a chunk's worker — which is the exact scenario your architecture is most exposed to.

### 1.3 Observability

**Genuinely good:** you measure *queue wait* separately from *service time*. Most engineers with five years of experience don't, and it's why your Week 6 conclusion was correct rather than lucky. Keep this front and centre.

**Problems:**

- **`jobsPerSec` is silently wrong under load.** It counts entries in a list capped at 1000 by `LTRIM`, divided by 60. Above ~1000 jobs/minute the list saturates and throughput reads a ceiling of 16.67/s no matter how fast you actually go. Your throughput metric breaks precisely when throughput becomes interesting.
- **Percentiles are over "the last 1000 jobs," not a time window.** Under mixed workloads the sample is polluted (your own before/after run demonstrated this — 60s p95 sitting next to 5ms p50 because two runs shared a buffer). Percentiles need a bounded time window to be meaningful.
- **`/stats` is O(n log n) per request.** Full `LRANGE` of 1000 elements plus two sorts, every scrape. Fine at your scale, wrong pattern at any real one — this is what histogram buckets exist for.
- **Counters are cumulative and never reset**, with no scrape timestamp. You can't derive a rate from two samples without knowing when they were taken.
- **`console.log` only.** No levels, no structure, no correlation ID. You cannot answer "what happened to job 2847?" without grepping four terminals. A single `jobId` field in JSON logs would make the system traceable.
- **No health endpoints.** Nothing to distinguish "process is up" from "process can reach Redis and is consuming."

### 1.4 Security

Everything here is a real gap, but almost all of it is *appropriately* deferred for the project's scope. The exceptions:

- **🟠 `attempts` is client-controlled.** `POST /jobs {"attempts": 1000000}` is a denial-of-service primitive handed to any caller. This one isn't "missing auth," it's a design flaw — resource limits should never be caller-specified without a server-side cap.
- **🟠 `ms` is unvalidated and untyped.** A string, a negative, or `Number.MAX_SAFE_INTEGER` all reach `setTimeout` unchallenged. `{"ms": 999999999}` occupies a worker effectively forever.
- **🟠 `GET /dlq` returns every dead job with full payloads, unpaginated.** Both an unbounded-memory endpoint and an information-disclosure surface, since payloads may contain whatever callers submitted.
- **🟡 No auth on `/stats` or `/dlq`** — operational endpoints exposing internal state.
- **🟡 Redis has no ACLs and no password.** In a shared environment, any client can `FLUSHALL`. Your CTO's point was directionally right even if the framing (Redis vs RabbitMQ) wasn't — the fix is access control, not a different broker.

### 1.5 Performance

**The Week 6 conclusion is correct.** Arrival rate exceeded service capacity, backlog grew unboundedly, wait time exploded while service time stayed flat. That's textbook and you proved it with numbers.

**But you pulled the expensive lever.** Your jobs are `sleep()` — pure I/O, no CPU. For I/O-bound work, `concurrency: N` inside one process gives you the same parallelism as N processes at a fraction of the memory and connection cost. Eight processes for I/O-bound work is 8× the Node runtimes and 8× the Redis connections to buy something one flag would have given you.

This isn't wrong — process isolation is a legitimate reason to prefer it, and it does mirror pod scaling. But **an interviewer will ask why you chose processes over concurrency, and "I wanted true isolation" is only half the answer.** The full answer is: *"For I/O-bound jobs, concurrency is the cheaper lever and I'd reach for it first. Processes matter when the work is CPU-bound — since Node is single-threaded, concurrency buys you nothing there. My fake job is a sleep, so honestly concurrency was the right first move and processes were the right *demonstration*."* Say that and you've turned a soft spot into a strong answer.

**Other findings:**

- **Redis will grow without bound.** BullMQ keeps completed and failed jobs by default. You're at job ID ~3050 already with none of them cleaned. Set `removeOnComplete` / `removeOnFail` with sensible retention.
- **`parent:*` keys are never expired.** Every batch leaks three keys forever. Needs a TTL.
- **The DLQ is a graveyard, not a recovery tool.** Nothing dequeues it, nothing re-drives it, nothing expires it. Add re-drive and retention or it becomes a slow leak.
- **Enqueue loop is sequential.** `for (...) { await queue.add(...) }` — N sequential round-trips. `addBulk` is one. Irrelevant at 8 chunks; matters at 8,000.
- **Redis is a single point of failure** with default persistence (RDB snapshots, no AOF). An unclean shutdown loses the window since the last snapshot.

### 1.6 Testing — the weakest dimension, and the highest-signal fix

You have zero automated tests. Everything was verified by eye in a terminal. Specifically what to write, in priority order:

**1. Unit tests (no Redis, milliseconds to run)**
- **Chunk boundary math.** Assert coverage and non-overlap for `total=100, chunkSize=25` (clean), `total=100, chunkSize=30` (remainder), `total=7, chunkSize=10` (single short chunk), `total=0`. This is where off-by-one bugs live and it's pure arithmetic — trivial to test, embarrassing to get wrong.
- **Percentile calculation.** Known input array → known p50/p95. Include the empty-array case, which currently returns 0 and would silently report "0ms latency" for a dead system.

**2. Integration tests (real Redis — Testcontainers, or a dedicated DB index)**
- Enqueue → poll → `completed` with the correct result.
- Failing job → retries the configured number of times → lands in DLQ with reason intact.
- Batch with one permanently-failing chunk → parent still completes, `okChunks` and `failedChunks` both correct. **This is your single most valuable test** — it's the invariant your whole Week 4 design rests on.
- Unknown ID → 404.

**3. The idempotency test that would have caught your live bug**
- Process the same chunk twice (simulating a stalled-job retry). Assert the parent still completes with the correct total and the counter doesn't overshoot. **This test fails against your current code.** Write it, watch it fail, then fix §1.1. That's the story to tell in an interview.

**4. Chaos test**
- `SIGKILL` a worker mid-job (not `SIGTERM` — you've tested the graceful path, not the violent one). Assert the job is re-delivered and the batch still completes. This is the difference between "I handled shutdown" and "I handled crashes."

**5. Load regression in CI**
- The k6 script with a threshold (`p95 wait < X ms` at a fixed worker count). Turns your Week 6 finding into a guardrail that catches performance regressions instead of a one-time demo.

### 1.7 Operational excellence

- ❌ **No README.** Right now your design doc is better than most repos' entire documentation, and it isn't the first thing a reader sees. This is the highest-ROI missing artifact in the whole project.
- ❌ **No Docker Compose.** Running this requires six terminals and tribal knowledge. `docker compose up` turns "trust me" into "clone and try it."
- ❌ **No CI.** One GitHub Action running the test suite is enough. Full CD is not the point.
- ❌ **No config management.** Redis host, port, worker concurrency, retention are hardcoded. Env vars with defaults.
- ❌ **No runbook.** "Queue depth is climbing — what do I check first?" should have a written answer.
- ⚠️ **Graceful shutdown doesn't close the store's Redis client**, though `process.exit` papers over it. Worth tidying.

---

## 2. Interview questions

### Week 1 — durable async queue

**Tier 1 — mechanics**
- Walk me from `POST /jobs` to the client's response. What's in Redis afterward?
- Why does the API respond before the work is done? What does 202 promise that 200 doesn't?
- Why is the worker a separate process rather than code inside the API?

**Tier 2 — design defense**
- Why a queue at all for a 3-second task? What specifically breaks if you do it inline?
- Why Redis over a Postgres table with a `status` column? What does Redis give you that a DB polling loop doesn't?
- Why does the BullMQ Worker need its own Redis connection instead of sharing the API's?

**Tier 3 — failure modes**
- 🎯 You killed the worker, queued jobs, restarted, and they ran. Why didn't they vanish? What would have happened if the queue lived in the worker's memory?
- 🎯 Redis dies mid-flight. What's lost? What is your durability actually bounded by, given default persistence settings?
- Two workers are running. Can the same job execute twice? Is this at-least-once, at-most-once, or exactly-once — and how do you know?

### Week 2 — the async contract

**Tier 1**
- What does `GET /jobs/:id` do, step by step? Where do `result` and `error` come from — who wrote them?
- Why does the client poll instead of receiving the outcome directly?

**Tier 2**
- Why `throw` in the worker instead of `return { ok: false }`? What does throwing unlock?
- Why call `getState()` rather than tracking status in your own table?
- Why does an unknown ID hit Redis instead of a local set of seen IDs?

**Tier 3**
- 🎯 A job succeeds, then the worker crashes before the queue records completion. What state is the job in? What runs next, and what does that mean for a job with side effects?
- 🎯 Polling doesn't scale — a thousand clients hammering `GET /jobs/:id`. What replaces it, and what does each option cost? (webhooks / SSE / WebSockets — and the trade-offs of each)
- A completed job ages out of Redis and a client polls it. 404 or 200? Which is correct, and what does your answer imply about job retention?

### Week 3 — retries, backoff, DLQ

**Tier 1**
- Trace a failure from `throw` to either retry or DLQ. Who decides which?
- With `attempts: 3`, how many times does the job run? Which value is your give-up threshold — `attempts` or `attemptsMade`?
- Which parts here are BullMQ's and which did you write?

**Tier 2**
- Why a separate `jobs-dead` queue rather than BullMQ's built-in failed set?
- What's the point of a queue nothing consumes?
- Why retry at all? What are you assuming about the cause of failure — and when is that assumption false?

**Tier 3**
- 🎯 A job charges a card, succeeds, then crashes before acknowledging. The retry charges again. How do you make retries safe to repeat? (idempotency keys, dedup windows, conditional writes)
- 🎯 A database dies; ten thousand jobs fail simultaneously and back off in perfect lockstep. The DB recovers and is immediately flattened again. What's missing from your backoff, and why does randomness fix it?
- Malformed input that can never succeed still consumed three attempts and two backoff windows. How would you classify errors as retryable vs. terminal, and what does that save you?
- A job sits in the DLQ. You fix the bug. How does it get re-run? Is your DLQ a recovery tool or a graveyard?

### Week 4 — fan-out / fan-in

**Tier 1**
- How many jobs does one batch request create, and who creates them?
- How does the parent know all chunks are finished? Show me the line.
- Why does `initParent` write `total` before any chunk is enqueued?

**Tier 2**
- 🎯 Why use the *return value* of `INCR` instead of `GET`-then-compare? Show me the race the second version has.
- Why does the counter track resolutions rather than successes? What happens to a batch containing a permanent failure if you count only successes?
- Why hand-roll coordination instead of using BullMQ Flows? What did you gain, and what did you give up?
- Your aggregator reads all M results the moment the counter hits M. Why is that safe without a lock? (single-threaded Redis; the M-th `INCR` implies all prior `HSET`s landed)

**Tier 3**
- 🎯 **The orphaned parent.** A chunk's worker dies and never reaches a terminal state, so its `INCR` never fires. What happens to the parent? How do you detect it and how do you recover? (And the sharper follow-up: what if the retry *does* fire the `INCR` a second time?)
- 🎯 A chunk records its result, then crashes before incrementing. Your two writes aren't atomic. Walk me through the fix. (Lua; or count distinct indices via `HLEN` rather than blind increments)
- One chunk takes 100× longer than the rest. What bounds your batch latency? What do real systems do about stragglers? (tail latency, speculative execution)
- This batch has 10,000 chunks instead of 4. What breaks first?
- Who cleans up `parent:*` keys? What does Redis look like after a million batches?

### Week 5 — scaling, metrics, shutdown

**Tier 1**
- Four workers, one queue. Who decides which worker gets which job?
- Why do metrics live in Redis rather than in each process?
- What does `worker.close()` actually do?

**Tier 2**
- 🎯 Why multiple processes instead of `concurrency: N` in one? Given that your jobs are `sleep()` — pure I/O — which lever is actually cheaper, and when would processes genuinely be the right call? (the honest answer: concurrency first for I/O; processes for CPU-bound work and isolation)
- Why a rolling window for p95 instead of histogram buckets? What breaks at scale?
- Why does graceful shutdown need a force-exit timer? What is an orchestrator's contract with your process?

**Tier 3**
- 🎯 Four processes finished chunks simultaneously and exactly one aggregated. What guaranteed that? What would break it?
- Your rolling window holds the last 1000 durations. At 5,000 jobs/minute, what does your p95 actually describe? What does `jobsPerSec` report, and why is it wrong?
- `SIGKILL` instead of `SIGTERM` — what happens to the in-flight job, and how does the system recover?
- You record duration only on success. What does that do to your latency numbers during an incident where the failing path is the slow path?

### Week 6 — load testing

**Tier 1**
- What does your k6 script actually apply — a fixed number of users or a fixed arrival rate? Why does that distinction matter?
- What's the difference between service time and wait time in your metrics?

**Tier 2**
- 🎯 Two workers, 500ms jobs, 15 requests/second arriving. Compute the capacity and predict the outcome before running anything. (2 workers × 2 jobs/s = 4/s vs 15/s arriving — backlog grows without bound; this is arithmetic, not mystery)
- Why did adding workers fix it? What would you have done if the bottleneck had been the API rather than the workers?
- Why measure percentiles rather than averages? What would the mean have hidden in your data?

**Tier 3**
- 🎯 Your API returned 202 in under 6ms with zero errors while jobs waited 60 seconds in the queue. Every health check was green. What does that tell you about monitoring response time, and what should you alert on instead?
- Under saturation, service time stayed flat at 501ms while wait time exploded. Explain why. (Little's Law — the system doesn't slow down, it queues)
- Load stops but the backlog persists. How long to drain, and what does that number tell you about capacity planning?
- What happens when the queue is so deep that jobs are irrelevant by the time they run? What do you do about it? (TTLs, load shedding, backpressure at the API — refuse work you can't complete in time)

---

## 3. What's missing — and what actually matters

### 3.1 Critical — fix before anyone reads the code

| Gap | Why it's critical | Effort |
|---|---|---|
| **Non-idempotent fan-in + strict equality** | A real bug. Stalled-job retry permanently hangs a batch. | 30 min (mitigation) / 2 hrs (Lua) |
| **Client-controlled `attempts`** | Trivial DoS vector. Cap server-side. | 15 min |
| **Input validation** | `ms` accepts strings, negatives, absurd values. Zod schema on both endpoints. | 1 hr |
| **Unbounded Redis growth** | `removeOnComplete`/`removeOnFail` + TTL on `parent:*`. Silent leak. | 30 min |

### 3.2 Worth doing — real value, low cost

| Gap | Why | Effort |
|---|---|---|
| **README with architecture diagram** | Highest ROI item in the entire project. First thing anyone sees. | 2 hrs |
| **Docker Compose** | Six-terminal setup → one command. Removes all friction from anyone evaluating it. | 1 hr |
| **Integration + idempotency tests** | Your weakest dimension. The idempotency test *fails today* — that's a great story. | 4 hrs |
| **Jitter on backoff** | Ten lines. Closes a gap you already explain well. | 15 min |
| **Structured logging with `jobId`** | Makes the system traceable instead of greppable. | 1 hr |
| **`/health` + `/ready`** | Trivial, and expected by anyone thinking about deployment. | 30 min |
| **One CI workflow running tests** | Proves the tests actually run. Full CD is not the point. | 1 hr |

### 3.3 Deliberately deferred — defensible, don't apologise

These were *named as limitations* in your design doc. That's the right call. Being able to say *"I know, here's the failure mode, here's the fix, here's why I scoped it out"* is a stronger signal than having silently implemented them:

- **Idempotency keys** — meaningless with a `sleep()` job that has no side effects. Explain when they'd be mandatory.
- **Lua atomic fan-in** — the correct fix, but understanding *why* the window exists is the learning; closing it is mechanical.
- **Reaper for orphaned parents** — the right answer to a problem you correctly identified.
- **Exactly-once semantics** — genuinely hard, arguably impossible without cooperation from the work itself. Knowing that is the point.

### 3.4 Over-engineering for this scope — skip these

- **Auth / RBAC / API keys.** No users, no tenants. Adding it demonstrates nothing about distributed systems. One README line saying "no auth; would use X" covers it.
- **Circuit breaker.** *There is no downstream service.* This appears on generic checklists; it does not apply to your architecture. Naming that is itself a good signal — it shows you read your own system instead of a template.
- **PagerDuty / Slack alerting.** Nobody is on call. The valuable version is defining *what you'd alert on* (queue depth, wait-time p95 — explicitly not response time), which is a design answer, not an integration.
- **Kubernetes manifests / full CD.** Deploying nowhere. Docker Compose covers the real need.
- **Rate limiting.** Real for a public API. Yours has one client — you. The `attempts` cap in §3.1 is the version that actually matters.
- **Prometheus / Grafana.** Your `/stats` endpoint already proves the concept. Wiring an exporter is plumbing, not insight.

### 3.5 What to do next — ranked

**If you have 8 hours,** in this order:

1. **Fix the fan-in bug** (§1.1). Non-negotiable — it's a live defect.
2. **Write the idempotency test that catches it.** Watch it fail, then fix, then watch it pass. That sequence is a genuinely good interview story.
3. **README + architecture diagram.** Highest visibility per hour spent.
4. **Docker Compose.** Removes every barrier to someone actually running it.
5. **Validation (Zod), the `attempts` cap, retention/TTL, jitter.** All small, all close gaps you can already articulate.

**If you have 40 hours,** add integration and chaos tests, structured logging, health endpoints, CI, and the Lua fan-in.

**But the real question is whether hour 40 on this project beats hour 1 on the next one.** My honest read: it doesn't. After the 8-hour list, this project has said everything it can say about you. A second project — different domain, different constraints — demonstrates *range*, which is what "can you learn our stack" actually tests. Polish, then move.

---

## 4. How to present this

**Lead with the problem, not the architecture.** "Slow work doesn't belong in an HTTP request" — timeouts, held connections, lost progress on restart. If they buy the problem, the design sells itself.

**Make Week 6 the centrepiece.** Everyone has a queue project. Almost nobody has *"here's what I measured, here's the bottleneck, here's the before/after."* That's your differentiator.

**Volunteer the limitations.** Junior engineers present work as flawless; senior engineers present it with the failure modes named. "This is at-least-once, so anything with side effects needs an idempotency key" and "my backoff has no jitter, which risks a thundering herd" read as rigour, not weakness. If they probe for holes, you've already found them — and you found the fan-in bug yourself.

**Own the process choice.** When asked why processes over concurrency, give the full answer (§1.5), including that concurrency was the cheaper lever for I/O-bound work. Volunteering the sharper answer to your own decision is exactly the senior signal being tested.
