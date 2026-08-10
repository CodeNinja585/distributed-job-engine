
INTERVIEW QUESTIONS FOR WEEK 1

**Tier 1 — walk me through it** (nail these cold)
- Take me from the moment `POST /jobs` is hit to the moment the client gets a response — every step.
- Why does the API respond *before* the job is done?
- What's actually sitting in Redis after you enqueue a job?
- Why is the worker a separate process instead of code inside the API?

**Tier 2 — why'd you build it this way** (design defense — where it's won or lost)
- Why a queue at all? It's a 3-second task — why not just run it in the request handler?
- Why Redis, and not a Postgres table or an in-memory array?
- Why 202 and not 200? What does 202 *promise* the caller?
- "Decoupled" — name one concrete thing you can now do that you couldn't if the worker lived inside the API.
- Why does the BullMQ Worker need its *own* Redis connection instead of sharing the API's?

**Tier 3 — what happens when…** (failure & scale — the senior signal, and the traps)
- 🎯 You killed the worker, queued jobs, restarted, and they ran. Why didn't they vanish? What would've happened if the queue lived in the worker's memory?
- What if Redis itself goes down mid-flight? What's your durability actually bounded by?
- 🎯 Four 3-second jobs took 12 seconds, single-file. Bug or not? How do you parallelize them — and what new problem does that open?
- Two workers running — can the same job run twice? Is the queue at-least-once, at-most-once, or exactly-once?
- Where does a job's *result* go right now? How does the client learn it succeeded? (yeah — real gap, that's Week 2)
- You trust `req.body.ms` blindly. What breaks if I send `{"ms":"hello"}`, a giant number, or no body at all?
- The job just sleeps. How would real CPU-bound work change what you'd see under load?

Week 2 Goal: Build the async contract — let clients check job status and get results. Add the ability for jobs to fail on command.
Questions to answer:
How do I query a job's state in BullMQ?
What states exist? (waiting, active, completed, failed?)
How do I return the result to the client?
How do I make a job fail deterministically for testing?
What happens if the client asks for an id that doesn't exist?

INTERVIEW QUESTIONS FOR WEEK 2

**Interview questions from Week 2** (all defensible from your code):

*Tier 1 — walk me through it*
- What does `GET /jobs/:id` actually do, step by step?
- Where do the `result` and `error` fields come from — who put them there?
- Why does the client have to *poll* to learn the outcome instead of just getting it back?

*Tier 2 — why this way*
- Why `throw` in the worker instead of `return { ok: false }`? What does throwing unlock that returning doesn't?
- Why call `job.getState()` instead of tracking status in your own variable or table?
- Why does an unknown id return 404 by asking Redis, rather than checking a local list of ids you've seen?

*Tier 3 — what happens when… (the senior signal)*
- 🎯 Polling is wasteful — the client hammers `GET /jobs/:id` in a loop. What would you use instead at scale, and what does it cost you? (webhooks, SSE, WebSockets — trade-offs)
- 🎯 A job succeeds, but the worker crashes *after* the work, *before* BullMQ records `completed`. What state is it in now? Is your system at-least-once or exactly-once?
- Two clients poll the same id at once — any problem? Why or why not?
- `failed` is currently terminal — the job just dies there. What *should* happen next? (yeah — that's literally Week 3)
- What if I poll an id for a job that finished and aged out of Redis — 404 or 200? Is that the right answer?

Both, tight and separate.

# Week 3 interview questions
Noted — skipping the recap since you've got this one cold. Straight to the questions. And Week 3 is the richest interview material in the whole project, so this set goes deeper than 1 and 2. Every one is defensible from your code or a half-step beyond it.

Tier 1 — walk me through it (mechanics, say them cold)


Trace a job failing, from the throw to either a retry or the DLQ. Who decides which?
attempts:3 — how many times does the job actually run? What's the difference between attempts and attemptsMade, and which one is your "give up" threshold?
Retries and the backoff delays — your code or the library? Point at the exact line you wrote to make dead-lettering happen.



Your failed handler fires on every attempt. In one line of logic, how does it tell "will retry" apart from "give up"?
Tier 2 — why'd you build it this way (design defense)
Why a separate jobs-dead queue instead of just leaving them in BullMQ's built-in failed set? (defend your own call)

Nothing consumes the DLQ. What's the point of a queue no worker ever reads?

🎯 Why exponential backoff over a fixed 5-second delay? What specifically breaks with fixed delay when a downstream service is down?



Why retry at all? When you retry a failed job, what are you assuming about why it failed — and when is that assumption dead wrong?

Tier 3 — what happens when… (the senior signal, where this gets won)
🎯 A job charges a credit card, succeeds at the charge, then the worker crashes before returning success. The retry runs → card charged twice. How do you make retries safe to run more than once? (this is the queue interview question)


🎯 A downstream DB goes down and 10,000 jobs fail at the same instant. They all back off in lockstep — 1s, 2s, 4s — and all retry together. The DB comes back up… and immediately gets flattened again. What's missing from your backoff?

A job's sitting in the DLQ. You've found and fixed the bug. Now what — how does it actually get re-run? Is your DLQ a recovery tool or just a graveyard?

Some input is malformed and will never succeed. Your system still retried it 3× with growing delays. What did those retries cost, and how would you avoid retrying an error that's not worth retrying?

The worker crashes during the 2-second backoff wait, before the retry fires. Is that pending retry lost? Why or why not? (you already proved the answer in Week 1)

The two 🎯 are the money — idempotency (charge-twice) and the retry storm / jitter problem. Nail those two and you're demonstrating the exact thing that separates "I bolted on retries" from "I understand what retries do to a system under load." They come up constantly in real backend interviews because every serious queue — SQS, Kafka, Sidekiq, Celery — lives and dies by them.

Two you'll want in your back pocket, because your current code doesn't handle them and a sharp interviewer will find the edge: your retries aren't idempotent yet, and your backoff has no jitter. That's not a gap to hide — it's a gap to name: "here's what I built, here's the failure mode I know it has, here's how I'd close it." That answer beats pretending it's bulletproof.

Want me to run these at you one at a time so you practice out loud — or hand you the model answers to study, especially idempotency and jitter since those are the two you'd currently fumble? Either way, Week 4 (chunking — fan-out/fan-in) is queued the moment you're ready.


## Part 1 — Week 4 interview questions

**Tier 1 — mechanics**
- Walk me from `POST /jobs/batch` to a completed parent. How many jobs get created, and who creates them?
- How does the parent know all its chunks are done? Point at the exact line.
- Why does `initParent` set `total` *before* any chunk is enqueued?
- Chunks finish in any order. How does your code stay correct regardless of ordering?
- Where do the partial results live between chunks finishing?

**Tier 2 — design defense**
- 🎯 Why use `INCR`'s **return value** instead of `GET`-then-compare? Show me the race that second version has.
- Why hand-roll the counter instead of using BullMQ Flows? What did you gain, what did you give up?
- Why fixed chunk *size* rather than a fixed chunk *count*?
- Why partial success over all-or-nothing? When would all-or-nothing be the right call? (financial transfers — know this counterexample)
- Your aggregator reads all M results the instant the counter hits M. Why is that safe with no lock?

**Tier 3 — the senior signal**
- 🎯 **The orphaned parent:** a chunk's worker dies mid-retry and never returns. That `INCR` never fires. What happens to the parent? How do you detect and fix it? (this is *the* question on your design — timeouts/reapers)
- 🎯 Two chunks somehow both see the counter equal M. What breaks? How would you make aggregation idempotent?
- A chunk succeeds, records its result, then crashes *before* the `INCR`. What state is the batch in now, and is your recording atomic across both writes?
- One chunk takes 100× longer than the rest. What's your batch latency bounded by? (stragglers/tail latency — and what real systems do: speculative execution)
- The batch is 10,000 chunks instead of 4. What breaks first in this design?
- Who cleans up `parent:*` keys in Redis after a batch finishes? What happens after a million batches?

The 🎯 three are the money — the orphaned parent especially, because it's the known hole in *your* design. Name it, don't hide it: "here's what I built, here's the failure mode, here's the reaper I'd add."

## Part 2 — Week 5 design decisions

**1. How to get real parallelism**
- **Worker `concurrency: N`** (one process, N jobs at once) — one flag, instant parallelism, easy to demo; still one process, so one crash takes all N with it, and CPU-bound work won't actually speed up.
- **Multiple worker processes** (run `node worker.js` ×4) — true process isolation, mirrors horizontal scaling in prod; more terminals to babysit, and metrics get split across processes.

**2. Where metrics come from**
- **Redis counters** (workers `INCR` on completion, `/stats` reads them) — survives restarts, aggregates across *all* workers no matter how many; more Redis writes on the hot path.
- **In-process JS variables** (a counter + array in the worker) — zero infra, dead simple; dies on restart and only sees *that one* process — which breaks the moment you scale to 4 workers.

**3. How to compute p95 latency**
- **Keep a rolling window of the last N durations, sort, take the 95th** — exact, trivially explainable in an interview; memory grows with N, and sorting on every read.
- **Fixed histogram buckets** (count how many landed in 0–100ms, 100–500ms…) — constant memory, how Prometheus actually does it; approximate, and you have to pick buckets up front.

**4. Graceful shutdown**
- **`worker.close()` on SIGTERM/SIGINT** — BullMQ stops taking new jobs and waits for in-flight ones; if a job is slow, shutdown hangs until it's done.
- **`worker.close()` + a force-exit timeout** — bounded shutdown, which is what orchestrators like Kubernetes expect; a long job can still get killed mid-flight.

Pick one per line. I'll flag anything wack, then we build it.