const { Worker } = require('bullmq');
const connection = require('./redis');
const deadQueue = require('./deadQueue');
const { recordChunkOutcome, getParent, getAllResults, setFinal,
        recordCompletion, recordFailure, recordRetry } = require('./store');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function processChunk(job) {
  const { parentId, index, start, end, ms, failChunkIndex } = job.data;
  console.log(`  chunk ${index} start (attempt ${job.attemptsMade + 1})`);
  await sleep(ms);
  if (index === failChunkIndex) throw new Error(`chunk ${index} forced failure`);   // ← doomed, before it records
  let sum = 0;
  for (let i = start; i <= end; i++) sum += i;
  console.log(`  chunk ${index} done → sum ${sum}`);
  const done = await recordChunkOutcome(parentId, index, { ok: true, sum });
  await maybeAggregate(parentId, done);
  return { ok: true, sum };
}

async function maybeAggregate(parentId, done) {
  const p = await getParent(parentId);
  if (!p || done < p.total) return;            // not the last chunk → do nothing
  const results = await getAllResults(parentId);
  const ok = results.filter(r => r.ok);
  const failed = results.filter(r => !r.ok);
  const total = ok.reduce((s, r) => s + r.sum, 0);
  await setFinal(parentId, {
    total, okChunks: ok.length,
    failedChunks: failed.map(f => ({ index: f.index, reason: f.reason })),
  });
  console.log(`✅ parent ${parentId} → total ${total} (${ok.length} ok, ${failed.length} failed)`);
}

// ... (keep all imports and processChunk/maybeAggregate functions exactly as they were) ...

const worker = new Worker('jobs', async (job) => {
  const t0 = Date.now();
  const waitMs = t0 - job.timestamp; // NEW: Calculate time spent in queue
  
  const run = async () => {
    if (job.name === 'chunk') return processChunk(job);
    
  
    const failFor = Number(job.data.failFor ?? 0);
    console.log(`start  ${job.id}  attempt ${job.attemptsMade + 1}/${job.opts.attempts}`);
    await sleep(job.data.ms);
    if (job.attemptsMade < failFor) throw new Error(`deliberate fail (attempt ${job.attemptsMade + 1})`);
    console.log(`finish ${job.id}  on attempt ${job.attemptsMade + 1}`);
    return { ok: true, ranFor: job.data.ms, attempts: job.attemptsMade + 1 };
  };
  
  const result = await run();
  await recordCompletion(Date.now() - t0, waitMs); // UPDATED: Pass waitMs
  return result;
}, { connection });

// ... (keep the worker.on('failed') and graceful shutdown code exactly as it was) ...

// fires on EVERY failed attempt — only dead-letter once all attempts are spent
worker.on('failed', async (job, err) => {
  if (!job) return;
  const terminal = job.attemptsMade >= (job.opts.attempts ?? 1);
  if (!terminal) {
    console.log(`retry  ${job.id}  attempt ${job.attemptsMade} failed → will retry`);
    await recordRetry();                    // ← add
    return;
  }
  await recordFailure();                    // ← add (before the chunk/DLQ branch)

  if (job.name === 'chunk') {
  // ...rest unchanged                               // ← chunk failure feeds the parent, not the DLQ
    const { parentId, index } = job.data;
    console.log(`  chunk ${index} FAILED permanently → ${err.message}`);
    const done = await recordChunkOutcome(parentId, index, { ok: false, reason: err.message });
    await maybeAggregate(parentId, done);                      // this might be the M-th terminal outcome
    return;
  }

  await deadQueue.add('dead', {                                // 'sleep' jobs still DLQ, exactly as Week 3
    originalId: job.id, data: job.data,
    reason: err.message, attemptsMade: job.attemptsMade,
    failedAt: new Date().toISOString(),
  });
  console.log(`💀 DLQ  ${job.id}  after ${job.attemptsMade} attempts`);
});

const shutdown = async (sig) => {
  console.log(`${sig} → draining (pid ${process.pid})…`);
  const force = setTimeout(() => { console.log('force exit'); process.exit(1); }, 10_000);  // bounded
  await worker.close();                     // stop taking new jobs, finish in-flight
  clearTimeout(force);
  console.log('clean exit');
  process.exit(0);
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));

console.log(`worker up  pid=${process.pid}`);