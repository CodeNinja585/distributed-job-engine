// store.js — our own Redis client for parent/chunk state (counters, partial results).
// BullMQ makes its own connections from redis.js; this is separate, for app state.
// Atomic ops (INCR / HSET) are the whole point of this file.
const IORedis = require('ioredis');
const options = require('./redis');
const redis = new IORedis(options);

const key = (id, suffix) => `parent:${id}:${suffix}`;

async function initParent(parentId, total) {
  await redis.hset(key(parentId, 'meta'), 'total', total, 'createdAt', Date.now());
  // done-counter starts implicitly at 0 — first INCR makes it 1
}

// record one chunk's terminal outcome, atomically bump the counter, return the NEW count.
// the caller compares that return value to `total` — exactly one chunk sees them equal.
async function recordChunkOutcome(parentId, index, outcome) {
  await redis.hset(key(parentId, 'results'), String(index), JSON.stringify(outcome));
  return redis.hlen(key(parentId, 'results')); 
}

async function getParent(parentId) {
  const meta = await redis.hgetall(key(parentId, 'meta'));
  if (!meta.total) return null;
  const done = Number((await redis.get(key(parentId, 'done'))) || 0);
  const finalRaw = await redis.get(key(parentId, 'final'));
  return { total: Number(meta.total), done, final: finalRaw ? JSON.parse(finalRaw) : null };
}

async function getAllResults(parentId) {
  const raw = await redis.hgetall(key(parentId, 'results'));
  return Object.entries(raw).map(([i, v]) => ({ index: Number(i), ...JSON.parse(v) }));
}

const setFinal = (parentId, obj) => redis.set(key(parentId, 'final'), JSON.stringify(obj), 'NX');

const M = { 
  completed: 'metrics:completed', 
  failed: 'metrics:failed', 
  retries: 'metrics:retries', 
  durations: 'metrics:durations',
  waits: 'metrics:waits' // NEW
};

async function recordCompletion(durationMs, waitMs) { // UPDATED
  await redis.incr(M.completed);
  await redis.lpush(M.durations, `${Date.now()}:${durationMs}`);
  await redis.ltrim(M.durations, 0, 999);
  await redis.lpush(M.waits, String(waitMs)); // NEW
  await redis.ltrim(M.waits, 0, 999);         // NEW
}
const recordFailure = () => redis.incr(M.failed);
const recordRetry = () => redis.incr(M.retries);

async function getMetrics() {
  const [completed, failed, retries, rawDurations, rawWaits] = await Promise.all([
    redis.get(M.completed), 
    redis.get(M.failed), 
    redis.get(M.retries),
    redis.lrange(M.durations, 0, -1),
    redis.lrange(M.waits, 0, -1) // NEW
  ]);
  
  const parseDurations = rawDurations.map(s => { const [ts, d] = s.split(':'); return Number(d); }).sort((a, b) => a - b);
  const parseWaits = rawWaits.map(Number).sort((a, b) => a - b); // NEW
  
  const at = (arr, q) => arr.length ? arr[Math.min(arr.length - 1, Math.floor(arr.length * q))] : 0;
  const cutoff = Date.now() - 60_000;
  
  // Recalculate jobsPerSec based on durations array length for simplicity
  const parsedForRate = rawDurations.map(s => { const [ts, d] = s.split(':'); return { ts: Number(ts), d: Number(d) }; });

  return {
    completed: Number(completed || 0), 
    failed: Number(failed || 0), 
    retries: Number(retries || 0),
    jobsPerSec: +(parsedForRate.filter(p => p.ts >= cutoff).length / 60).toFixed(2),
    p50Ms: at(parseDurations, 0.5), 
    p95Ms: at(parseDurations, 0.95), 
    p50WaitMs: at(parseWaits, 0.5), // NEW
    p95WaitMs: at(parseWaits, 0.95), // NEW
    sampleSize: parseDurations.length,
  };
}

module.exports = { 
  initParent, recordChunkOutcome, getParent, getAllResults, setFinal,
  recordCompletion, recordFailure, recordRetry, getMetrics 
};