const deadQueue = require('./deadQueue');
const express = require('express');
const jobQueue = require('./queue');
const { initParent, getParent, getMetrics } = require('./store');

const app = express();
app.use(express.json());

app.post('/jobs', async (req, res) => {
  const { ms = 1000, failFor = 0, attempts = 3 } = req.body;
  const job = await jobQueue.add('sleep', { ms, failFor }, {
    attempts,
    backoff: { type: 'exponential', delay: 1000 },
  });
  res.status(202).json({ id: job.id });
});

app.get('/jobs/:id', async (req, res) => {
  const job = await jobQueue.getJob(req.params.id);
  if (!job) return res.status(404).json({ error: 'job not found' });

  const state = await job.getState();
  res.json({
    id: job.id,
    state,
    result: job.returnvalue ?? null,
    error: job.failedReason ?? null,
  });
});

app.get('/dlq', async (req, res) => {
  const jobs = await deadQueue.getJobs(['waiting', 'delayed']);
  res.json({ count: jobs.length, jobs: jobs.map((j) => j.data) });
});

app.get('/stats', async (req, res) => {
  const [metrics, depth, dlq] = await Promise.all([
    getMetrics(),
    jobQueue.getJobCounts('waiting', 'active', 'delayed'),
    deadQueue.getJobCounts('waiting'),
  ]);
  res.json({ ...metrics, queue: depth, dlqCount: dlq.waiting });
});

app.post('/jobs/batch', async (req, res) => {
  const { total = 100, chunkSize = 25, ms = 300, failChunkIndex = -1 } = req.body;
  const parentId = `p_${Date.now()}`;
  const chunks = Math.ceil(total / chunkSize);

  await initParent(parentId, chunks);

  for (let index = 0; index < chunks; index++) {
    const start = index * chunkSize + 1;
    const end = Math.min((index + 1) * chunkSize, total);
    
    await jobQueue.add('chunk', { parentId, index, start, end, ms, failChunkIndex }, {
      attempts: 2, 
      backoff: { type: 'exponential', delay: 500 }
    });
  }
  
  res.status(202).json({ parentId, chunks });
});

app.get('/jobs/batch/:id', async (req, res) => {
  const p = await getParent(req.params.id);
  if (!p) return res.status(404).json({ error: 'batch not found' });
  if (p.final) return res.json({ state: 'completed', ...p.final });
  res.json({ state: 'processing', done: p.done, total: p.total });
});

app.listen(3000, () => console.log('api on :3000'));