// deadQueue.js — dead jobs land here for manual inspection. Nothing processes it.
const { Queue } = require('bullmq');
const connection = require('./redis');
module.exports = new Queue('jobs-dead', { connection });
