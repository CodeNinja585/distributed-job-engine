// queue.js
const { Queue } = require('bullmq');
const connection = require('./redis');

module.exports = new Queue('jobs', { connection }); // one queue, named 'jobs'