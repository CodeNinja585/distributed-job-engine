// redis.js — BullMQ makes its own connections from these (the Worker needs its own; it blocks)
module.exports = {
  host: process.env.REDIS_HOST || 'localhost',
  port: Number(process.env.REDIS_PORT || 6379),
  maxRetriesPerRequest: null,
};

