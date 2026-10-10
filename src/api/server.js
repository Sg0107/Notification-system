const express = require('express');
const { connectProducer } = require('../kafka/producer');
const { connectMongo } = require('../config/mongo');
const { ensureIndexes } = require('../dao/notificationDao');
const notificationsRouter = require('./routes/notifications');

const app = express();
app.use(express.json());

app.get('/health', (req, res) => res.json({ status: 'ok' }));
app.use('/api/notifications', notificationsRouter);

async function start() {
  // Connect to Kafka once, before accepting any HTTP traffic - if Kafka is
  // unreachable, we want the whole process to fail loudly at startup,
  // rather than accepting requests we can't actually fulfil.
  await connectProducer();

  // Same reasoning for Mongo - fail loudly at startup if it's unreachable,
  // rather than discovering it mid-request.
  await connectMongo();
  await ensureIndexes();

  const port = 3000;
  app.listen(port, () => {
    console.log(`Notification API listening on port ${port}`);
  });
}

start().catch((err) => {
  console.error('Failed to start API:', err);
  process.exit(1);
});
