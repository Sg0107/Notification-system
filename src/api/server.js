const express = require('express');
const { connectProducer } = require('../kafka/producer');
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

  const port = 3000;
  app.listen(port, () => {
    console.log(`Notification API listening on port ${port}`);
  });
}

start().catch((err) => {
  console.error('Failed to start API:', err);
  process.exit(1);
});
