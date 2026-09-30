const mongoose = require('mongoose');
const { mongoUri } = require('./env');
const logger = require('../utils/logger');

let connected = false;

async function connectMongo() {
  if (connected) return mongoose.connection;
  await mongoose.connect(mongoUri);
  connected = true;
  logger.info('MongoDB connected', { uri: mongoUri });
  return mongoose.connection;
}

module.exports = { connectMongo };
