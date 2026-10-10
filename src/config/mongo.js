const { MongoClient } = require('mongodb');
const { mongoUri } = require('./env');

// Same "one shared client, explicit connect, reused everywhere via
// require() caching" pattern as kafka/client.js and config/redis.js.
const client = new MongoClient(mongoUri);
let db = null;

async function connectMongo() {
  if (!db) {
    await client.connect();
    // No db name argument - MongoClient already parsed one out of the
    // connection string (the "/notifications" in mongodb://host:port/notifications).
    db = client.db();
    console.log('MongoDB connected');
  }
  return db;
}

// Anything that needs the db should call connectMongo() once at startup,
// then getDb() everywhere else - this throws instead of silently returning
// undefined if someone forgets the connect step, which is a much easier
// bug to catch at the exact wrong call site.
function getDb() {
  if (!db) {
    throw new Error('MongoDB not connected - call connectMongo() before getDb()');
  }
  return db;
}

module.exports = { connectMongo, getDb };
