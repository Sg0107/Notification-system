'use strict';

// The one place the app logs through.
//
//   const logger = require('../utils/logger');
//   logger.info('Notification API listening', { port: 3000 });
//   logger.error('Failed to start API:', err);
//
// Every call prints to the console exactly as console.log/warn/error would, and
// also sends a copy to Logbook (the log collector you host). Sending is queued,
// batched and never blocks, never throws, and never takes the app down: if
// Logbook is unreachable the logs wait in memory and are retried.
//
// Sending is switched on by two settings in .env. Without them this is just a
// console logger, so running the app locally needs nothing:
//   LOGBOOK_URL     e.g. https://mystackdog.shubhamco.com
//   LOGBOOK_TOKEN   this product's ingest token (starts with lbi_)
//   LOGBOOK_SOURCE  optional: overrides the process name used as the log source
//
// Methods: debug, info, warn, error, fatal. Each takes (message, extra) where
// extra is optional and can be:
//   - an object, shown as searchable fields in Logbook
//   - an Error, which adds its message to the text and its stack to the fields
//   - anything else, appended to the message
// Call `await logger.flush()` before a deliberate process.exit() so the last
// logs get out.

const os = require('node:os');
const path = require('node:path');
const util = require('node:util');

require('dotenv').config(); // idempotent, so the settings are there however early this loads

const DEFAULTS = {
  flushMs: 1000, // how long to wait to collect a batch
  maxBatch: 100, // logs per request (Logbook accepts up to 1000)
  maxQueue: 2000, // logs held in memory while Logbook is unreachable
  timeoutMs: 3000, // give up on one request after this long
  maxBackoffMs: 30000,
};
const MAX_MESSAGE = 10000;
const MAX_STRING = 2000;
const MAX_DEPTH = 5;
const LEVELS = ['debug', 'info', 'warn', 'error', 'fatal'];
const CONSOLE_METHOD = { debug: 'debug', info: 'log', warn: 'warn', error: 'error', fatal: 'error' };
const SENSITIVE = /pass(word)?|secret|token|authorization|api[-_]?key|cookie/i;

// The real console methods, captured once. The logger prints with these, and
// its own notices use them, so nothing can loop back into the logger.
const realConsole = {};
for (const method of new Set(Object.values(CONSOLE_METHOD))) realConsole[method] = console[method].bind(console);

// ---------------------------------------------------------------------------
// Turning whatever was passed in into text and safe fields
// ---------------------------------------------------------------------------

// toSafe copies a value into something JSON can always carry: errors become
// plain objects, circular references and deep nesting are cut off, long strings
// are shortened, and anything that looks like a secret is hidden.
function toSafe(value, depth = 0, seen = new WeakSet()) {
  if (value === null || typeof value === 'number' || typeof value === 'boolean') return value;
  if (typeof value === 'string') return value.length > MAX_STRING ? value.slice(0, MAX_STRING) + '...' : value;
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value.toISOString();
  if (value instanceof Error) return { name: value.name, message: value.message, code: value.code };
  if (typeof value !== 'object') return undefined; // functions, symbols, undefined
  if (seen.has(value)) return '[circular]';
  if (depth >= MAX_DEPTH) return '[too deep]';
  seen.add(value);
  if (Array.isArray(value)) return value.slice(0, 100).map((v) => toSafe(v, depth + 1, seen) ?? null);
  const out = {};
  for (const [key, v] of Object.entries(value)) {
    const safe = SENSITIVE.test(key) ? '[redacted]' : toSafe(v, depth + 1, seen);
    if (safe !== undefined) out[key] = safe;
  }
  return out;
}

const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Error);

function describe(value) {
  if (typeof value === 'string') return value;
  if (value instanceof Error) return value.message;
  return util.inspect(value, { depth: 3, breakLength: Infinity });
}

// format returns the message text and the fields for one logger call.
function format(message, extra) {
  let text = describe(message);
  const fields = isPlainObject(extra) ? toSafe(extra) : {};
  const err = message instanceof Error ? message : extra instanceof Error ? extra : null;

  if (extra instanceof Error && !(message instanceof Error)) {
    text = `${text.replace(/:\s*$/, '')}: ${extra.message}`; // "Failed to start API: connect ECONNREFUSED"
  } else if (extra !== undefined && !isPlainObject(extra) && !(extra instanceof Error)) {
    text = `${text} ${describe(extra)}`;
  }
  if (err) fields.error = { name: err.name, message: err.message, code: err.code, stack: err.stack };
  return { text, fields };
}

// ---------------------------------------------------------------------------
// The client: queue, batch, send, retry
// ---------------------------------------------------------------------------

function createClient(options) {
  const o = { ...DEFAULTS, warn: realConsole.warn, ...options };
  const endpoint = o.url.replace(/\/+$/, '') + '/api/logs';
  const base = { host: os.hostname(), pid: process.pid };

  let queue = [];
  let timer = null;
  let sending = null;
  let failures = 0;
  let dropped = 0;
  let stopped = false;
  let lastNotice = 0;

  // At most one notice a minute, so an outage cannot flood the terminal.
  function notice(text, force) {
    const now = Date.now();
    if (!force && now - lastNotice < 60000) return;
    lastNotice = now;
    o.warn(`[logger] ${text}`);
  }

  function enqueue(level, message, fields) {
    if (stopped) return;
    const text = String(message ?? '').slice(0, MAX_MESSAGE);
    if (!text.trim()) return; // Logbook needs a message
    if (queue.length >= o.maxQueue) {
      queue.shift();
      dropped++;
    }
    queue.push({
      ts: new Date().toISOString(), // when it happened, not when it was sent
      level,
      source: o.source,
      message: text,
      fields: { ...base, ...fields },
    });
    schedule(o.flushMs);
  }

  function schedule(ms) {
    if (timer || stopped) return;
    timer = setTimeout(() => {
      timer = null;
      flushNow();
    }, ms);
    timer.unref(); // a pending timer must never keep the process alive
  }

  // 'ok' sent, 'retry' try again later, 'drop' the server refused this batch for
  // good, 'stop' the token is no good so sending is switched off.
  async function post(batch) {
    try {
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${o.token}` },
        body: JSON.stringify(batch),
        signal: AbortSignal.timeout(o.timeoutMs),
      });
      if (res.ok) return 'ok';
      if (res.status === 401 || res.status === 403) {
        notice(`Logbook refused the token (HTTP ${res.status}). Sending logs is switched off for this process.`, true);
        return 'stop';
      }
      if (res.status === 429 || res.status >= 500) {
        notice(`Logbook answered HTTP ${res.status}. Will retry; logs are queued in memory.`);
        return 'retry';
      }
      notice(`Logbook rejected a batch (HTTP ${res.status}). Dropping it.`);
      return 'drop';
    } catch {
      notice('Could not reach Logbook. Will retry; logs are queued in memory.');
      return 'retry';
    }
  }

  function flushNow() {
    if (sending) return sending;
    sending = (async () => {
      try {
        while (queue.length && !stopped) {
          const batch = queue.slice(0, o.maxBatch);
          const result = await post(batch);
          if (result === 'retry') {
            failures++;
            schedule(Math.min(o.maxBackoffMs, 1000 * 2 ** Math.min(failures, 10)));
            return;
          }
          if (result === 'stop') {
            stopped = true;
            queue = [];
            return;
          }
          // Remove exactly what was sent. The queue may have changed meanwhile.
          const sent = new Set(batch);
          queue = queue.filter((e) => !sent.has(e));
          if (result === 'ok') {
            failures = 0;
            if (dropped) {
              const n = dropped;
              dropped = 0;
              enqueue('warn', `Logger dropped ${n} log(s) while Logbook was unreachable.`, {});
            }
          }
        }
      } finally {
        sending = null;
      }
    })();
    return sending;
  }

  // flush sends what is queued and waits, but never longer than timeoutMs, so
  // calling it before exiting cannot hang the process.
  function flush(timeoutMs = o.timeoutMs) {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    const giveUp = new Promise((resolve) => setTimeout(resolve, timeoutMs).unref());
    return Promise.race([flushNow(), giveUp]);
  }

  return { enqueue, flush, get queued() { return queue.length; } };
}

// ---------------------------------------------------------------------------
// The logger
// ---------------------------------------------------------------------------

// createLogger wires the methods to a client (or to nothing, for console only)
// and to a place to print. The app uses the default one created below.
function createLogger({ client = null, print = realConsole } = {}) {
  const logger = {
    enabled: Boolean(client),
    flush: (timeoutMs) => (client ? client.flush(timeoutMs) : Promise.resolve()),
  };
  for (const level of LEVELS) {
    logger[level] = (message, extra) => {
      try {
        // Local output first, in the same shape console.log(message, extra) gives.
        const printer = print[CONSOLE_METHOD[level]];
        if (extra === undefined) printer(message);
        else printer(message, extra);
      } catch {
        /* a broken stdout must not break logging */
      }
      if (!client) return;
      try {
        const { text, fields } = format(message, extra);
        client.enqueue(level, text, fields);
      } catch {
        /* logging must never break the app */
      }
    };
  }
  return logger;
}

// Handle the last logs before a stop or a crash.
function installProcessHooks(logger) {
  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.once(signal, async () => {
      await logger.flush(2000);
      process.kill(process.pid, signal); // our handler is gone now, so this stops the process as usual
    });
  }
  process.on('uncaughtException', async (err, origin) => {
    logger.fatal(err, { origin }); // also prints the error and stack, as an unhandled crash would
    await logger.flush(2000);
    process.exit(1); // the same outcome as an unhandled crash
  });
  process.on('beforeExit', () => {
    logger.flush();
  });
}

// The scripts here are named after what they run. Give each a clear source name.
const SOURCE_NAMES = {
  server: 'api',
  emailWorker: 'email-worker',
  smsWorker: 'sms-worker',
  pushWorker: 'push-worker',
};

function sourceName(env) {
  if (env.LOGBOOK_SOURCE) return env.LOGBOOK_SOURCE;
  const script = require.main ? path.basename(require.main.filename, '.js') : 'app';
  return SOURCE_NAMES[script] || script;
}

function fromEnvironment(env = process.env) {
  const url = env.LOGBOOK_URL;
  const token = env.LOGBOOK_TOKEN;
  if (!url || !token) return createLogger();
  if (!/^https?:\/\//.test(url)) {
    realConsole.warn('[logger] LOGBOOK_URL must start with http:// or https://. Sending logs is off.');
    return createLogger();
  }
  const logger = createLogger({ client: createClient({ url, token, source: sourceName(env) }) });
  installProcessHooks(logger);
  return logger;
}

module.exports = fromEnvironment();
module.exports.createLogger = createLogger;
module.exports.createClient = createClient;
module.exports.format = format;
module.exports.fromEnvironment = fromEnvironment;
