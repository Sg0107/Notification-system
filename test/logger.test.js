'use strict';

const { test, describe } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { spawn } = require('node:child_process');

const { createLogger, createClient, format, fromEnvironment } = require('../src/utils/logger');
const { fakeLogbook, until } = require('./helpers');

// A print target that records what would have gone to the console.
function recorder() {
  const calls = [];
  const rec = (name) => (...args) => calls.push([name, ...args]);
  return { calls, print: { debug: rec('debug'), log: rec('log'), warn: rec('warn'), error: rec('error') } };
}

function loggerFor(server, opts = {}) {
  const rec = recorder();
  const warnings = [];
  const client = createClient({ url: server.url, token: 'lbi_testtoken', source: 'api', flushMs: 10, warn: (m) => warnings.push(m), ...opts });
  return { logger: createLogger({ client, print: rec.print }), client, warnings, ...rec };
}

describe('format: turning a call into text and fields', () => {
  test('a plain message has no fields', () => {
    assert.deepEqual(format('hello'), { text: 'hello', fields: {} });
  });

  test('an object becomes fields', () => {
    const { text, fields } = format('sent OK', { recipient: 'a@b.c', attempt: 2 });
    assert.equal(text, 'sent OK');
    assert.deepEqual(fields, { recipient: 'a@b.c', attempt: 2 });
  });

  test('an Error adds its message to the text and its stack to the fields', () => {
    const { text, fields } = format('Failed to start API:', new Error('connect ECONNREFUSED'));
    assert.equal(text, 'Failed to start API: connect ECONNREFUSED'); // one colon, not two
    assert.equal(fields.error.message, 'connect ECONNREFUSED');
    assert.match(fields.error.stack, /ECONNREFUSED/);
    assert.equal(format('Failed to start API', new Error('x')).text, 'Failed to start API: x');
  });

  test('an Error as the message works too, with extra fields kept', () => {
    const { text, fields } = format(new Error('kaboom'), { origin: 'uncaughtException' });
    assert.equal(text, 'kaboom');
    assert.equal(fields.origin, 'uncaughtException');
    assert.equal(fields.error.message, 'kaboom');
  });

  test('a string extra is appended, like console.log would', () => {
    assert.equal(format('Redis error:', 'connection lost').text, 'Redis error: connection lost');
  });

  test('anything that looks like a secret is hidden, at any depth', () => {
    const { fields } = format('x', { user: 'a', password: 'p', nested: { apiKey: 'k', Authorization: 'Bearer z', ok: 1 }, list: [{ token: 't' }] });
    assert.equal(fields.user, 'a');
    assert.equal(fields.password, '[redacted]');
    assert.equal(fields.nested.apiKey, '[redacted]');
    assert.equal(fields.nested.Authorization, '[redacted]');
    assert.equal(fields.nested.ok, 1);
    assert.equal(fields.list[0].token, '[redacted]');
  });

  test('awkward values never throw: circular, BigInt, functions, huge strings', () => {
    const loop = { name: 'loop' };
    loop.self = loop;
    const { fields } = format('x', { loop, big: 10n, fn() {}, long: 'a'.repeat(5000), when: new Date('2026-01-02T03:04:05Z') });
    assert.equal(fields.loop.self, '[circular]');
    assert.equal(fields.big, '10');
    assert.equal('fn' in fields, false);
    assert.equal(fields.long.length, 2003);
    assert.equal(fields.when, '2026-01-02T03:04:05.000Z');
    assert.doesNotThrow(() => JSON.stringify(fields));
  });

  test('a non-string message is shown readably', () => {
    assert.equal(format({ a: 1 }).text, '{ a: 1 }');
    assert.equal(format(42).text, '42');
  });
});

describe('the logger prints like console, with or without Logbook', () => {
  test('with no client it only prints, and flush is harmless', async () => {
    const { calls, print } = recorder();
    const logger = createLogger({ print });
    logger.info('a');
    logger.warn('b', { x: 1 });
    logger.error('c', new Error('e'));
    logger.debug('d');
    logger.fatal('f');
    assert.deepEqual(calls.map((c) => c[0]), ['log', 'warn', 'error', 'debug', 'error']);
    assert.deepEqual(calls[1], ['warn', 'b', { x: 1 }]); // same arguments console.warn(message, extra) would get
    assert.deepEqual(calls[0], ['log', 'a']); // no stray undefined argument
    assert.equal(logger.enabled, false);
    await logger.flush();
  });

  test('printing still works if the console itself throws', () => {
    const logger = createLogger({ print: { log() { throw new Error('EPIPE'); }, warn() {}, error() {}, debug() {} } });
    assert.doesNotThrow(() => logger.info('x'));
  });
});

describe('sending to Logbook', () => {
  test('logs arrive as one batch with the token, source, level, time and host', async () => {
    const server = await fakeLogbook();
    const { logger, calls } = loggerFor(server);
    logger.info('listening', { port: 3000 });
    logger.warn('slow');
    logger.error('Failed:', new Error('boom'));
    await until(() => server.entries().length === 3);

    assert.equal(server.received.length, 1, 'three quick logs should share one request');
    const [req] = server.received;
    assert.equal(req.method, 'POST');
    assert.equal(req.url, '/api/logs');
    assert.equal(req.auth, 'Bearer lbi_testtoken');
    const [a, b, c] = req.body;
    assert.deepEqual([a.level, b.level, c.level], ['info', 'warn', 'error']);
    assert.equal(a.source, 'api');
    assert.equal(a.message, 'listening');
    assert.equal(a.fields.port, 3000);
    assert.ok(a.fields.host && a.fields.pid);
    assert.ok(!Number.isNaN(Date.parse(a.ts)));
    assert.equal(c.message, 'Failed: boom');
    assert.equal(c.fields.error.message, 'boom');
    assert.equal(calls.length, 3, 'and everything still printed locally');
    await server.close();
  });

  test('the time recorded is when it happened, not when it was sent', async () => {
    const server = await fakeLogbook();
    const { logger } = loggerFor(server, { flushMs: 300 });
    const before = Date.now();
    logger.info('early');
    await new Promise((r) => setTimeout(r, 200));
    await until(() => server.entries().length === 1);
    assert.ok(Date.parse(server.entries()[0].ts) - before < 100);
    await server.close();
  });

  test('big bursts are split into batches', async () => {
    const server = await fakeLogbook();
    const { logger, client } = loggerFor(server, { maxBatch: 3 });
    for (let i = 0; i < 7; i++) logger.info(`m${i}`);
    await client.flush();
    assert.deepEqual(server.received.map((r) => r.body.length), [3, 3, 1]);
    assert.deepEqual(server.entries().map((e) => e.message), ['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6']);
    await server.close();
  });

  test('empty messages are skipped, since Logbook would reject them', async () => {
    const server = await fakeLogbook();
    const { logger, client } = loggerFor(server);
    logger.info('');
    logger.info('   ');
    logger.info('real');
    await client.flush();
    assert.deepEqual(server.entries().map((e) => e.message), ['real']);
    await server.close();
  });
});

describe('Logbook trouble never hurts the app', () => {
  test('logging returns at once even when Logbook never answers, and flush gives up on time', async () => {
    const server = await fakeLogbook();
    server.behave('hang');
    const { logger, client } = loggerFor(server, { timeoutMs: 5000 });

    const t0 = Date.now();
    for (let i = 0; i < 200; i++) logger.info(`m${i}`);
    assert.ok(Date.now() - t0 < 100, 'logging must not wait on the network');

    const t1 = Date.now();
    await client.flush(300);
    assert.ok(Date.now() - t1 < 800, 'flush must not hang the process');
    await server.close();
  });

  test('when Logbook is down, nothing throws and the logs are kept and retried', async () => {
    const server = await fakeLogbook();
    const { port } = server;
    await server.close(); // now nothing is listening on that port
    const { logger, client, warnings } = loggerFor(server);

    assert.doesNotThrow(() => { logger.info('one'); logger.error('two', new Error('x')); });
    await client.flush();
    assert.equal(client.queued, 2, 'still waiting to be delivered');
    assert.ok(warnings[0].includes('Could not reach Logbook'));
    assert.ok(port > 0);
  });

  test('after a server error the same logs are delivered once it recovers, none lost or doubled', async () => {
    const server = await fakeLogbook();
    server.behave(503);
    const { logger, client } = loggerFor(server);
    logger.info('a');
    logger.info('b');
    await client.flush();
    assert.equal(server.entries().length, 0);
    assert.equal(client.queued, 2);

    server.behave(200);
    await client.flush();
    assert.deepEqual(server.entries().map((e) => e.message), ['a', 'b']);
    assert.equal(client.queued, 0);
    await server.close();
  });

  test('a refused token switches sending off, with one clear notice, and the app keeps logging locally', async () => {
    const server = await fakeLogbook();
    server.behave(401);
    const { logger, client, warnings, calls } = loggerFor(server);
    logger.info('a');
    await client.flush();
    logger.info('b');
    logger.info('c');
    await client.flush();
    assert.equal(client.queued, 0);
    assert.equal(warnings.filter((w) => w.includes('refused the token')).length, 1);
    assert.equal(calls.length, 3, 'local printing is unaffected');
    server.behave(200);
    logger.info('d');
    await client.flush();
    assert.equal(server.entries().length, 0, 'it stays off for this process');
    await server.close();
  });

  test('a batch the server rejects for good is dropped, not retried forever', async () => {
    const server = await fakeLogbook();
    server.behave(400);
    const { logger, client } = loggerFor(server);
    logger.info('bad');
    await client.flush();
    assert.equal(client.queued, 0);
    await server.close();
  });

  test('during a long outage memory stays bounded and the loss is reported afterwards', async () => {
    const server = await fakeLogbook();
    server.behave(503);
    const { logger, client } = loggerFor(server, { maxQueue: 5 });
    for (let i = 0; i < 8; i++) logger.info(`m${i}`);
    assert.equal(client.queued, 5);

    server.behave(200);
    await client.flush();
    await client.flush();
    const messages = server.entries().map((e) => e.message);
    assert.deepEqual(messages.slice(0, 5), ['m3', 'm4', 'm5', 'm6', 'm7'], 'the newest are kept');
    assert.ok(messages.some((m) => m.includes('dropped 3 log(s)')));
    await server.close();
  });
});

describe('configuration from the environment', () => {
  test('without LOGBOOK_URL and LOGBOOK_TOKEN it is just a console logger', () => {
    assert.equal(fromEnvironment({}).enabled, false);
    assert.equal(fromEnvironment({ LOGBOOK_URL: 'http://x' }).enabled, false);
    assert.equal(fromEnvironment({ LOGBOOK_TOKEN: 'lbi_x' }).enabled, false);
  });

  test('a URL without http(s) leaves it off', () => {
    assert.equal(fromEnvironment({ LOGBOOK_URL: 'mystackdog.example.com', LOGBOOK_TOKEN: 'lbi_x' }).enabled, false);
  });
});

// These run the real thing in a separate process, as the services do.
function run(script, env) {
  const child = spawn(process.execPath, [path.join(__dirname, 'fixtures', script)], {
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const out = { stdout: '', stderr: '' };
  child.stdout.on('data', (d) => (out.stdout += d));
  child.stderr.on('data', (d) => (out.stderr += d));
  const exited = new Promise((resolve) => child.on('exit', (code, signal) => resolve({ code, signal, ...out })));
  return { child, out, exited };
}

describe('the last logs before a process ends', () => {
  test('a startup failure that flushes before process.exit is delivered, under the right source name', async () => {
    const server = await fakeLogbook();
    const { exited } = run('emailWorker.js', { LOGBOOK_URL: server.url, LOGBOOK_TOKEN: 'lbi_t' });
    const r = await exited;
    assert.equal(r.code, 1);
    const entries = server.entries();
    assert.deepEqual(entries.map((e) => e.message), ['worker starting', 'Email worker failed to start: boom']);
    assert.equal(entries[1].level, 'error');
    assert.equal(entries[1].source, 'email-worker', 'named after the script that ran');
    assert.equal(entries[1].fields.error.message, 'boom');
    assert.match(r.stderr, /Email worker failed to start:/, 'and it still printed locally');
    await server.close();
  });

  test('an uncaught exception is logged as fatal and the process still exits with code 1', async () => {
    const server = await fakeLogbook();
    const { exited } = run('crash.js', { LOGBOOK_URL: server.url, LOGBOOK_TOKEN: 'lbi_t' });
    const r = await exited;
    assert.equal(r.code, 1);
    const fatal = server.entries().find((e) => e.level === 'fatal');
    assert.ok(fatal, 'the crash should reach Logbook');
    assert.equal(fatal.message, 'kaboom');
    assert.equal(fatal.fields.origin, 'uncaughtException');
    assert.match(fatal.fields.error.stack, /crash\.js/);
    assert.match(r.stderr, /kaboom/, 'and the crash still printed locally');
    await server.close();
  });

  test('on SIGTERM the queued logs are sent, then the process stops as usual', async () => {
    const server = await fakeLogbook();
    const { child, out, exited } = run('waiter.js', { LOGBOOK_URL: server.url, LOGBOOK_TOKEN: 'lbi_t', LOGBOOK_SOURCE: 'api' });
    await until(() => out.stdout.includes('READY'));
    child.kill('SIGTERM'); // well inside the 1 second batching window
    const r = await exited;
    assert.equal(r.signal, 'SIGTERM', 'it still ends because of the signal');
    assert.deepEqual(server.entries().map((e) => e.message), ['server is up']);
    assert.equal(server.entries()[0].source, 'api');
    await server.close();
  });

  test('without Logbook configured the same scripts behave exactly as before', async () => {
    const r = await run('emailWorker.js', { LOGBOOK_URL: '', LOGBOOK_TOKEN: '' }).exited;
    assert.equal(r.code, 1);
    assert.match(r.stdout, /worker starting/);
    assert.match(r.stderr, /Email worker failed to start:/);
  });
});
