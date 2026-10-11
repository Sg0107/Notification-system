'use strict';

const http = require('node:http');

// A stand-in for Logbook. `behave` can be changed mid-test:
//   200 (default) accept, or any other status to refuse, or 'hang' to never answer.
async function fakeLogbook() {
  const received = []; // each entry: { auth, body } where body is the parsed array
  let behave = 200;
  const server = http.createServer((req, res) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      if (behave === 'hang') return; // never answer
      if (behave === 200) received.push({ url: req.url, method: req.method, auth: req.headers.authorization, body: JSON.parse(raw) });
      res.statusCode = behave;
      res.end('{}');
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  return {
    url: `http://127.0.0.1:${port}`,
    port,
    received,
    entries: () => received.flatMap((r) => r.body),
    behave: (b) => { behave = b; },
    close: () => { server.closeAllConnections(); return new Promise((resolve) => server.close(resolve)); },
  };
}

// Waits until fn() is truthy, or fails after a timeout.
async function until(fn, ms = 4000) {
  const start = Date.now();
  while (!fn()) {
    if (Date.now() - start > ms) throw new Error('timed out waiting for a condition');
    await new Promise((r) => setTimeout(r, 15));
  }
}

module.exports = { fakeLogbook, until };
