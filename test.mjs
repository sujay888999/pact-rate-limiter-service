import { createProxyServer, createRateLimiter, MemoryStore } from './rate-limiter.mjs';
import http from 'node:http';

const upstream = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: true, path: req.url }));
});

await new Promise((r) => upstream.listen(0, r));
const upPort = upstream.address().port;

const limiter = createRateLimiter({ algorithm: 'token_bucket', capacity: 3, ratePerSec: 1 });
const { server } = createProxyServer({ limiter, upstream: `http://127.0.0.1:${upPort}`, sweepMs: false });
await new Promise((r) => server.listen(0, r));
const port = server.address().port;

const hit = () => new Promise((resolve) => {
  http.get(`http://127.0.0.1:${port}/test`, (res) => {
    let body = '';
    res.on('data', (c) => (body += c));
    res.on('end', () => resolve({
      status: res.statusCode,
      remaining: res.headers['x-ratelimit-remaining'],
      retryAfter: res.headers['retry-after'],
      body,
    }));
  });
});

const results = [];
for (let i = 0; i < 6; i++) results.push(await hit());
results.forEach((r, i) => console.log(`req${i + 1}: ${r.status} remaining=${r.remaining ?? '-'} retryAfter=${r.retryAfter ?? '-'} ${r.body}`));

const blocked = results.filter((r) => r.status === 429);
const passed = results.filter((r) => r.status === 200);
const headersOk = passed.every((r) => r.remaining !== undefined);

// sliding window mode
const sw = createRateLimiter({ algorithm: 'sliding_window', capacity: 2, windowMs: 60000, store: new MemoryStore() });
const a = await sw.check({ headers: {}, socket: { remoteAddress: '9.9.9.9' } });
const b = await sw.check({ headers: {}, socket: { remoteAddress: '9.9.9.9' } });
const c = await sw.check({ headers: {}, socket: { remoteAddress: '9.9.9.9' } });

const pass = passed.length === 3 && blocked.length === 3 && headersOk && a.allowed && b.allowed && !c.allowed;
console.log(`\nproxy: ${passed.length} passed / ${blocked.length} throttled · limit headers present: ${headersOk}`);
console.log(`sliding_window: allow allow block -> ${a.allowed} ${b.allowed} ${c.allowed}`);
console.log(pass ? 'ALL TESTS PASSED' : 'TESTS FAILED');

upstream.close(); server.close();
process.exit(pass ? 0 : 1);
