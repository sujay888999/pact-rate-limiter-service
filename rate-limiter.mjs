import http from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';

const KEY_IP = 'ip';
const KEY_HEADER = process.env.RATE_KEY_HEADER || 'x-api-key';
const KEY_PREFIX = process.env.RATE_KEY_PREFIX || 'rl';

export class MemoryStore {
  constructor() { this.buckets = new Map(); this.windows = new Map(); }

  async bucket(key, create) {
    const now = Date.now();
    let b = this.buckets.get(key);
    if (!b || b.expiresAt <= now) { b = create(now); this.buckets.set(key, b); }
    return b;
  }

  async saveBucket(key, b) { this.buckets.set(key, b); }

  async count(key, windowMs) {
    const now = Date.now();
    const arr = (this.windows.get(key) || []).filter((t) => t > now - windowMs);
    arr.push(now);
    this.windows.set(key, arr);
    return arr.length;
  }

  sweep() {
    const now = Date.now();
    for (const [k, b] of this.buckets) if (b.expiresAt <= now) this.buckets.delete(k);
    for (const [k, arr] of this.windows) {
      const kept = arr.filter((t) => t > now - 600000);
      if (kept.length) this.windows.set(k, kept); else this.windows.delete(k);
    }
  }
}

export class RedisStore {
  constructor(client, prefix = KEY_PREFIX) { this.r = client; this.p = prefix; }

  async bucket(key, create) {
    const k = `${this.p}:tb:${key}`;
    const raw = await this.r.get(k);
    if (raw) return JSON.parse(raw);
    return create(Date.now());
  }

  async saveBucket(key, b) {
    const ttl = Math.max(1, Math.ceil((b.expiresAt - Date.now()) / 1000));
    await this.r.set(`${this.p}:tb:${key}`, JSON.stringify(b), { EX: ttl });
  }

  async count(key, windowMs) {
    const now = Date.now();
    const k = `${this.p}:sw:${key}`;
    await this.r.zremrangebyscore(k, 0, now - windowMs);
    const n = await this.r.zadd(k, { NX: true, score: now, value: `${now}:${Math.random()}` });
    await this.r.expire(k, Math.ceil(windowMs / 1000) + 5);
    return (n === 1 ? 0 : 0) + (await this.r.zcard(k));
  }

  async sweep() { /* redis TTLs handle expiry */ }
}

function limiterFor(algo, cfg, store) {
  if (algo === 'sliding_window') {
    return async (key) => {
      const used = await store.count(`${key}:sw`, cfg.windowMs);
      const allowed = used <= cfg.capacity;
      return {
        allowed,
        remaining: Math.max(0, cfg.capacity - used),
        retryAfter: allowed ? 0 : Math.ceil((cfg.windowMs - ((Date.now() % cfg.windowMs))) / 1000),
        resetAt: Date.now() + cfg.windowMs,
      };
    };
  }
  return async (key) => {
    const now = Date.now();
    const b = await store.bucket(`${key}:tb`, (t) => ({
      tokens: cfg.capacity, updatedAt: t, expiresAt: t + Math.ceil((cfg.capacity / cfg.ratePerSec) * 1000) + 1000,
    }));
    const elapsed = (now - b.updatedAt) / 1000;
    b.tokens = Math.min(cfg.capacity, b.tokens + elapsed * cfg.ratePerSec);
    b.updatedAt = now;
    let allowed = false;
    if (b.tokens >= 1) { b.tokens -= 1; allowed = true; }
    b.expiresAt = now + Math.ceil((b.tokens >= cfg.capacity ? cfg.capacity - b.tokens : 0) / cfg.ratePerSec * 1000) + 1000;
    await store.saveBucket(`${key}:tb`, b);
    const needed = allowed ? 0 : Math.ceil((1 - b.tokens) / cfg.ratePerSec);
    return {
      allowed,
      remaining: Math.floor(b.tokens),
      retryAfter: needed,
      resetAt: now + needed * 1000,
    };
  };
}

export function createRateLimiter(opts = {}) {
  const cfg = {
    algorithm: opts.algorithm || process.env.RATE_ALGORITHM || 'token_bucket',
    capacity: Number(opts.capacity || process.env.RATE_CAPACITY || 60),
    ratePerSec: Number(opts.ratePerSec || process.env.RATE_PER_SEC || 1),
    windowMs: Number(opts.windowMs || process.env.RATE_WINDOW_MS || 60000),
    by: opts.by || process.env.RATE_BY || KEY_IP,
    keyHeader: opts.keyHeader || process.env.RATE_KEY_HEADER || KEY_HEADER,
  };
  const store = opts.store || new MemoryStore();
  const run = limiterFor(cfg.algorithm, cfg, store);

  return {
    config: cfg,
    check: async (req) => {
      const headerKey = cfg.by === 'header' ? req.headers[cfg.keyHeader.toLowerCase()] : null;
      const id = headerKey || req.socket.remoteAddress || 'unknown';
      const r = await run(id);
      return {
        ...r,
        headers: {
          'X-RateLimit-Limit': String(cfg.capacity),
          'X-RateLimit-Remaining': String(r.remaining),
          'X-RateLimit-Reset': String(Math.ceil(r.resetAt / 1000)),
          ...(r.allowed ? {} : { 'Retry-After': String(r.retryAfter) }),
        },
      };
    },
    sweep: () => store.sweep?.(),
  };
}

export function createProxyServer(opts = {}) {
  const limiter = opts.limiter || createRateLimiter(opts);
  const upstream = opts.upstream || process.env.RATE_UPSTREAM;
  if (!upstream) throw new Error('upstream target required (opts.upstream or RATE_UPSTREAM)');
  const target = new URL(upstream);

  const server = http.createServer(async (req, res) => {
    const r = await limiter.check(req);
    for (const [k, v] of Object.entries(r.headers)) res.setHeader(k, v);

    if (!r.allowed) {
      res.writeHead(429, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'rate_limit_exceeded', retryAfter: r.retryAfter }));
      return;
    }

    const fwd = http.request({
      protocol: target.protocol,
      hostname: target.hostname,
      port: target.port || 80,
      path: req.url,
      method: req.method,
      headers: { ...req.headers, host: target.host, 'x-forwarded-for': req.socket.remoteAddress },
    }, (up) => {
      res.writeHead(up.statusCode || 502, up.headers);
      up.pipe(res);
    });
    fwd.on('error', () => {
      if (!res.headersSent) res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'upstream_unreachable' }));
    });
    req.pipe(fwd);
  });

  if (opts.sweepMs !== false) {
    const t = setInterval(() => limiter.sweep(), Number(opts.sweepMs || 60000));
    t.unref?.();
  }
  return { server, limiter };
}

if (import.meta.url === `file://${process.argv[1].replace(/\\/g, '/')}`) {
  const port = Number(process.env.PORT || 8081);
  const { server } = createProxyServer({});
  server.listen(port, () => console.log(`rate-limiter proxy on :${port} → ${process.env.RATE_UPSTREAM}`));
}
