# API rate-limiter service

Standalone Node service you drop in front of any HTTP API. Zero runtime dependencies for the
in-memory mode; Redis mode is optional and uses any Redis client that speaks `get/set/zadd/zcard`.

## Run

```bash
RATE_UPSTREAM=http://localhost:3000 RATE_CAPACITY=60 RATE_PER_SEC=1 node rate-limiter.mjs
```

Then point your clients at `:8081` instead of the upstream. Every response carries
`X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`; throttled requests get
`429` plus `Retry-After`.

## Configuration

| Env | Default | Meaning |
| --- | --- | --- |
| `RATE_UPSTREAM` | – | Target to proxy to (required in proxy mode) |
| `RATE_ALGORITHM` | `token_bucket` | `token_bucket` or `sliding_window` |
| `RATE_CAPACITY` | `60` | Burst size (token bucket) / max requests per window |
| `RATE_PER_SEC` | `1` | Sustained refill rate (token bucket) |
| `RATE_WINDOW_MS` | `60000` | Window length (sliding window) |
| `RATE_BY` | `ip` | `ip` or `header` to key on |
| `RATE_KEY_HEADER` | `x-api-key` | Header used when `RATE_BY=header` |
| `PORT` | `8081` | Listen port |

## Library use

```js
import { createRateLimiter, MemoryStore, RedisStore } from './rate-limiter.mjs';

const limiter = createRateLimiter({ algorithm: 'token_bucket', capacity: 10, ratePerSec: 0.5 });

// inside any http/node request handler
const r = await limiter.check(req);
if (!r.allowed) return res.writeHead(429, r.headers).end();
Object.entries(r.headers).forEach(([k, v]) => res.setHeader(k, v));
```

## Redis (multi-instance)

```js
import { createClient } from 'redis';
import { createProxyServer, RedisStore } from './rate-limiter.mjs';

const client = createClient({ url: process.env.REDIS_URL });
await client.connect();
createProxyServer({ store: new RedisStore(client), upstream: process.env.RATE_UPSTREAM }).server.listen(8081);
```

Buckets are stored under `rl:tb:<key>` and sliding-window sets under `rl:sw:<key>`, both with
TTLs, so Redis evicts state on its own.

## Design notes

- **Token bucket** — smooths bursts, allows `capacity` in one go then refills at `ratePerSec`.
  Best for protecting an upstream that tolerates short bursts.
- **Sliding window** — hard cap of `capacity` requests per `windowMs`, counted from the real
  timestamps of past hits. Best for quota/billing semantics.
- Keyed per client IP, or per API key header when clients already present one. Client IP uses
  `x-forwarded-for` only as an inbound hint; set your own trusted proxy layer before relying on it.
- `sweep()` prunes idle in-memory state every 60s so memory stays flat under many distinct IPs.
