---
title: "Operations"
description: "Production deployment, Redis topology, load balancing, and the hetu CLI."
type: docs
weight: 40
prev: advanced
---

# Operations

Everything you need to take a working dev setup to production: deployment
options, Redis topology, reverse-proxy setup, and a CLI reference.

## Deployment options

### Docker (recommended)

The published image (`heerozh/hetu:latest`, mirrored at
`registry.cn-shanghai.aliyuncs.com/heerozh/hetu:latest` for mainland China)
gives you a Python 3.14 environment with HeTu pre-installed. Your project
extends it:

```dockerfile
# Mainland-China users: replace the registry with the Aliyun mirror above.
FROM registry.cn-shanghai.aliyuncs.com/heerozh/hetu:latest

WORKDIR /app

COPY . .
RUN pip install .

ENTRYPOINT ["hetu", "start", "--config=./config.yml"]
```

Your project must follow the **src-layout** for `pip install .` to succeed
(`pyproject.toml` plus a `src/<package>/` directory).

Build and run:

```bash
docker build -t my-game .
docker run -it --rm -p 2466:2466 --name game-srv my-game
```

The Docker workflow exists primarily so you can run on cheap pre-emptible
instances behind a reverse proxy: containers come and go, and the proxy
drops or adds them automatically.

### `pip` native (no container)

For a long-running dedicated host, native `pip` install avoids the ~20%
container overhead. Install Python 3.14 (via `uv`, `conda`, or your distro's
package manager), then:

```bash
cd your_app_directory
pip install .
# Use python -O in production to disable asserts (small perf win)
python -O -m hetu start --config=./config.yml
```

## Redis topology

### Master + read replicas (baseline)

The recommended starting point: one Redis master for writes, several read
replicas for the subscription fan-out. The
[`SubscriptionBroker`](concepts.md#subscriptions) reads from any replica, so
adding replicas scales subscription throughput linearly without touching the
master.

### Redis Cluster

HeTu uses only basic Redis features (hashes, sorted sets, pub/sub), so
Redis Cluster works without special configuration. You'll want it once a
single shard's write throughput becomes the bottleneck.

### Drop-in alternatives

The same wire protocol means you can substitute:

- **ValKey** — open-source Redis fork, tracks the same protocol.
- **Aliyun Tair** — managed Redis-compatible service with proxy support.
- **Redis Proxy** — for transparent sharding in front of multiple Redis
  instances.

### Persistence

Enable AOF or RDB on the Redis side; HeTu doesn't choose for you, but it does
expect the backend to survive restarts. Pure-volatile setups will lose state
between deploys.

## Load balancing

### Caddy (recommended)

The recommended setup is `caddy-docker-proxy` running in **controller mode**
inside Docker Swarm. HeTu containers carry labels that Caddy reads to
auto-register them in the reverse-proxy pool. When a container stops, Caddy
drops it; when one starts, Caddy adds it. Free TLS via Let's Encrypt is
automatic.

This pairs well with pre-emptible / spot instances: the game-server fleet
churns continuously while the proxy keeps a stable client-facing endpoint.
Game clients need automatic reconnect, which the official SDKs already do.

If you'd rather not run Swarm, talk to Caddy's admin API directly from your
own orchestration code.

### Why not Nginx

Nginx works, but its config syntax for the dynamic add/remove pattern HeTu
encourages is verbose and easy to get wrong. Caddy fits the workload better.

## The `hetu` CLI

The `hetu` command (installed by `pip install hetudb`) is your operations
entry point. The three subcommands:

### `hetu start`

Starts the server. Most common form:

```bash
hetu start --config=./config.yml
```

CLI flags override config-file fields. The most useful ones:

| Flag | Purpose |
|---|---|
| `--config FILE` | Load `config.yml` (see [`CONFIG_TEMPLATE.yml`](https://github.com/Heerozh/HeTu/blob/main/CONFIG_TEMPLATE.yml) for the full schema) |
| `--app-file FILE` | Path to your `app.py` (component & system definitions) |
| `--db URL` | Backend DSN: `redis://host:6379/0`, `sqlite:///path.db`, `postgresql://user:pw@host/db`, or `mysql://...` |
| `--namespace NAME` | Which namespace from `app.py` to run |
| `--instance NAME` | Logical instance id (used for snowflake worker assignment; each running process needs a unique one) |

Run `hetu start --help` for the full list.

### `hetu upgrade`

Schema migration. Run it before deploying a release that changes a Component
shape (added column, changed dtype, new index). It compares the current
schema in Redis against your `app.py` and applies the difference:

```bash
hetu upgrade --config=./config.yml
```

If you do not run `upgrade`, `hetu start` will refuse to launch when it sees
a schema mismatch.

### `hetu build`

Generates client-side SDK code (typed C# classes, JS types) from your
server-side Component definitions. Run it once per Component change and
commit the output into your client project:

```bash
hetu build --config=./config.yml --target=csharp --output=../client/Generated/
```

This keeps client and server schemas in lockstep without hand-written
boilerplate.

## Configuration file

The full schema is in [`CONFIG_TEMPLATE.yml`](https://github.com/Heerozh/HeTu/blob/main/CONFIG_TEMPLATE.yml).
A minimal production `config.yml`:

```yaml
APP_FILE: app.py
NAMESPACE: my_game
INSTANCES:
  - server1
  - server2

LISTEN: 0.0.0.0:2466
WORKER_NUM: -1          # auto-detect CPU count
DEBUG: false
ACCESS_LOG: false
PROXIES_COUNT: 1        # one reverse proxy in front

BACKENDS:
  default:
    type: redis
    master: redis://master.internal:6379/0
    servants:
      - redis://replica1.internal:6379/0
      - redis://replica2.internal:6379/0
```

Key things to set per-environment:

- `INSTANCES` — one entry per process replica you'll run on this host.
- `WORKER_NUM` — set to `-1` for auto, or pin to a specific number when you
  share the host with other workloads.
- `BACKENDS.default.master` and `.servants` — split reads across replicas.
- `MAX_ANONYMOUS_CONNECTION_BY_IP`, `CLIENT_SEND_LIMITS`, `MAX_ROW_SUBSCRIPTION` —
  rate limits applied to unauthenticated connections; defaults are
  conservative. Authenticated connections (post-`elevate`) get 10x / 50x
  multipliers.

## Logging

HeTu uses a process-safe queue/listener (`hetu/safelogging/`). Logging is
configured in the `LOGGING:` section of `config.yml` (standard Python
`dictConfig` schema). Common production setup: root logger at `WARNING`,
`HeTu.*` at `INFO`, file handler with rotation.

## Where to next

- **[API Reference](api/)** — every public symbol you'll touch in `app.py`.
- The [README](https://github.com/Heerozh/HeTu/blob/main/README.md) has
  performance benchmarks (in Chinese) for capacity-planning context.
