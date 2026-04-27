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
FROM heerozh/hetu:latest
# or use this Aliyun mirror in Shanghai location.
# FROM registry.cn-shanghai.aliyuncs.com/heerozh/hetu:latest

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

The Docker workflow exists primarily so you can run on cheap Spot
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

### Redis Cluster

HeTu uses only basic Redis features (hashes, sorted sets, pub/sub),
moreover, the concept of component clusters is specifically designed to serve database
clusters, so Redis Cluster works without special configuration.
You'll want it once a single shard's write throughput becomes the bottleneck.

However, we do not recommend using the native Redis Cluster, we recommend using Redis
Proxy to achieve same functionality, because this makes it easier to manage Cluster
level read-write separation.

## Load balancing

### Caddy (recommended)

The recommended setup is `caddy-docker-proxy` running in **controller mode**
inside Docker Swarm. HeTu containers carry labels that Caddy reads to
auto-register them in the reverse-proxy pool. When a container stops, Caddy
drops it; when one starts, Caddy adds it. Free TLS via Let's Encrypt is
automatic.

This pairs well with pre-emptible / spot instances: the game-server fleet
churns continuously while the proxy keeps a stable client-facing endpoint.
Game clients need automatic reconnect, this is easily achievable using the official SDK.

If you'd rather not run Swarm, talk to Caddy's admin API directly from your
own orchestration code.

### Why not Nginx

Nginx works, but its config syntax for the dynamic add/remove pattern HeTu
encourages is verbose and easy to get wrong. Caddy fits the workload better.

## The `hetu` CLI

The `hetu` command (run by `uv run hetu`) is your operations
entry point. The three subcommands:

### `hetu start`

The server can be launched in **two mutually exclusive modes**: from a YAML
config file, or entirely from CLI flags. They cannot be mixed — when
`--config` is supplied, all other flags are ignored.

#### Mode 1 — Config file (recommended for production)

```bash
hetu start --config=./config.yml
```

Everything is read from the YAML file. See
[Configuration file](#configuration-file) below for a minimal sample, and
[`CONFIG_TEMPLATE.yml`](https://github.com/Heerozh/HeTu/blob/main/CONFIG_TEMPLATE.yml)
for the full schema.

#### Mode 2 — CLI flags (no config file)

For ad-hoc launches without a YAML:

```bash
hetu start --app-file=./app.py --namespace=my_game --instance=server1 \
    --db=redis://127.0.0.1:6379/0 --port=2466
```

`--app-file`, `--namespace`, and `--instance` are required in this mode.

| Flag               | Default                    | Purpose                                                                                              |
|--------------------|----------------------------|------------------------------------------------------------------------------------------------------|
| `--app-file FILE`  | `/app/app.py`              | Path to your `app.py` (component & system definitions)                                               |
| `--namespace NAME` | —                          | Which namespace from `app.py` to run                                                                 |
| `--instance NAME`  | —                          | Logical instance id (each running process needs a unique one for snowflake worker assignment)        |
| `--port PORT`      | `2466`                     | WebSocket listening port                                                                             |
| `--db URL`         | `redis://127.0.0.1:6379/0` | Backend DSN; scheme picks the backend (`redis://`, `sqlite:///`, `postgresql://`, `mysql://`, ...)   |
| `--workers N`      | `4`                        | Worker process count (rule of thumb: `CPU * 1.2`)                                                    |
| `--debug 0/1/2`    | `0`                        | `1` enables hot reload + verbose logs; `2` also enables Python coroutine debug (90% slower)          |
| `--cert DIR`       | `""`                       | TLS cert directory, or `auto` for self-signed; usually better to terminate TLS at the reverse proxy  |
| `--authkey KEY`    | `""`                       | Crypto-layer auth key for handshake signature; empty disables it                                     |

Run `hetu start --help` for the full list.

### `hetu upgrade`

Schema migration. Run it before deploying a release that changes a Component
shape (added column, changed dtype, new index). It compares the current
schema in the backend against your `app.py` and applies the difference.

Like `hetu start`, it accepts either a config file **or** direct CLI flags
(not both):

```bash
# Mode 1 — from config
hetu upgrade --config=./config.yml

# Mode 2 — from CLI flags
hetu upgrade --app-file=./app.py --namespace=my_game --instance=server1 \
    --db=redis://127.0.0.1:6379/0
```

Extra flags that apply to both modes:

- `-y` — skip the data-backup confirmation prompt (use in CI/CD).
- `--drop-data` — force-migrate by discarding data that cannot be migrated.
  **Do not use in production.**

If you do not run `upgrade`, `hetu start` will refuse to launch when it sees
a schema mismatch.

### `hetu build`

Generates client-side SDK code (typed C# classes) from your server-side
Component definitions. Run it once per Component change and commit the
output into your client project. Unlike `start` and `upgrade`, this command
has no config-file mode — only CLI flags:

```bash
hetu build --app-file=./app.py --namespace=my_game \
    --output=../client/Generated/Components.cs
```

`--namespace` and `--output` are required; `--app-file` defaults to
`/app/app.py`.

This keeps client and server schemas in lockstep without hand-written
boilerplate.

## Configuration file

The full schema is in [
`CONFIG_TEMPLATE.yml`](https://github.com/Heerozh/HeTu/blob/main/CONFIG_TEMPLATE.yml).
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
