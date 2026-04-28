---
title: "Operations"
description: "Production deployment, Redis topology, load balancing, and the hetu CLI."
type: docs
weight: 40
prev: advanced
---

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

In HeTu's config the backend is declared like this:

```yaml
backends:
  backend_name:
    type: Redis                        # backend type; 
    master: redis://127.0.0.1:6379/0   # master server, exactly one address
    servants: [ ]                       # read-only replicas; Reads are randomly load-balanced across them
    # URL format: redis://[[username]:password]@host:6379/0  (username may be empty)
```

`System` commits all run against the **master**; ClientSDK
subscriptions/queries (and some `System` reads) run against a **random
servant**. This split lets one HeTu deployment carry a very large user
population while staying consistent.

**About master and servants**

- Add as many `servants` as you need; each is a Redis read-only replica that
  syncs from the master.
- `servants` is optional. Leaving it empty puts you in single-master mode —
  fine for small games.
- Set Redis `client-output-buffer-limit` on the servants conservatively;
  too generous a limit risks Redis OOM under subscription bursts.

**Redis connection budget**

- Servant connections ≈ number of HeTu ClientSDK connections (online users).
- Master connections ≈ `workers × concurrent System calls per worker`.
- Redis caps at ~10K connections per instance. If your concurrent online
  population gets close to that, scale out by adding more `servants`.

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

*You can use the HeTu server root path（https://localhost/） as the health check
endpoint.*

### Why not Nginx

Nginx works, but its config syntax for the dynamic add/remove pattern HeTu
encourages is verbose and easy to get wrong. Caddy fits the workload better.

## Migrations

You'll need a migration whenever `app.py` changes in a way that the live
backend cannot serve as-is:

- **`System` references change** — `Systems` get regrouped into different
  co-location clusters.
- **`Component` schema changes** — a column is added, removed, renamed,
  retyped, or an index changes.

Both are driven by `hetu upgrade`, but they behave very differently.

### Cluster reshuffles — handled automatically

When only the cluster grouping changes, no row data needs to move.
`upgrade` renames the table to its new cluster id and you're done — there's
no script to review and no risk of data loss.

### Schema changes — via a migration script

When a `Component`'s schema changes, the first run of `upgrade` generates a
default migration script under `<your-app-dir>/maint/migration/` — one
file per `Component`, versioned by schema hash. From there:

1. **Most cases — `upgrade` self-completes.** Added columns are filled
   from each property's default value; losslessly castable type changes
   (e.g., `int32 → int64`) are applied automatically. The generated
   script runs on the same `upgrade` invocation; just commit the file
   afterward so every environment migrates the same way.

2. **Lossy cases — you have to intervene.** If a column is dropped or a
   type change can't be cast cleanly, the script's `prepare()` returns
   `unsafe` and `upgrade` refuses to proceed. Two options:

    - **Edit the generated script.** The common case is a "drop + add"
      pair that's really a rename — modify the script's `upgrade()` body
      to copy the old column into the new one before the old is dropped.
    - **Force it with `--drop-data`.** Discards the affected attributes
      outright. Don't use in production.

Commit everything under `maint/migration/` to your repo so deployed
environments don't regenerate (and possibly diverge from) the script you
already reviewed.

Downgrading is currently not supported, may adding this feature in the future.

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

| Flag               | Default                    | Purpose                                                                                             |
|--------------------|----------------------------|-----------------------------------------------------------------------------------------------------|
| `--app-file FILE`  | `/app/app.py`              | Path to your `app.py` (component & system definitions)                                              |
| `--namespace NAME` | —                          | Which namespace from `app.py` to run                                                                |
| `--instance NAME`  | —                          | Logical instance id (each running process needs a unique one for snowflake worker assignment)       |
| `--port PORT`      | `2466`                     | WebSocket listening port                                                                            |
| `--db URL`         | `redis://127.0.0.1:6379/0` | Backend DSN; scheme picks the backend (`redis://`, `sqlite:///`, `postgresql://`, `mysql://`, ...)  |
| `--workers N`      | `4`                        | Worker process count (rule of thumb: `CPU * 1.2`)                                                   |
| `--debug 0/1/2`    | `0`                        | `1` enables hot reload + verbose logs; `2` also enables Python coroutine debug (90% slower)         |
| `--cert DIR`       | `""`                       | TLS cert directory, or `auto` for self-signed; usually better to terminate TLS at the reverse proxy |
| `--authkey KEY`    | `""`                       | Crypto-layer auth key for handshake signature; empty disables it                                    |

Run `hetu start --help` for the full list.

### `hetu upgrade`

Schema migration. Run it before deploying a release that changes a `Component`
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
`Component` definitions. Run it once per `Component` change and commit the
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

The specific definitions are explained in detail within the document's comments.

## Where to next

- **[API Reference](api/)** — every public symbol you'll touch in `app.py`.
- The [README](https://github.com/Heerozh/HeTu/blob/main/README.md) has
  performance benchmarks (in Chinese) for capacity-planning context.
  comments.

