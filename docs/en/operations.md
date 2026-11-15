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

COPY .. .
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
instances behind a gateway: machines come and go, and the gateway discovers
them automatically (see Load balancing below). For a multi-machine
deployment, turn node discovery (`DISCOVERY`) on and run on the host network
(`docker run --network host`, without `-p`).

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
- Servants need `notify-keyspace-events` enabled (HeTu `CONFIG SET`s it at
  startup when it has permission, and warns otherwise).
- **Replication lag budget**: a subscription notification carries no content,
  so the server re-reads the row from a replica (chosen by the proxy in a proxy
  topology). After every notification, and after a subscription becomes active
  or pub/sub reconnects, the server reads again at least `1/UPDATE_FREQUENCY`
  (100 ms by default) later, so as long as replica lag stays below that, clients
  always end up with the latest value. When lag exceeds it (replica CPU
  saturated, full resync, network hiccups), a client may keep stale data until
  that row changes again — which is why subscription pushes are only guaranteed
  to be up to date in about 99% of cases. Monitor replica replication lag (e.g.
  the "sync delay" metric in your cloud console) and keep it well below 100 ms.
- Every logged-in connection also subscribes to the `Connection` table's
  `owner == this user` index-value channel, so when it gets kicked the server
  closes it proactively and the RPC path no longer reads the row on every
  call; if a notification is lost, `CONNECTION_ALIVE_RECHECK_INTERVAL`
  (default 5 s) re-checks as a fallback. Before disconnecting, the server
  sends a WebSocket close frame with code `4001` and reason `kicked`, so the
  client can tell the user "logged in elsewhere" — and should not reconnect
  automatically (reconnecting logs in again and kicks the other side).
- Every server-initiated disconnect carries a close code, and clients decide
  whether to reconnect from it: `4002 rejected` means the client broke the
  rules (illegal call, malformed message, send-rate limit — typically a
  client newer than the server), `1011 internal error` means a server-side
  error (backend unreachable, a System raised). Everything in 4000-4999 is
  terminal and the SDK does not reconnect; 1011 and network loss (1006, no
  close frame) reconnect with backoff. The details are only in the server log.
- `servants` is optional. Leaving it empty puts you in single-master mode —
  fine for small games.
- Set Redis `client-output-buffer-limit` on the servants conservatively;
  too generous a limit risks Redis OOM under subscription bursts.

**Redis connection budget**

- Each worker process keeps **one** pub/sub connection per servant (per Redis
  node in cluster mode) that fans notifications out to all of its WebSocket
  connections, plus a bounded pool of short-lived read/write connections
  (`max_connections`, default 64, queued when full). So connections per
  servant ≈ `workers × (1 + max_connections)` — independent of the number of
  online users.
- Master connections ≈ `workers × (1 + max_connections)` as well; the pool is
  shared by all concurrent System calls of a worker.
- Redis caps at ~10K connections per instance; with the defaults that is
  ~150 workers per instance. Scale subscription *throughput* (not connections)
  by adding `servants`: each worker's pub/sub connection and reads are spread
  over them at random.

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
For most projects, the read/write split is more than
enough; You'll want it once a single shard's write throughput becomes the bottleneck.

However, we do not recommend using the native Redis Cluster, we recommend using Redis
Proxy to achieve same functionality, because this makes it easier to manage Cluster
level read-write separation.

To stay ready for that future, design your `Components` with sharding in mind:

- HeTu distributes data by computing whether `Systems` overlap on
  `Components` — that overlap forms the **System clusters** that get pinned
  to a shard.
- The more finely you split `Components`, the more System clusters you get,
  and the better Cluster performance scales.
- This is not automatic. While writing code, actively watch for *hub
  `Components`* — a single `Component` referenced by many otherwise
  unrelated `Systems` collapses everything into one giant cluster.
- Avoid wide "god tables". Split each kind of attribute into its own
  `Component`.
- Use the `hetu` CLI during development to inspect each cluster's size and
  catch hub-shaped growth early.

## Load balancing

You can run any number of HeTu servers to add capacity in parallel, with a
gateway in front of them. The reference deployment (multiple machines in the
cloud):

- **Gateway**: one fixed machine (not a spot instance) with a public IP,
  running the official Caddy image. It obtains certificates, terminates TLS,
  writes access logs and forwards the websockets to HeTu. Next to it runs
  `hetu dns`, which tells Caddy which HeTu nodes are up.
- **HeTu**: runs in an autoscaling group, cheap spot instances included, one
  container per machine (`WORKER_NUM` set to the CPU count), on the host
  network, with `DISCOVERY` turned on in the config. The group's health check
  can probe HeTu's root path `/`.
- **Node discovery**: once a node is serving, it registers its private address
  in Redis (the default backend, key `hetu:nodes`), refreshes the entry every
  5 seconds and withdraws it first when it shuts down. `hetu dns` reads the
  registry every second and answers it as the A records of `hetu.internal`,
  which Caddy's built-in dynamic upstreams (`dynamic a`) look up every 2
  seconds. Scaling never touches Caddy's config, needs no scripts, and the
  nodes need no cloud API credentials; Caddy doesn't reload, and connections on
  the other machines are left alone.

### Nodes

Turn node discovery on in the config:

```yaml
DISCOVERY: true
PROXIES_COUNT: 1        # nodes sit behind the gateway: take the player's address from X-Forwarded-For
LISTEN: 0.0.0.0:2466    # every node uses the same port
```

Run on the host network (no `-p`):

```bash
docker run -d --restart always --network host --name game-srv my-game
```

**Only the host network is supported.** On a bridge network a node only sees
the address the Docker bridge gave it, which the gateway on another machine
can't reach; the host network also skips the port-mapping hop, so it is the
fastest. In compose that's `network_mode: host`, in k8s `hostNetwork: true`.

The address a node registers: the IP in `LISTEN` if you wrote one; with
`0.0.0.0`, the local address the node uses to reach the Redis master (the
gateway and Redis normally share a private network, so the gateway can reach
it). If the machine has several NICs and Redis and the gateway are reached
through different ones, write the IP in `LISTEN`, e.g.
`LISTEN: ${HOST_IP}:2466`.

Before serving, `hetu start` checks the deployment. If a check fails it prints
the cause and the fix and exits with code 2, before anything is started:

- not on the host network (the address is on a container's virtual NIC), or
  the address is on a local container bridge (docker0 and the like);
- `LISTEN` is bound to a loopback address, is a host name, or names an IP this
  machine doesn't have;
- `PROXIES_COUNT` is 0: every player would appear to come from the gateway, and
  the per-IP limit on anonymous connections would lock players out;
- the default backend isn't Redis, or the address can't be worked out (Redis
  on the same machine, or over a unix socket); write the IP in `LISTEN` then.

### Gateway

compose:

```yaml
services:
  caddy:
    image: caddy:2
    network_mode: host
    restart: always
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile
      - caddy_data:/data
  hetu-dns:
    image: heerozh/hetu:latest
    network_mode: host
    restart: always
    command: ["dns", "--namespace", "my_game"]
    environment:
      # point it at a Redis replica, using the read-only user created below
      HETU_DNS_REDIS: redis://hetu-dns:${DNS_REDIS_PASSWORD}@10.0.1.20:6379/0
volumes:
  caddy_data:
```

Caddyfile:

```
game.example.com {
	reverse_proxy {
		# ask hetu dns every 2 seconds for the current HeTu nodes
		dynamic a {
			name hetu.internal
			port 2466
			refresh 2s
			resolvers 127.0.0.1:1053
			versions ipv4
		}
		# websockets are long-lived: send new ones to the least busy node
		lb_policy least_conn
		# if a node can't be reached, try others for 5s and skip it for 30s
		lb_try_duration 5s
		fail_duration 30s
		# when you do edit the Caddy config, keep open websockets up to 2 hours
		stream_close_delay 2h
		transport http {
			dial_timeout 2s
		}
	}
}
```

- `--namespace` is the `NAMESPACE` in the nodes' config. `port` must match the
  nodes' `LISTEN` port; if it isn't 2466, pass `--port` to `hetu dns` as well.
  If `name` isn't `hetu.internal`, pass `--name`.
- Keep `refresh` at 2s: a node keeps serving for 4 seconds after withdrawing
  its entry before it stops listening, and that is sized for it.
- `versions ipv4`: without it Caddy looks up both A and AAAA every time.

The Redis user for `hetu dns` only needs to read `hetu:nodes`. The gateway
faces the internet, so don't give it a user that can read or write anything
else. Create it on the replica `hetu dns` connects to (ACLs are not
replicated; on a managed Redis, create the account in the console), and add
`+select` if you don't use database 0:

```
ACL SETUSER hetu-dns on >password resetkeys ~hetu:nodes resetchannels -@all +hgetall +ping
```

Give the replica's default user a password too, or disable it. The gateway has to
reach the replica's Redis port, and if the default user needs no password, an
attacker who gets into the gateway simply uses it: they can read all the data and
even change the replica's replication setup, and the read-only user protects
nothing. With a password set, put it in the nodes' `BACKENDS` URLs; with the
default user disabled, the nodes use another user.

`hetu dns` checks its deployment at startup too. It exits with code 2 when it
isn't on the host network (Caddy queries the host's 127.0.0.1, which a bridge
network never sees) or when Redis is a native cluster (not supported in the
first version; go through a proxy); with code 3 when it can't connect to
Redis, the password is wrong, or the ACL refuses it (it prints the user to
create); and it warns when the user is too powerful, when Redis accepts a
connection without a password (the default user has none), when it is
connected to the master, or when `--listen` isn't a loopback address. At run time, a node
that shows up for the first time is probed with one TCP connection from the
gateway; until that succeeds the node isn't answered, and the log says to
check the security group.

Troubleshooting:

- On the gateway: `hetu dns --namespace my_game --once` (with the same
  environment) prints which addresses would be answered, which wouldn't, and
  why.
- On a node or an ops machine: `hetu nodes --config config.yml` prints a
  summary of the registry: workers, connections and state per address, plus
  stale entries.

### Things to know

- **The client's real IP.** Caddy adds `X-Forwarded-For`, and with
  `PROXIES_COUNT: 1` (the config template's default) HeTu reads the client
  address from it; the per-IP limit on anonymous connections depends on it.
  HeTu only reads this header, not `Forwarded`. Use a security group so that
  only the gateway can reach HeTu's port, or `X-Forwarded-For` can be forged.
  If you put a CDN or load balancer in front of the gateway, add its addresses
  to Caddy's `trusted_proxies` and set `PROXIES_COUNT` to the total number of
  proxies.
- **Stopping a HeTu machine** (scaling in, a spot instance getting its reclaim
  notice, an upgrade): just stop the server (see "Shutting down" below). The
  node withdraws its registry entry first and keeps serving for 4 seconds
  until Caddy has picked that up and stops sending it new connections; only
  then does it stop listening. Its players get a 1001 and reconnect to another
  machine automatically. A node restarted in place gets new connections as
  soon as it is serving again.
- **A machine that vanished without shutting down** (a crash, a kill, a
  reclaimed spot instance): once its entry hasn't been refreshed for 15
  seconds, `hetu dns` stops answering it. Until then Caddy fails to reach it
  and moves on, so players just wait a moment.
- **Many players reconnecting at once** (the gateway restarts, or every node
  restarts together): connections that don't fit in HeTu's listen queue are
  dropped by the kernel, and a node Caddy can't reach within 2 seconds is
  skipped for 30 seconds as well. The queue defaults to 2048
  (`LISTEN_BACKLOG`); on Linux it is also capped by `net.core.somaxconn`
  (4096 by default since kernel 5.4). Raise both if one machine takes more
  connections at once. The SDK adds random jitter to its reconnect delay,
  which spreads out players who dropped at the same moment. Planned shutdowns
  and restarts never get a node skipped (a node registers only once it is
  serving, and stops listening only after Caddy has dropped it); only crashed
  or killed nodes do.
- **The gateway is a single point of failure.** Use a stable machine; if it
  goes down, players reconnect automatically. At larger scale, add gateways
  behind your cloud's load balancer. If `hetu dns` dies, new connections can't
  get in (open ones are unaffected), so give it `restart: always`.
- **Running on a single cloud?** You can also attach the autoscaling group to
  the cloud's load balancer, which needs no scripts either; the price is the
  load balancer's monthly fee and being tied to that cloud.

## Shutting down

On SIGTERM (what `docker stop`, systemd and k8s send) or Ctrl+C, `hetu start`
shuts down in this order:

1. With node discovery (`DISCOVERY`) on, withdraw from the registry first
   and keep serving for 4 seconds until the gateway has picked that up (see
   Load balancing above).
2. Stop accepting new connections.
3. **Drain**: every connection stops reading new messages; RPCs already
   received run to completion and their replies are sent, then the connection
   is closed with close code 1001 (the SDK reconnects automatically). Messages
   queued behind them are not executed, so resending them after reconnecting
   does not run anything twice. This waits at most `SHUTDOWN_DRAIN_TIMEOUT`
   (5 seconds by default); RPCs still running then are cancelled. A
   cancellation that lands before the commit means nothing is committed, but
   one that lands in the middle of the commit may leave it committed with the
   reply lost, so the client's retry after reconnecting runs it again. For
   calls that must not run twice (charges, item grants), have the client send an
   order id and call a `call_lock=True` System from an Endpoint with
   `ctx.systems.call(..., uuid=order_id)` to deduplicate (see
   [Advanced](advanced.md)).
4. Run the disconnect System and delete the Connection rows, waiting at most
   `GRACEFUL_SHUTDOWN_TIMEOUT` (15 seconds by default).
5. Stop FutureCalls, delete the registry entry, release the Worker ID lease
   and disconnect from the backends.

Further SIGTERMs or Ctrl+Cs during shutdown do not interrupt it (the exception
is a single-process server on Windows, where a second Ctrl+C forces an exit).

With several workers, the manager process tells each worker to stop and waits
up to `SHUTDOWN_DRAIN_TIMEOUT + 3 × GRACEFUL_SHUTDOWN_TIMEOUT + 10` seconds
(60 by default; 5 more with node discovery on, so 65) for it, then kills
any worker still running. A normal shutdown
takes nowhere near that long; it is the worst case, because after draining,
cancelling the connections, waiting for them to be torn down and stopping
FutureCalls each get their own `GRACEFUL_SHUTDOWN_TIMEOUT`.

**Give your orchestrator a longer grace period than that.** Docker waits only
10 seconds and k8s 30 before sending SIGKILL, and a kill skips the cleanup
above: disconnect Systems don't run, and `hetu upgrade` has to wait for the
Worker ID leases to expire (at most 60 seconds). With the default settings, set
the grace period to 70 seconds or more, or lower the two timeouts accordingly:

- docker compose: `stop_grace_period: 70s`
- `docker stop -t 70`
- k8s: `terminationGracePeriodSeconds: 70`

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

If any [`hetu.headless`](advanced.md#reading-and-writing-tables-from-a-non-server-process-hetuheadless)
processes share the backend, restart them after the migration: they write
under the cluster id they read at startup, and without a restart their rows
would land under the old prefix.

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

Volatile components (`volatile=True`) skip the migration script: `upgrade`
wipes their data anyway, so on any schema change it simply recreates the
table under the new definition — dropped or retyped columns don't need
`--drop-data`.

Commit everything under `maint/migration/` to your repo so deployed
environments don't regenerate (and possibly diverge from) the script you
already reviewed.

Downgrading is currently not supported, may adding this feature in the future.

## The `hetu` CLI

The `hetu` command (run by `uv run hetu`) is your operations
entry point: `start`, `upgrade` (migrations), `build` (client codegen), plus
`call` / `get` / `range` / `shell` for debugging (see
[Debugging from the command line](#debugging-from-the-command-line)), and
`dns` (the gateway's DNS bridge, which reads no config file) / `nodes` (shows
the registry) for node discovery (see [Load balancing](#load-balancing)).

Subcommands that need a config look for it in this order: `--config` > CLI-flag
mode (any of `--app-file` / `--namespace` / `--db` given) > the `HETU_CONFIG`
environment variable > `config.yml` in the current directory. The official
Docker image sets `HETU_CONFIG=/app/config.yml`. Relative paths for `APP_FILE`
and SQLite database files (`sqlite:///./hetu.db`) resolve against **the config
file's directory**, wherever you run the command from.

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
[`hetu/CONFIG_TEMPLATE.yml`](https://github.com/Heerozh/HeTu/blob/main/hetu/CONFIG_TEMPLATE.yml)
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
| `--db URL`         | `redis://127.0.0.1:6379/0` | Backend DSN; scheme picks the backend (`redis://`, `rediss://`, or `sqlite:///<file>` for dev)      |
| `--workers N`      | `4`                        | Worker process count (rule of thumb: `CPU * 1.2`)                                                   |
| `--debug 0/1/2`    | `0`                        | `1` enables hot reload + verbose logs; `2` also enables Python coroutine debug (90% slower)         |
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
- `--no-rebuild-index` — skip rebuilding indexes (see below).

If you do not run `upgrade`, `hetu start` will refuse to launch when it sees
a schema mismatch.

By default every `upgrade` rebuilds the indexes of persistent `Component`s
from the row data, which repairs leftover index entries — for example, a
maintenance script deleted a row but not its index entry, the server log
reports that the index and the row data don't match, and every `System` that
reads that row keeps retrying. Run it with the servers **stopped** (writes
between scanning the rows and replacing the index would be lost). All indexes
are built and checked in full before they replace the old ones, so a failed
rebuild leaves the old indexes untouched. With a lot of data the rebuild is
slow; pass `--no-rebuild-index` to skip it. When you add or remove `groupby`
(composite index), or change `unique` / `index`, the migration rebuilds that
component's indexes even with `--no-rebuild-index`: the index definition
changed, and the old index could not be queried.

A migration that adds, drops or changes columns first has the migration script
move the data to the new definition, and only then rebuilds the indexes. If that
rebuild fails (e.g. a unique conflict, or duplicate numbers in a commit sequence
group), the data has already been moved and the old indexes are gone: the table
is marked "rebuild pending" and the server refuses to start. Fix the rows
directly (e.g. with `TableMaintenance.upsert_row`) and run `upgrade` again; it
rebuilds with the current definition right away.

The same happens when HeTu itself changes how indexes are stored. For example,
after rows sharing an index value started being ordered by the numeric `id`,
indexes written by older versions sort them wrongly: after upgrading HeTu every
table reports that it needs migrating and the server refuses to start; one
`upgrade` run rebuilds the indexes of all persistent components in the new
format (regardless of `--no-rebuild-index`), without moving row data or
generating migration scripts. Do not run old and new versions against the same
database during this upgrade either.

Turning an index that is in use into a composite index is a **breaking
change**: every query without group values fails afterwards, older client SDKs
subscribing to that index are disconnected by the server (4002, which the SDK
treats as final and does not reconnect), and a new SDK sending group values to
an older server is disconnected too. Prefer adding the composite index on a
**new field**, switch the server code and the clients over, then drop the old
index. Older HeTu versions cannot read the schema of a component that uses
`groupby`, so do not run old and new versions against the same database while
upgrading.

Commit sequence columns (`commit_seq`) are numbered from one counter per group;
keep these in mind when migrating and maintaining:

- **Turning `commit_seq` on**: on an existing sequence field the values are
  kept, and the index rebuild sets each group's counter to its largest number so
  numbering carries on; the rebuild happens even with `--no-rebuild-index`. If a
  group has duplicate numbers or a number reaches 2^53, `upgrade` fails: when
  only `commit_seq` changed it changes nothing, so write a migration script that
  renumbers them first; when columns were also added, dropped or changed, the
  migration script has already moved the data and the table is marked rebuild
  pending (see above): fix the rows and run `upgrade` again. When
  you add a new `commit_seq` column, existing rows get 0, i.e. no number; to
  number them, assign 1, 2, 3… per group in the migration script.
- **Custom migration scripts** should follow the template: rename the original
  table to a temporary one, copy, then drop the temporary table — don't drop the
  original table. The counters stay under the original table name and keep
  going after the migration. If you rename the component or the field, or
  change the `groupby` of the `commit_seq` field, the old counters no longer
  apply; new ones are calibrated from the data, so numbers of deleted rows at
  the tail may be handed out again. Counters are not cleaned up after turning
  `commit_seq` off or removing the field.
- **After importing only rows, or deleting a counter by hand**, the counter is
  behind the data and would hand out duplicate numbers; run `upgrade` once (it
  rebuilds all indexes by default) to recalibrate. Restoring the whole database
  from a backup keeps data and counters consistent, so no calibration is needed;
  but numbering goes back to where the backup was, and numbers handed out after
  the backup will be handed out again — consumers that keep their cursor
  outside the database must check snowflake ids with `servant_seq_read` and
  `after_id` (see the consumers-outside-the-database subsection of
  "Binlog-style syncing" in [Advanced](advanced.md)).
- **Volatile tables** are cleared at every maintenance, and numbering starts
  over from 1.
- **Don't use an `allkeys-*` eviction policy**: counters (and data rows) can be
  evicted, and a missing counter hands out duplicate numbers.
- When the group value is something disposable, such as a match or a session,
  every group keeps one counter key (about 100 bytes) even after the group is
  emptied, so they keep piling up.
- **Upgrade headless clients before turning `commit_seq` on**: older servers,
  `LocalApp`, and headless clients that connect by component name fail on a
  schema that uses `commit_seq` and refuse to start, but an older headless
  client that connects with a component class does not fail: it keeps writing
  rows without a number (0), which consumers never see.

Before doing anything, `upgrade` checks whether any server is still running
(on Redis backends, from the worker leases) and exits with code 1 if so,
leaving everything untouched: migrating, wiping volatile tables and rebuilding
indexes all corrupt data while servers are running. If a server crashed, wait
for its lease to expire (at most 60 seconds) and try again. On Windows there is
no need to wait for servers on the same machine that have already exited: a
process that is force-killed (ended from Task Manager, or a second Ctrl+C on a
single-process server) never gets to release its lease, and such leases are
not counted. Servers on the SQLite backend
have no leases, so the check can't see them; make sure they are stopped
yourself. `hetu call` / `hetu shell` hold leases too (on Redis and SQLite), so
`upgrade` also refuses while they run and lists them separately.

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

## Debugging from the command line

`hetu call` / `get` / `range` / `shell` **connect to the configured backend
from the current process** to call Systems as admin or as a given player and to
read component rows. They don't go through the server and don't need one
running (they work on a SQLite dev database too). Writes go through the same
commit path as Systems, so online clients get their subscription pushes as
usual. Whoever can run them already holds the database address and
credentials, so the server gains no new network entry point.

```bash
hetu call --list                          # every System: params, permission, components, first doc line
hetu get --list                           # components, fields, indexes
hetu call add_gold 1001 500               # ADMIN System, runs as admin by default
hetu call --as 1001 buy_item 3 2          # USER Systems need a player id
hetu call --as 1001 --group gm gm_kick 2002
hetu call send_mail --args-file args.json # args from a JSON-array file (- for stdin)
hetu call add_gold 1001 500 --dry-run     # run it but don't commit; shows what it would write
hetu get Player owner=1001                # one row by id or an indexed field
hetu get Item template=3 owner=1001       # composite index: the field plus every group-by column (--index picks one if several match)
hetu range Item owner 1001 --limit 50     # omit the right bound for an exact match; ( / [ prefix = open / closed
hetu range Rank level 10 20 --group zone=3  # composite index: one --group per group-by column, values taken literally
hetu shell -c "await call_system('add_gold', 1001, 500); show(await get('Player', owner=1001))"
```

- **Output**: `call` / `get` / `range` / `--list` print exactly one JSON line
  on stdout; logs and `print`s from your app go to stderr. `call` reports the
  System's raw return value `result`, what the client would actually receive
  as `client` (or `wire_error` when it can't be serialized), the race-retry
  count, the call's write set `writes`, and `target` (config file, instance,
  backend addresses with passwords masked). Failures carry `error_type` and
  `error`, plus the full `traceback` when your code raised. A commit that was
  interrupted mid-flight (a timeout, say) shows `committed: "unknown"` in
  `writes`: it may have taken effect, so check the data before re-running.
- **Exit codes**: 0 success; 1 code or call failure (the System raised, timed
  out); 2 usage error (arguments, identity, unknown name); 3 environment not
  ready (no config, missing database file or table, schema differs from the
  code, write protection).
- **Identity**: without `--as` / `--group` it follows the System's permission:
  ADMIN, GM and internal Systems (`permission=None`) run as admin, EVERYBODY as
  guest, and **USER requires `--as`** (production USER endpoints never admit
  logged-out connections, and running as player 0 would create owner=0 rows).
  With `--as` the group defaults to guest. There is no connection:
  `ctx.connection_id` is 0 and `ctx.request` is None, so Systems that call
  `elevate` fail; pass login-time `ctx.user_data` with `--user-data`.
- **Arguments**: each one is parsed as JSON, falling back to a plain string;
  parameters annotated `str` are never parsed. Something that looks like JSON
  but doesn't parse is an error (PowerShell often eats the quotes); use
  `--args-file` or stdin then. Put non-numeric arguments starting with `-`
  after `--`.
- **Reads**: `get` / `range` don't load your app; the schema comes from the
  table meta in the database, so they work even when the app code is broken.
  They read a replica by default; use `--master` right after a write. `range`
  returns at most 10 rows by default and `truncated` says whether there were
  more.
- **Schema**: `call` / `shell` never create or migrate tables. A table that is
  missing, or whose schema / cluster differs from your local code, is an
  error: start the server once (it creates new tables) or run `hetu upgrade`.
- **`hetu shell`** preloads `call_system` / `get` / `must_get` / `insert` /
  `upsert` (the same API as the test `Sandbox`), plus `app.range` and `show()`
  (prints as JSON, numpy rows with field names), with top-level await. Code
  comes from `-c`, a script file or stdin; with none of them you get an
  interactive prompt. Scripts run as `__main__` (with `__file__` set); at the
  interactive prompt Ctrl+C interrupts only the running statement and Ctrl+D
  exits. Restart the shell after changing code.

**It runs your local code**: `hetu call` re-imports your local app every time,
which says nothing about what the server has loaded — a single-worker server
doesn't auto-reload, so restart it before testing the client path. In
production run it inside the deployed container (`docker exec <container> hetu
call ...`) rather than writing to the production database from a dev checkout:
the schema check can't catch logic differences. Also: background tasks a
System starts with `create_task` are cancelled when the command exits,
initialization hooked up in `on_server_setup` doesn't run, and FutureCalls
created from the CLI are executed by the running servers.

**Write protection and audit**: set `CLI_ALLOW_WRITE` (default true) to false
and the CLI can only read or `--dry-run`; real writes fail with exit code 3.
On a config with `DEBUG` off, real writes produce a warning. Every `call` and
`shell` run is appended to `CLI_AUDIT_LOG` (default
`logs/hetu_cli_audit.jsonl` next to the config): who, on which host, the
command and arguments (database passwords masked), which rows each commit
changed, and the outcome. The
audit stays on the machine that ran the CLI; the server keeps no trace.

## Configuration file

The full schema is in [
`hetu/CONFIG_TEMPLATE.yml`](https://github.com/Heerozh/HeTu/blob/main/hetu/CONFIG_TEMPLATE.yml).

The specific definitions are explained in detail within the document's comments.

## Where to next

- **[API Reference](../api/)** — every public symbol you'll touch in `app.py`.
- The [README](https://github.com/Heerozh/HeTu/blob/main/README.md#-benchmarks) has
  performance benchmarks for capacity-planning context.

