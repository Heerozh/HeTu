# HeTu project overview

- Purpose: HeTu (河图) is a high-performance, auto-scaling game server engine using ECS architecture with Redis backend, exposing DB-like APIs via WebSocket for subscriptions and RPC.
- Languages: Python (primary), C# (Client SDK source generation / Unity client).
- Runtime/frameworks: Sanic server, Redis backend, NumPy structured arrays, PyNaCl crypto.
- Core abstractions: Component (`@define_component`), System (`@define_system`), Endpoint (`@define_endpoint`).
- Data flow: WebSocket client -> Sanic worker -> EndpointExecutor/SystemCaller/SubscriptionBroker -> Redis.
- Important modules: `hetu/data`, `hetu/system`, `hetu/endpoint`, `hetu/server`, `hetu/sourcegen`, tests under `tests/`.
- Architecture patterns: singleton metaclass, factory auto-registration, optimistic transactions with race retry, pub/sub subscription broker.