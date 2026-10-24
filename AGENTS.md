# AGENTS.md

FastAPI + WebSocket prototype that benchmarks **Redis Pub/Sub vs RabbitMQ** as a cross-instance
message bus. Both buses are implemented (`BUS_BACKEND=redis` / `BUS_BACKEND=rabbitmq`); default is
`BUS_BACKEND=local` (no external service). `publish()`/`deliver()` in `app/routers/websocket.py` are
the delivery path (shared by both); `app/bus/` holds `RedisBus` and `RabbitMQBus`.

## Commands

`uv` is the only toolchain; there is no CI, pre-commit, Makefile, or type checker.

```bash
uv sync                                  # install/refresh .venv from uv.lock
uv run fastapi dev app/main.py           # dev server with reload, 127.0.0.1:8000
uv run python -m app.main                # uses HOST/PORT/LOG_LEVEL from .env
BUS_BACKEND=redis uv run python -m app.main   # Redis mode (needs Redis on REDIS_URL)
BUS_BACKEND=rabbitmq uv run python -m app.main # RabbitMQ mode (needs broker on RABBITMQ_URL)
uv run pytest                            # all tests (fast, ~1s, no external services)
uv run pytest tests/test_api.py::test_broadcast_para_os_demais_usuarios -q   # single test
uv run pytest tests/test_bus.py -q       # Redis bus tests (fakeredis)
uv run pytest tests/test_rabbitmq_bus.py -q   # RabbitMQ bus tests (AMQP fakes)
uv run ruff check . && uv run ruff format --check .   # lint + format gate
uv run ruff format .                     # apply formatting
```

- Do not invoke `.venv/Scripts/python` or `pip` directly; `uv run` keeps the lock in sync.
- Verification order: `pytest` → `ruff check` → `ruff format --check`.
- Python is pinned to 3.11 (`.python-version`, `target-version` in `pyproject.toml`).

## Docker

`compose.yaml` builds one image (`complexas:local`, from `Dockerfile`) and runs it as
`instance-1/2/3` on host ports 8000/8001/8002; the app always listens on 8000 inside the container.

```bash
docker compose up --build -d                       # Redis variant (default)
BUS_BACKEND=rabbitmq docker compose --profile rabbitmq up --build -d   # RabbitMQ variant
docker compose ps                                  # healthchecks hit /api/health internally
docker compose down
```

- Backend comes from interpolation `BUS_BACKEND: ${BUS_BACKEND:-redis}` in `x-env`, so the env var
  must be set in the shell running compose; instances get `REDIS_URL` and `RABBITMQ_URL` regardless.
- `depends_on` only waits for Redis health: a profiled service cannot be a dependency (compose
  errors with "depends on undefined service"), so RabbitMQ-variant instances may restart a few
  times until the broker is ready.
- Only RabbitMQ uses a profile (`rabbitmq`); Redis always starts with the stack.
- The image runs `uv sync --frozen`; rebuild after editing `pyproject.toml` or `uv.lock`.
- `.dockerignore` excludes everything except `README.md` among markdown files, so `AGENTS.md` never
  reaches the image.
- Compose is validated with `docker compose config`; a Docker daemon is required to actually build.

## Multi-instance manual testing (without Docker)

Simulate instances with separate processes and ports:

```powershell
$env:PORT="8001"; $env:INSTANCE_ID="instance-2"; uv run python -m app.main
uv run python scripts/ws_client.py -u ws://127.0.0.1:8001/ws/message -n 2
```

For cross-instance delivery in this mode, start both processes with `BUS_BACKEND=redis` (or
`rabbitmq`) and a reachable `REDIS_URL`/`RABBITMQ_URL`; otherwise each one delivers locally.
Queue names for RabbitMQ are `RABBITMQ_QUEUE_PREFIX + INSTANCE_ID`, so random ids leave orphan
durable queues behind — prefer explicit `INSTANCE_ID`s when testing by hand.

`INSTANCE_ID` defaults to a random `instance-xxxxxxxx` **per process**; set it explicitly whenever
output must be attributed to an instance. Config is `pydantic-settings` reading `.env` from the
**current working directory** (`.env` is gitignored; template is `.env.example`).

## Architecture that is not obvious from filenames

- `app/ws/manager.py` holds a **single module-level `manager`** shared by all routers — the process-
  global registry of local WebSockets. Do not introduce a second registry; bus implementations must
  reuse it for local fan-out.
- `app/bus/__init__.py` exposes a module-level `bus`: `None` in local mode, `RedisBus` or
  `RabbitMQBus` per `BUS_BACKEND`. It is created at import from `settings`; tests rely on the local
  default and monkeypatch `websocket.bus`/`settings` to exercise the other modes.
- `publish()` serializes the envelope and either broadcasts locally or publishes to the bus
  (`return 0`). `deliver()` handles bus messages (Redis **and** RabbitMQ): it broadcasts the **raw**
  string (do not re-serialize) and excludes `connection_id` from the envelope so the sender never
  gets an echo — each instance receives its own published event back.
- The WS handler wraps `await publish(...)` in `try/except Exception`: a broker outage **drops the
  event with a warning** and never kills the user's WebSocket (a raise used to close the connection
  without a close frame and crash `ws_client.py`). Regression: `test_conexao_ws_nao_cai_quando_o_barramento_falha`.
- `bus.start(websocket.deliver)` runs in the lifespan; `bus.close()` runs before
  `manager.close_all()`. A broker outage at startup fails the app (compose restarts the container).
- `RabbitMQBus` details: `await connection.channel(...)` is mandatory — the returned channel is only
  usable after `__await__` (without it `set_qos` hangs forever and startup never completes); topology
  is a durable fanout exchange + durable queue `complexas.<INSTANCE_ID>` bound to it; consumer runs
  with `prefetch_count=1` and manual acks — `ack` after local delivery, `nack(requeue=True)` on
  failure (at-least-once, may duplicate locally). aio-pika's robust connection restores channel,
  queue and consumer after a broker restart.
- `RedisBus.publish()` retries once after `connection_pool.disconnect()` because pooled connections
  go stale when Redis restarts. The subscriber loop resubscribes with a 1s delay; messages published
  during that window are lost by design (Pub/Sub has no replay).
- Preserve these protocol rules: sender never receives its own message; JSON frames become
  `payload` verbatim, plain text becomes a string payload; binary frames are ignored; envelope
  fields `type`, `connection_id`, `instance_id`, `server_timestamp` are asserted by tests.
- `connection_id` is generated per connection (`uuid4().hex`), not per user.
- New routers must be registered in `create_app()` in `app/main.py`; shutdown cleanup lives in the
  `lifespan` handler (`manager.close_all()`).

## Testing gotchas

- Always use `with TestClient(app)` (the fixture does) so the lifespan startup/shutdown runs.
- The manager is global across tests and the fixture asserts `manager.count == 0` on teardown:
  close WebSocket contexts before a test ends. Do not run these tests in parallel.
- `tests/test_bus.py` covers local publish, Redis routing, `deliver` echo suppression, and
  `RedisBus` via **fakeredis**; `tests/test_rabbitmq_bus.py` covers topology, acks and retry with
  injected AMQP fakes — never require a real broker in tests. Async scenarios are driven with
  `asyncio.run()` (no pytest-asyncio plugin).
- `FakePendingChannel` in the RabbitMQ tests only resolves after `await connection.channel()`: it
  exists to keep the "forgot to await the channel" startup hang from regressing.
- `tests/conftest.py` forces `BUS_BACKEND=local` and a fixed `INSTANCE_ID` **before** importing
  `app`: `pydantic-settings` reads `.env` from the CWD, so a developer `.env` with
  `BUS_BACKEND=redis|rabbitmq` (or an empty `INSTANCE_ID`) would make the lifespan connect to a
  real broker and every test error at setup. Keep hermeticity there, not in each test.
- `fastapi.testclient` works because `fastapi[standard]` pulls in `websockets`/`httpx`; no extra
  dev dependency is needed for `scripts/ws_client.py`.

## Conventions

- Documentation, docstrings, log messages, and test names are **Portuguese (pt-BR)**; code
  identifiers remain English. Keep that split.
- Comments in `.py` files avoid accented characters; user-facing README text uses accents.
- Ruff: line length 100, rules `E,F,I,UP,B,ASYNC` (see `pyproject.toml`).
- Runtime deps go in `[project] dependencies` (via `uv add`) and `uv.lock` is committed.
- `.reasonix/` is gitignored local tooling scratch; never edit or commit it.
