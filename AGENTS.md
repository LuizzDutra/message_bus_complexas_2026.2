# AGENTS.md

FastAPI + WebSocket prototype that benchmarks **Redis Pub/Sub vs RabbitMQ** as a cross-instance
message bus. Redis Pub/Sub is implemented (`BUS_BACKEND=redis`); default is `BUS_BACKEND=local`
(no external service), and RabbitMQ is not wired yet. `publish()`/`deliver()` in
`app/routers/websocket.py` are the delivery path; `app/bus/` holds the bus.

## Commands

`uv` is the only toolchain; there is no CI, pre-commit, Makefile, or type checker.

```bash
uv sync                                  # install/refresh .venv from uv.lock
uv run fastapi dev app/main.py           # dev server with reload, 127.0.0.1:8000
uv run python -m app.main                # uses HOST/PORT/LOG_LEVEL from .env
BUS_BACKEND=redis uv run python -m app.main   # Redis mode (needs Redis on REDIS_URL)
uv run pytest                            # all tests (fast, ~1s, no external services)
uv run pytest tests/test_api.py::test_broadcast_para_os_demais_usuarios -q   # single test
uv run pytest tests/test_bus.py -q       # bus-only tests (fakeredis)
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
docker compose up --build -d
docker compose ps                          # healthchecks hit /api/health internally
docker compose --profile rabbitmq up -d    # adds RabbitMQ 5672/15672 (NOT used by app)
docker compose down
```

- Compose sets `BUS_BACKEND=redis` and `REDIS_URL=redis://redis:6379/0` for every instance;
  instances wait for the Redis healthcheck.
- The image runs `uv sync --frozen`; rebuild after editing `pyproject.toml` or `uv.lock`.
- Only RabbitMQ uses a profile; Redis always starts with the stack.
- `.dockerignore` excludes everything except `README.md` among markdown files, so `AGENTS.md` never
  reaches the image.
- Compose is validated with `docker compose config`; a Docker daemon is required to actually build.

## Multi-instance manual testing (without Docker)

Simulate instances with separate processes and ports:

```powershell
$env:PORT="8001"; $env:INSTANCE_ID="instance-2"; uv run python -m app.main
uv run python scripts/ws_client.py -u ws://127.0.0.1:8001/ws/message -n 2
```

For cross-instance delivery in this mode, start both processes with `BUS_BACKEND=redis` and a
reachable `REDIS_URL` (default `redis://127.0.0.1:6379/0`); otherwise each one delivers locally.

`INSTANCE_ID` defaults to a random `instance-xxxxxxxx` **per process**; set it explicitly whenever
output must be attributed to an instance. Config is `pydantic-settings` reading `.env` from the
**current working directory** (`.env` is gitignored; template is `.env.example`).

## Architecture that is not obvious from filenames

- `app/ws/manager.py` holds a **single module-level `manager`** shared by all routers — the process-
  global registry of local WebSockets. Do not introduce a second registry; bus implementations must
  reuse it for local fan-out.
- `app/bus/__init__.py` exposes a module-level `bus`: `None` in local mode, `RedisBus` when
  `BUS_BACKEND=redis`. It is created at import from `settings`; tests rely on the local default.
- `publish()` serializes the envelope and either broadcasts locally or publishes to Redis
  (`return 0`). `deliver()` handles Redis messages: it broadcasts the **raw** string (do not
  re-serialize) and excludes `connection_id` from the envelope so the sender never gets an echo —
  each instance receives its own published event back.
- `bus.start(websocket.deliver)` runs in the lifespan; `bus.close()` runs before
  `manager.close_all()`. A Redis outage at startup fails the app (compose retries via restart).
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
  `RedisBus` via **fakeredis** — never require a real Redis in tests. Async scenarios are driven
  with `asyncio.run()` (no pytest-asyncio plugin).
- `fastapi.testclient` works because `fastapi[standard]` pulls in `websockets`/`httpx`; no extra
  dev dependency is needed for `scripts/ws_client.py`.

## Conventions

- Documentation, docstrings, log messages, and test names are **Portuguese (pt-BR)**; code
  identifiers remain English. Keep that split.
- Comments in `.py` files avoid accented characters; user-facing README text uses accents.
- Ruff: line length 100, rules `E,F,I,UP,B,ASYNC` (see `pyproject.toml`).
- Runtime deps go in `[project] dependencies` (via `uv add`) and `uv.lock` is committed.
- `.reasonix/` is gitignored local tooling scratch; never edit or commit it.
