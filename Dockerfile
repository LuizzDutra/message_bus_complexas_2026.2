# Imagem base ja traz Python 3.11 + uv (o mesmo toolchain usado fora do container).
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# 1) dependencias primeiro (melhor cache: so invalida quando pyproject/uv.lock mudam)
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# 2) codigo da aplicacao
COPY README.md ./
COPY app ./app
COPY scripts ./scripts
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# HOST/PORT/INSTANCE_ID vem do ambiente (compose) ou dos defaults de app/config.py.
CMD ["python", "-m", "app.main"]
