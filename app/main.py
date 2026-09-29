"""Aplicacao FastAPI: prototipo de barramento de eventos para WebSockets.

Execute com:
    uv run fastapi dev app/main.py      (desenvolvimento, com reload)
    uv run python -m app.main           (usando HOST/PORT do .env)
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.routers import health, users, websocket
from app.ws.manager import manager

logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    logger.info("instancia %s iniciando (pid-based)", settings.instance_id)
    try:
        yield
    finally:
        await manager.close_all()
        logger.info("instancia %s encerrada", settings.instance_id)


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        summary="Comparacao RabbitMQ vs Redis Pub/Sub como message bus para WebSockets",
        lifespan=lifespan,
    )
    app.include_router(health.router)
    app.include_router(users.router)
    app.include_router(websocket.router)
    return app


app = create_app()


def main() -> None:
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
