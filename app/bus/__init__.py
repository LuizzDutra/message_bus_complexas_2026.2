"""Barramento de eventos (message bus) compartilhado entre instancias.

O modo default e `local`: cada processo entrega apenas para as suas proprias conexoes.
Com `BUS_BACKEND=redis` a aplicacao publica os envelopes no Redis Pub/Sub e cada
instancia (inclusive a de origem) entrega o que recebe aos seus clientes locais.
"""

from __future__ import annotations

from app.bus.redis import RedisBus
from app.config import settings


def create_bus() -> RedisBus | None:
    """Cria o barramento conforme `BUS_BACKEND` (None = entrega somente local)."""
    if settings.bus_backend == "redis":
        return RedisBus(settings.redis_url, settings.redis_channel)
    return None


# Instancia unica do barramento neste processo (None no modo local).
bus = create_bus()

__all__ = ["RedisBus", "bus", "create_bus"]
