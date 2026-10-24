"""Barramento de eventos (message bus) compartilhado entre instancias.

O modo default e `local`: cada processo entrega apenas para as suas proprias conexoes.
Com `BUS_BACKEND=redis` ou `BUS_BACKEND=rabbitmq` a aplicacao publica os envelopes no
barramento e cada instancia (inclusive a de origem) entrega o que recebe aos seus
clientes locais — o filtro do remetente fica em `app.routers.websocket.deliver`.
"""

from __future__ import annotations

from app.bus.rabbitmq import RabbitMQBus
from app.bus.redis import RedisBus
from app.config import settings

MessageBus = RedisBus | RabbitMQBus


def create_bus() -> MessageBus | None:
    """Cria o barramento conforme `BUS_BACKEND` (None = entrega somente local)."""
    if settings.bus_backend == "redis":
        return RedisBus(settings.redis_url, settings.redis_channel)
    if settings.bus_backend == "rabbitmq":
        queue = f"{settings.rabbitmq_queue_prefix}{settings.instance_id}"
        return RabbitMQBus(settings.rabbitmq_url, settings.rabbitmq_exchange, queue)
    return None


# Instancia unica do barramento neste processo (None no modo local).
bus = create_bus()

__all__ = ["MessageBus", "RabbitMQBus", "RedisBus", "bus", "create_bus"]
