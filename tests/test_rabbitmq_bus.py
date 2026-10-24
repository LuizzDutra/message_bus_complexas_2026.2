"""Testes do barramento RabbitMQ: topologia, acks e roteamento (sem broker)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from aio_pika.exceptions import AMQPError

from app.bus import RabbitMQBus, create_bus
from app.config import settings


class FakeMessage:
    """Mensagem recebida do broker (so o que `RabbitMQBus._on_message` usa)."""

    def __init__(self, body: bytes) -> None:
        self.body = body
        self.acks = 0
        self.requeues = 0

    async def ack(self) -> None:
        self.acks += 1

    async def nack(self, *, requeue: bool = False) -> None:
        self.requeues += int(requeue)


class FakeQueue:
    def __init__(self, name: str, durable: bool) -> None:
        self.name = name
        self.durable = durable
        self.bound_to: list[str] = []
        self.callback: Any = None
        self.no_ack: bool | None = None
        self.consumer_tag: str | None = None
        self.cancelled: str | None = None

    async def bind(self, exchange: Any, routing_key: str | None = None) -> None:
        self.bound_to.append(exchange.name)

    async def consume(self, callback: Any, no_ack: bool = False) -> str:
        self.callback = callback
        self.no_ack = no_ack
        self.consumer_tag = f"ctag-{self.name}"
        return self.consumer_tag

    async def cancel(self, consumer_tag: str) -> None:
        self.cancelled = consumer_tag


class FakeExchange:
    def __init__(self, name: str, type: Any, durable: bool) -> None:
        self.name = name
        self.type = type
        self.durable = durable
        self.published: list[bytes] = []
        self.failures = 0

    async def publish(self, message: Any, routing_key: str = "") -> None:
        if self.failures:
            self.failures -= 1
            raise AMQPError("canal caiu")
        self.published.append(message.body)


class FakeChannel:
    def __init__(self) -> None:
        self.prefetch: int | None = None
        self.exchanges: dict[str, FakeExchange] = {}
        self.queues: dict[str, FakeQueue] = {}
        self.closed = False

    async def set_qos(self, prefetch_count: int = 0) -> None:
        self.prefetch = prefetch_count

    async def declare_exchange(self, name: str, type: Any, durable: bool = False) -> FakeExchange:
        return self.exchanges.setdefault(name, FakeExchange(name, type, durable))

    async def declare_queue(self, name: str, durable: bool = False) -> FakeQueue:
        return self.queues.setdefault(name, FakeQueue(name, durable))

    async def close(self) -> None:
        self.closed = True


class FakePendingChannel:
    """So e utilizavel apos `await connection.channel()` (como no aio-pika real)."""

    def __init__(self, channel: FakeChannel) -> None:
        self._channel = channel

    def __await__(self) -> Any:
        async def _initialize() -> FakeChannel:
            return self._channel

        return _initialize().__await__()


class FakeConnection:
    def __init__(self) -> None:
        self.channel_obj = FakeChannel()
        self.closed = False

    def channel(self, publisher_confirms: bool = True) -> FakePendingChannel:
        return FakePendingChannel(self.channel_obj)

    async def close(self) -> None:
        self.closed = True


def _bus() -> tuple[RabbitMQBus, FakeConnection]:
    connection = FakeConnection()
    bus = RabbitMQBus(
        "amqp://localhost:5672/", "complexas", "complexas.instance-1", connection=connection
    )
    return bus, connection


def test_topologia_durable_e_entrega_com_ack() -> None:
    bus, connection = _bus()
    received: list[str] = []

    async def handler(message: str) -> None:
        received.append(message)

    asyncio.run(bus.start(handler))
    channel = connection.channel_obj
    exchange = channel.exchanges["complexas"]
    queue = channel.queues["complexas.instance-1"]

    assert exchange.type == "fanout"
    assert exchange.durable is True
    assert queue.durable is True
    assert queue.bound_to == ["complexas"]
    assert channel.prefetch == 1
    assert queue.no_ack is False

    asyncio.run(bus.publish('{"payload":"oi"}'))
    assert exchange.published == [b'{"payload":"oi"}']

    # entrega da fila para o handler local, com ack manual
    incoming = FakeMessage(b'{"payload":"oi"}')
    asyncio.run(queue.callback(incoming))
    assert received == ['{"payload":"oi"}']
    assert (incoming.acks, incoming.requeues) == (1, 0)


def test_nack_requeue_quando_handler_falha() -> None:
    bus, connection = _bus()

    async def handler(message: str) -> None:
        raise RuntimeError("broadcast falhou")

    asyncio.run(bus.start(handler))
    incoming = FakeMessage(b"qualquer")
    asyncio.run(connection.channel_obj.queues["complexas.instance-1"].callback(incoming))

    assert (incoming.acks, incoming.requeues) == (0, 1)


def test_publish_sem_start_levanta_erro() -> None:
    bus, _ = _bus()
    with pytest.raises(RuntimeError, match="nao iniciado"):
        asyncio.run(bus.publish("x"))


def test_publish_repete_apois_falha_de_conexao() -> None:
    bus, connection = _bus()

    async def handler(message: str) -> None:
        pass

    asyncio.run(bus.start(handler))
    exchange = connection.channel_obj.exchanges["complexas"]
    exchange.failures = 1

    asyncio.run(bus.publish("retry"))

    assert exchange.published == [b"retry"]


def test_close_cancela_consumidor_e_fecha_conexao() -> None:
    bus, connection = _bus()

    async def handler(message: str) -> None:
        pass

    asyncio.run(bus.start(handler))
    queue = connection.channel_obj.queues["complexas.instance-1"]
    asyncio.run(bus.close())

    assert queue.cancelled == "ctag-complexas.instance-1"
    assert connection.channel_obj.closed is True
    assert connection.closed is True


def test_create_bus_usa_rabbitmq_quando_configurado(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "bus_backend", "rabbitmq")
    created = create_bus()
    assert isinstance(created, RabbitMQBus)
    assert created._queue_name == f"{settings.rabbitmq_queue_prefix}{settings.instance_id}"


def test_create_bus_none_no_modo_local(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "bus_backend", "local")
    assert create_bus() is None
