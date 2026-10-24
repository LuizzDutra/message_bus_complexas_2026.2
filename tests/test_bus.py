"""Testes do barramento: modo local, roteamento para o Redis e entrega sem eco."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import fakeredis.aioredis
import pytest

from app.bus.redis import RedisBus
from app.routers import websocket
from app.ws.manager import manager


class FakeWebSocket:
    """WebSocket minimo para exercitar o manager sem subir a aplicacao."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def accept(self) -> None:
        pass

    async def send_text(self, message: str) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        pass


def _envelope(connection_id: str, payload: Any = "oi") -> dict[str, Any]:
    return {
        "type": "message",
        "connection_id": connection_id,
        "instance_id": "instance-test",
        "server_timestamp": 1.0,
        "payload": payload,
    }


def test_publish_local_entrega_sem_eco(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(websocket, "bus", None)

    async def scenario() -> None:
        alice_ws, bob_ws = FakeWebSocket(), FakeWebSocket()
        alice = await manager.connect(alice_ws)
        await manager.connect(bob_ws)
        try:
            envelope = _envelope(alice.id)
            delivered = await websocket.publish(envelope, sender_id=alice.id)
            assert delivered == 1
            assert bob_ws.sent == [json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))]
            assert alice_ws.sent == []
        finally:
            await manager.close_all()

    asyncio.run(scenario())


def test_publish_usa_barramento_quando_configurado(monkeypatch: pytest.MonkeyPatch) -> None:
    published: list[str] = []

    class RecordingBus:
        async def publish(self, message: str) -> None:
            published.append(message)

    monkeypatch.setattr(websocket, "bus", RecordingBus())

    envelope = _envelope("alice")
    delivered = asyncio.run(websocket.publish(envelope, sender_id="alice"))

    assert delivered == 0
    assert [json.loads(message) for message in published] == [envelope]


def test_deliver_encaminha_mensagem_do_barramento_sem_eco() -> None:
    async def scenario() -> None:
        alice_ws, bob_ws = FakeWebSocket(), FakeWebSocket()
        alice = await manager.connect(alice_ws)
        await manager.connect(bob_ws)
        try:
            message = json.dumps(_envelope(alice.id))
            delivered = await websocket.deliver(message)
            assert delivered == 1
            assert bob_ws.sent == [message]
            assert alice_ws.sent == []
        finally:
            await manager.close_all()

    asyncio.run(scenario())


def test_deliver_ignora_envelope_invalido() -> None:
    assert asyncio.run(websocket.deliver("nao e json")) == 0


def test_redis_bus_publica_e_recebe() -> None:
    async def scenario() -> None:
        server = fakeredis.FakeServer()
        client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
        received: list[str] = []
        first_message = asyncio.Event()

        async def handler(message: str) -> None:
            received.append(message)
            first_message.set()

        redis_bus = RedisBus("redis://localhost:6379/0", "teste", client=client)
        await redis_bus.start(handler)
        try:
            await redis_bus.publish('{"type":"message","connection_id":"alice"}')
            await asyncio.wait_for(first_message.wait(), timeout=2)
        finally:
            await redis_bus.close()

        assert received == ['{"type":"message","connection_id":"alice"}']

    asyncio.run(scenario())
