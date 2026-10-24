"""Testes de fumaca dos endpoints HTTP e do WebSocket de broadcast."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import websocket
from app.ws.manager import manager


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client
    assert manager.count == 0, "o manager deve ficar vazio ao final de cada teste"


def test_health(client: TestClient) -> None:
    response = client.get("/api/health")
    assert response.status_code == 200

    body = response.json()
    assert body["status"] == "ok"
    assert body["instance_id"]
    assert body["bus_backend"] == "local"
    assert body["uptime_seconds"] >= 0


def test_count_users_sem_conexoes(client: TestClient) -> None:
    response = client.get("/api/count/users")
    assert response.status_code == 200
    assert response.json()["connected_users"] == 0


def test_count_users_com_duas_conexoes(client: TestClient) -> None:
    with (
        client.websocket_connect("/ws/message") as first,
        client.websocket_connect("/ws/message") as second,
    ):
        first.receive_json()
        second.receive_json()

        assert client.get("/api/count/users").json()["connected_users"] == 2

    assert client.get("/api/count/users").json()["connected_users"] == 0


def test_broadcast_para_os_demais_usuarios(client: TestClient) -> None:
    with client.websocket_connect("/ws/message") as alice:
        alice_hello = alice.receive_json()
        assert alice_hello["type"] == "connected"
        assert alice_hello["instance_id"]

        with client.websocket_connect("/ws/message") as bob:
            bob_hello = bob.receive_json()
            assert bob_hello["connection_id"] != alice_hello["connection_id"]
            assert bob_hello["instance_id"] == alice_hello["instance_id"]
            assert bob_hello["connected_users"] == 2

            # JSON: o valor decodificado vira o payload do envelope
            alice.send_text(json.dumps({"sala": "geral", "texto": "ola"}))
            received = bob.receive_json()
            assert received["type"] == "message"
            assert received["connection_id"] == alice_hello["connection_id"]
            assert received["instance_id"] == alice_hello["instance_id"]
            assert received["server_timestamp"] > 0
            assert received["payload"] == {"sala": "geral", "texto": "ola"}

            # texto puro: o proprio texto vira o payload
            bob.send_text("ping")
            echo = alice.receive_json()
            assert echo["connection_id"] == bob_hello["connection_id"]
            assert echo["payload"] == "ping"

            # o remetente nao recebe a propria mensagem: alice so leu a de bob
            assert alice_hello["connection_id"] != echo["connection_id"]


def test_conexao_ws_nao_cai_quando_o_barramento_falha(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Uma falha de publicacao nao pode derrubar a conexao WebSocket do cliente.

    Regressao: reiniciar o broker (Redis/RabbitMQ) fazia o publish levantar excecao
    dentro do endpoint, o WebSocket era fechado sem close frame e o cliente morria.
    """
    calls: list[dict[str, Any]] = []

    async def publish_quebrado(envelope: dict[str, Any], *, sender_id: str) -> int:
        calls.append(envelope)
        raise RuntimeError("barramento fora do ar")

    monkeypatch.setattr(websocket, "publish", publish_quebrado)

    with client.websocket_connect("/ws/message") as alice:
        hello = alice.receive_json()
        assert hello["type"] == "connected"

        alice.send_text("primeiro")
        alice.send_text("segundo")

        # publish roda no portal do TestClient (outra thread): aguarda as duas chamadas
        deadline = time.monotonic() + 5
        while len(calls) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)

        assert len(calls) == 2, "as duas mensagens devem chegar ao publish"
        assert calls[0]["payload"] == "primeiro"
        assert calls[1]["payload"] == "segundo"

        # a conexao continuou viva: a segunda mensagem foi processada normalmente
        assert client.get("/api/count/users").json()["connected_users"] == 1
