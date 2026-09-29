"""Testes de fumaca dos endpoints HTTP e do WebSocket de broadcast."""

from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from app.main import app
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
