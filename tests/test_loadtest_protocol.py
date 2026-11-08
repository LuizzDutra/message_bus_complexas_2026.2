"""Testes das funcoes puras do teste de carga (sem importar Locust/gevent)."""

from __future__ import annotations

import json
from collections import Counter

import pytest

from loadtest.protocol import (
    Gap,
    ReceivedMessage,
    SequenceTracker,
    build_message,
    normalize_ws_url,
    parse_hosts,
    parse_incoming,
    rotate_urls,
)


def test_build_message_produz_json_compacto() -> None:
    assert json.loads(build_message("abc", 7, sent_at=100.5)) == {
        "client_id": "abc",
        "seq": 7,
        "sent_at": 100.5,
    }


def test_parse_incoming_extrai_a_mensagem_do_envelope() -> None:
    envelope = {
        "type": "message",
        "connection_id": "srv-1",
        "instance_id": "instance-1",
        "server_timestamp": 101.0,
        "payload": {"client_id": "abc", "seq": 7, "sent_at": 100.5},
    }

    assert parse_incoming(json.dumps(envelope)) == ReceivedMessage(
        sender_id="abc", seq=7, sent_at=100.5, server_timestamp=101.0
    )


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "ping",
        '{"type": "connected", "connection_id": "x"}',
        '{"type": "message", "payload": "texto puro", "server_timestamp": 1.0}',
        '{"type": "message", "payload": {"seq": 1}, "server_timestamp": 1.0}',
        "nao e json",
    ],
)
def test_parse_incoming_ignora_frames_que_nao_sao_mensagem(raw: str) -> None:
    assert parse_incoming(raw) is None


def test_sequence_tracker_ignora_a_primeira_mensagem_de_cada_remetente() -> None:
    tracker = SequenceTracker()

    assert tracker.observe("a", 10_000) == Gap()
    assert tracker.observe("b", 3) == Gap()


def test_sequence_tracker_conta_lacunas() -> None:
    tracker = SequenceTracker()
    tracker.observe("a", 10)

    assert tracker.observe("a", 11) == Gap()
    assert tracker.observe("a", 14) == Gap(missing=2)
    assert tracker.observe("a", 15) == Gap()


def test_sequence_tracker_conta_repeticoes_sem_avancar_o_ultimo_seq() -> None:
    tracker = SequenceTracker()
    tracker.observe("a", 3)

    assert tracker.observe("a", 3) == Gap(repeats=1)
    assert tracker.observe("a", 2) == Gap(repeats=1)
    assert tracker.observe("a", 4) == Gap()


@pytest.mark.parametrize(
    ("entrada", "esperado"),
    [
        ("127.0.0.1:8000", "ws://127.0.0.1:8000/ws/message"),
        ("http://127.0.0.1:8001", "ws://127.0.0.1:8001/ws/message"),
        ("https://exemplo.test", "wss://exemplo.test/ws/message"),
        ("ws://127.0.0.1:8002/ws/message", "ws://127.0.0.1:8002/ws/message"),
        ("ws://127.0.0.1:8002/outro", "ws://127.0.0.1:8002/outro"),
    ],
)
def test_normalize_ws_url(entrada: str, esperado: str) -> None:
    assert normalize_ws_url(entrada) == esperado


def test_normalize_ws_url_rejeita_esquema_invalido() -> None:
    with pytest.raises(ValueError):
        normalize_ws_url("ftp://127.0.0.1:21")


def test_parse_hosts_aceita_lista_separada_por_virgula() -> None:
    assert parse_hosts("http://a:8000, ws://b:8001/ws/message") == [
        "ws://a:8000/ws/message",
        "ws://b:8001/ws/message",
    ]


def test_parse_hosts_vazio_retorna_lista_vazia() -> None:
    assert parse_hosts("") == []
    assert parse_hosts(None) == []


def test_rotate_urls_distribui_o_inicio_por_offset() -> None:
    urls = ["a", "b", "c"]

    assert rotate_urls(urls, 0) == ["a", "b", "c"]
    assert rotate_urls(urls, 1) == ["b", "c", "a"]
    assert rotate_urls(urls, 4) == ["b", "c", "a"]
    assert rotate_urls(urls, -1) == ["c", "a", "b"]
    assert rotate_urls([], 3) == []


def test_rotate_urls_equilibra_usuarios_entre_instancias() -> None:
    urls = [f"instance-{n}" for n in range(6)]
    totals: Counter[str] = Counter()

    for worker in range(12):
        ordered = rotate_urls(urls, worker)
        for user in range(10):
            totals[ordered[user % len(ordered)]] += 1

    assert set(totals.values()) == {20}
