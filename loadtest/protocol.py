"""Funcoes puras do teste de carga: protocolo, URLs e deteccao de lacunas."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

WS_PATH = "/ws/message"
_WS_SCHEMES = {"ws", "wss"}
_SCHEME_MAP = {"http": "ws", "https": "wss"}


@dataclass(frozen=True, slots=True)
class ReceivedMessage:
    """Mensagem de outro cliente, extraida do envelope do servidor."""

    sender_id: str
    seq: int
    sent_at: float
    server_timestamp: float


@dataclass(frozen=True, slots=True)
class Gap:
    """Resultado de uma observacao de sequencia: lacunas ou repeticoes."""

    missing: int = 0
    repeats: int = 0


def build_message(client_id: str, seq: int, *, sent_at: float | None = None) -> str:
    """Monta o frame JSON enviado ao servidor (vira o campo `payload` do envelope)."""
    payload = {
        "client_id": client_id,
        "seq": seq,
        "sent_at": time.time() if sent_at is None else sent_at,
    }
    return json.dumps(payload, separators=(",", ":"))


def parse_incoming(raw: str) -> ReceivedMessage | None:
    """Extrai a mensagem de um envelope; devolve None para qualquer outro frame."""
    try:
        envelope = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(envelope, dict) or envelope.get("type") != "message":
        return None
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return None
    try:
        return ReceivedMessage(
            sender_id=str(payload["client_id"]),
            seq=int(payload["seq"]),
            sent_at=float(payload["sent_at"]),
            server_timestamp=float(envelope["server_timestamp"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


class SequenceTracker:
    """Detecta lacunas e repeticoes por remetente, ancorado no primeiro seq visto.

    A primeira mensagem de cada remetente vira o baseline: quem entra no meio do
    teste nao pode contar como perdido o que passou antes de conectar.
    """

    def __init__(self) -> None:
        self._last: dict[str, int] = {}

    def observe(self, sender_id: str, seq: int) -> Gap:
        last = self._last.get(sender_id)
        if last is None:
            self._last[sender_id] = seq
            return Gap()
        if seq == last + 1:
            self._last[sender_id] = seq
            return Gap()
        if seq > last:
            self._last[sender_id] = seq
            return Gap(missing=seq - last - 1)
        return Gap(repeats=1)


def normalize_ws_url(value: str) -> str:
    """Aceita `host:porta`, `http(s)://...` ou `ws(s)://...` e devolve a URL do WS."""
    raw = value.strip()
    if not raw:
        raise ValueError("URL vazia")
    if "://" not in raw:
        raw = f"ws://{raw}"
    parts = urlsplit(raw)
    scheme = _SCHEME_MAP.get(parts.scheme, parts.scheme)
    if scheme not in _WS_SCHEMES:
        raise ValueError(f"esquema nao suportado para WebSocket: {parts.scheme!r}")
    if not parts.netloc:
        raise ValueError(f"URL sem host: {value!r}")
    path = parts.path if parts.path not in {"", "/"} else WS_PATH
    return urlunsplit((scheme, parts.netloc, path, parts.query, parts.fragment))


def parse_hosts(value: str | None) -> list[str]:
    """Converte `COMPLEXAS_WS_URLS` (lista separada por virgula) em URLs normalizadas."""
    if not value:
        return []
    parts = value.replace("\n", ",").split(",")
    return [normalize_ws_url(part) for part in parts if part.strip()]
