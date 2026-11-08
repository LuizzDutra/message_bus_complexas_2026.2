"""Cliente WebSocket e usuario Locust do teste de carga.

O Locust nao tem cliente WebSocket nativo; aqui o protocolo e falado com
`websocket-client` (Python puro, compativel com o monkey-patching do gevent que o
Locust aplica no import). Cada conexao envia JSON com `client_id`, `seq` e
`sent_at`; ao receber, calcula a latencia e detecta lacunas na sequencia.

Importante: `import locust` aplica `gevent.monkey.patch_all()` no processo, por isso
este modulo nao pode ser importado pelos testes do pytest.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import Counter
from collections.abc import Iterator
from itertools import cycle
from typing import TYPE_CHECKING
from uuid import uuid4

import gevent
import websocket
from locust import User, constant_pacing, events, task

from loadtest.protocol import (
    ReceivedMessage,
    SequenceTracker,
    build_message,
    normalize_ws_url,
    parse_hosts,
    parse_incoming,
)

if TYPE_CHECKING:
    from locust.env import Environment

logger = logging.getLogger("loadtest")

DEFAULT_URL = "ws://127.0.0.1:8000/ws/message"
CONNECT_TIMEOUT = 5.0
READ_TIMEOUT = 0.2
MESSAGE_INTERVAL = max(float(os.environ.get("COMPLEXAS_MSG_INTERVAL", "1.0")), 0.01)
BUS_LATENCY = os.environ.get("COMPLEXAS_BUS_LATENCY", "1").lower() not in {"0", "false", "no"}

COUNTERS: Counter[str] = Counter()
_urls: Iterator[str] | None = None


def _pick_url(host: str | None) -> str:
    """Alterna entre as instancias em round-robin a cada nova conexao."""
    global _urls
    if _urls is None:
        resolved = parse_hosts(os.environ.get("COMPLEXAS_WS_URLS"))
        if not resolved:
            resolved = [normalize_ws_url(host or DEFAULT_URL)]
        _urls = cycle(resolved)
        logger.info("locust alternando entre %d URL(s): %s", len(resolved), ", ".join(resolved))
    return next(_urls)


def _connected_instance(raw: str | bytes | None) -> str | None:
    """Devolve o instance_id do frame `connected`; None para qualquer outro frame."""
    if not isinstance(raw, str):
        return None
    try:
        frame = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(frame, dict) or frame.get("type") != "connected":
        return None
    instance = frame.get("instance_id")
    return instance if isinstance(instance, str) and instance else "?"


class WebSocketClient:
    """Uma conexao WebSocket com leitura em greenlet proprio."""

    def __init__(self, url: str, environment: Environment) -> None:
        self.url = url
        self.environment = environment
        self.client_id = uuid4().hex
        self.seq = 0
        self.closed = False
        self._socket: websocket.WebSocket | None = None
        self._reader: gevent.Greenlet | None = None
        self._tracker = SequenceTracker()

    def connect(self) -> None:
        started = time.perf_counter()
        self._socket = websocket.create_connection(self.url, timeout=CONNECT_TIMEOUT)
        self._socket.settimeout(READ_TIMEOUT)
        self._fire("conexao", (time.perf_counter() - started) * 1000, 0)
        self._reader = gevent.spawn(self._read_loop)

    def send(self) -> None:
        if self.closed or self._socket is None:
            return
        self.seq += 1
        payload = build_message(self.client_id, self.seq)
        started = time.perf_counter()
        try:
            self._socket.send(payload)
        except (websocket.WebSocketException, OSError):
            self.closed = True
            COUNTERS["falhas_envio"] += 1
            return
        COUNTERS["enviadas"] += 1
        self._fire("envio", (time.perf_counter() - started) * 1000, len(payload))

    def close(self) -> None:
        self.closed = True
        if self._socket is not None:
            try:
                self._socket.close()
            except (websocket.WebSocketException, OSError):
                pass
            self._socket = None

    def _read_loop(self) -> None:
        assert self._socket is not None
        while not self.closed:
            try:
                raw = self._socket.recv()
            except websocket.WebSocketTimeoutException:
                continue
            except (websocket.WebSocketConnectionClosedException, OSError):
                break
            if not raw:
                break
            instance = _connected_instance(raw)
            if instance is not None:
                COUNTERS["conexoes"] += 1
                COUNTERS[f"conexao:{instance}"] += 1
                continue
            message = parse_incoming(raw)
            if message is not None:
                self._on_message(message, len(raw))
        self.closed = True

    def _on_message(self, message: ReceivedMessage, size: int) -> None:
        now = time.time()
        gap = self._tracker.observe(message.sender_id, message.seq)
        COUNTERS["recebidas"] += 1
        COUNTERS["perdidas"] += gap.missing
        COUNTERS["repetidas"] += gap.repeats
        self._fire("latencia_ponta_a_ponta", max((now - message.sent_at) * 1000, 0.0), size)
        if BUS_LATENCY:
            self._fire(
                "latencia_barramento",
                max((now - message.server_timestamp) * 1000, 0.0),
                size,
            )

    def _fire(self, name: str, response_time: float, size: int) -> None:
        self.environment.events.request.fire(
            request_type="WS",
            name=name,
            response_time=response_time,
            response_length=size,
        )


class ComplexasUser(User):
    """Cada usuario Locust mantem uma conexao e envia uma mensagem por intervalo."""

    wait_time = constant_pacing(MESSAGE_INTERVAL)

    def on_start(self) -> None:
        self.ws = WebSocketClient(_pick_url(self.environment.host), self.environment)
        try:
            self.ws.connect()
        except Exception as exc:
            COUNTERS["falhas_conexao"] += 1
            self.environment.events.request.fire(
                request_type="WS",
                name="conexao",
                response_time=0.0,
                response_length=0,
                exception=exc,
            )
            raise

    @task
    def enviar(self) -> None:
        self.ws.send()

    def on_stop(self) -> None:
        self.ws.close()


@events.quitting.add_listener
def _imprimir_resumo(environment: Environment, **_kwargs) -> None:
    """Imprime perda/repeticoes, que o Locust nao agrega sozinho."""
    recebidas = COUNTERS["recebidas"]
    perdidas = COUNTERS["perdidas"]
    esperadas = recebidas + perdidas
    taxa = (100.0 * perdidas / esperadas) if esperadas else 0.0
    linhas = [
        "=== Resumo do teste de carga (contadores deste processo) ===",
        f"conexoes criadas: {COUNTERS['conexoes']}",
        f"mensagens enviadas: {COUNTERS['enviadas']}",
        f"mensagens recebidas: {recebidas}",
        f"mensagens perdidas por lacuna: {perdidas} ({taxa:.3f}%)",
        f"mensagens repetidas/fora de ordem: {COUNTERS['repetidas']}",
        f"falhas de conexao: {COUNTERS['falhas_conexao']}",
        f"falhas de envio: {COUNTERS['falhas_envio']}",
    ]
    por_instancia = sorted(chave for chave in COUNTERS if chave.startswith("conexao:"))
    if por_instancia:
        detalhe = ", ".join(f"{c.split(':', 1)[1]}={COUNTERS[c]}" for c in por_instancia)
        linhas.append(f"conexoes por instancia: {detalhe}")
    if recebidas == 0 and COUNTERS["enviadas"] > 0:
        linhas.append(
            "AVISO: nada foi recebido; o remetente nao recebe o proprio evento. "
            "Use ao menos 2 usuarios (-u 2)."
        )
    texto = "\n".join(linhas)
    print(texto, flush=True)
