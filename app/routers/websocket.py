"""WS /ws/message - endpoint que recebe a carga e propaga para os demais usuarios.

Protocolo
---------
Cliente -> servidor: qualquer frame de texto. Se for JSON, o valor decodificado
vira o campo `payload` do envelope; caso contrario o proprio texto vira o payload.
Servidor -> demais clientes: um envelope JSON

    {"type": "message", "connection_id": ..., "instance_id": ...,
     "server_timestamp": ..., "payload": <o que o cliente enviou>}

No handshake o cliente recebe um aviso de boas-vindas

    {"type": "connected", "connection_id": ..., "instance_id": ...,
     "connected_users": ..., "server_timestamp": ...}

para saber sua origem e em qual instancia caiu (necessario nos testes
comparativos entre instancias).

O ponto de extensao do trabalho e a funcao `publish`: com `BUS_BACKEND=redis` ela
publica o envelope no Redis e cada instancia entrega aos seus proprios clientes ao
receber o evento de volta (`deliver`); no modo `local` (default) ela entrega apenas
para as conexoes deste processo.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from fastapi import APIRouter, WebSocket

from app.bus import bus
from app.config import settings
from app.ws.manager import Connection, manager

router = APIRouter(tags=["websocket"])
logger = logging.getLogger(__name__)


@router.websocket("/ws/message")
async def websocket_message(websocket: WebSocket) -> None:
    connection = await manager.connect(websocket)
    await manager.send(connection.id, _dumps(_connected_event(connection)))
    logger.info("cliente %s conectado em %s", connection.id, settings.instance_id)

    try:
        while True:
            frame = await websocket.receive()
            if frame["type"] == "websocket.disconnect":
                break
            raw = frame.get("text")
            if raw is None:
                logger.debug("frame binario ignorado em %s", connection.id)
                continue
            try:
                await publish(_build_envelope(connection, raw), sender_id=connection.id)
            except Exception as exc:  # barramento fora do ar: nao derrubar a conexao WS
                logger.warning(
                    "falha ao publicar mensagem de %s; evento descartado (%s)",
                    connection.id,
                    exc,
                )
    finally:
        await manager.disconnect(connection.id)
        logger.info("cliente %s desconectado de %s", connection.id, settings.instance_id)


async def publish(envelope: dict[str, Any], *, sender_id: str) -> int:
    """Entrega um evento aos clientes conectados.

    Sem barramento (`BUS_BACKEND=local`) entrega somente as conexoes locais.
    Com Redis, publica no canal e retorna 0: a entrega acontece de forma assincrona
    em `deliver`, quando o evento volta pelo subscribe de cada instancia.
    """
    message = _dumps(envelope)
    if bus is None:
        return await manager.broadcast(message, exclude=(sender_id,))
    await bus.publish(message)
    return 0


async def deliver(message: str) -> int:
    """Entrega um envelope recebido do barramento aos clientes locais.

    Cada instancia recebe o proprio evento de volta; o remetente original e
    filtrado pelo `connection_id` do envelope, evitando eco e entrega duplicada.
    """
    try:
        envelope = json.loads(message)
    except json.JSONDecodeError:
        logger.warning("envelope invalido recebido do barramento: %r", message[:200])
        return 0
    sender = envelope.get("connection_id") if isinstance(envelope, dict) else None
    exclude = (sender,) if isinstance(sender, str) else ()
    return await manager.broadcast(message, exclude=exclude)


def _build_envelope(connection: Connection, raw: str) -> dict[str, Any]:
    return {
        "type": "message",
        "connection_id": connection.id,
        "instance_id": settings.instance_id,
        "server_timestamp": time.time(),
        "payload": _decode(raw),
    }


def _connected_event(connection: Connection) -> dict[str, Any]:
    return {
        "type": "connected",
        "connection_id": connection.id,
        "instance_id": settings.instance_id,
        "connected_users": manager.count,
        "server_timestamp": time.time(),
    }


def _decode(raw: str) -> Any:
    """JSON quando possivel, texto puro caso contrario."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
