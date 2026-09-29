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

O ponto de extensao do trabalho e a funcao `publish`: hoje ela entrega apenas
para as conexoes locais; nas variantes com barramento ela passara a publicar no
Redis (PUBLISH) ou no RabbitMQ (exchange) e cada instancia entregara aos seus
proprios clientes ao receber o evento de volta.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from fastapi import APIRouter, WebSocket

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
            await publish(_build_envelope(connection, raw), sender_id=connection.id)
    finally:
        await manager.disconnect(connection.id)
        logger.info("cliente %s desconectado de %s", connection.id, settings.instance_id)


async def publish(envelope: dict[str, Any], *, sender_id: str) -> int:
    """Entrega um evento aos clientes conectados.

    >>> PONTO DE EXTENSAO DE BARRAMENTO <<<
    Na versao final esta funcao publicara o envelope no barramento
    (Redis Pub/Sub ou RabbitMQ) em vez de chamar `manager.broadcast` diretamente,
    ou fara as duas coisas (entrega local + publicacao) caso esta instancia tambem
    receba de volta o evento que publicou.
    """
    return await manager.broadcast(_dumps(envelope), exclude=(sender_id,))


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
