"""Gerenciador de conexoes WebSocket mantidas por uma unica instancia do servidor.

Nesta etapa o gerenciador e puramente local: apenas as conexoes abertas neste
processo recebem as mensagens. O ponto de extensao para o barramento de eventos
(Redis Pub/Sub ou RabbitMQ) fica em `app.routers.websocket.publish`.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from uuid import uuid4

from fastapi import WebSocket

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Connection:
    """Uma conexao WebSocket ativa e o id que a identifica."""

    id: str
    websocket: WebSocket


class ConnectionManager:
    """Registro das conexoes WebSocket ativas nesta instancia.

    O dicionario interno nunca e aguardado durante um `await`, portanto leituras
    como `count` e `connection_ids` sao seguras sem lock. O lock protege apenas
    as secoes que mutam o registro.
    """

    def __init__(self) -> None:
        self._connections: dict[str, Connection] = {}
        self._lock = asyncio.Lock()

    @property
    def count(self) -> int:
        """Quantidade de usuarios conectados nesta instancia."""
        return len(self._connections)

    def connection_ids(self) -> list[str]:
        """Ids das conexoes ativas (util para debug e para o barramento)."""
        return list(self._connections)

    async def connect(self, websocket: WebSocket, connection_id: str | None = None) -> Connection:
        """Aceita o handshake e registra a conexao."""
        await websocket.accept()
        connection = Connection(id=connection_id or uuid4().hex, websocket=websocket)
        async with self._lock:
            self._connections[connection.id] = connection
        logger.debug("conexao %s registrada (total=%d)", connection.id, self.count)
        return connection

    async def disconnect(self, connection_id: str) -> None:
        """Remove a conexao do registro (idempotente)."""
        async with self._lock:
            self._connections.pop(connection_id, None)
        logger.debug("conexao %s removida (total=%d)", connection_id, self.count)

    async def send(self, connection_id: str, message: str) -> bool:
        """Envia para uma conexao especifica. Retorna False se ela nao existe ou falhou."""
        connection = self._connections.get(connection_id)
        if connection is None:
            return False
        return await self._safe_send(connection, message)

    async def broadcast(self, message: str, *, exclude: Iterable[str] = ()) -> int:
        """Envia a mesma mensagem para todas as conexoes, exceto as de `exclude`.

        Retorna quantas conexoes receberam a mensagem com sucesso. Conexoes que
        falharem sao descartadas.
        """
        excluded = set(exclude)
        targets = [conn for conn_id, conn in self._connections.items() if conn_id not in excluded]
        if not targets:
            return 0
        results = await asyncio.gather(*(self._safe_send(conn, message) for conn in targets))
        return sum(results)

    async def close_all(self) -> None:
        """Fecha todas as conexoes (usado no shutdown da aplicacao)."""
        async with self._lock:
            connections = list(self._connections.values())
            self._connections.clear()
        if connections:
            await asyncio.gather(
                *(conn.websocket.close() for conn in connections),
                return_exceptions=True,
            )

    async def _safe_send(self, connection: Connection, message: str) -> bool:
        try:
            await connection.websocket.send_text(message)
        except Exception as exc:  # conexao morta/quebrada nao deve derrubar o broadcast
            logger.warning("falha ao enviar para %s: %s", connection.id, exc)
            await self.disconnect(connection.id)
            return False
        return True


# Instancia unica compartilhada pelos routers desta instancia do servidor.
manager = ConnectionManager()
