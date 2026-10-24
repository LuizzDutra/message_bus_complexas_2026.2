"""Barramento de eventos usando Redis Pub/Sub.

Cada instancia assina um canal unico. O envelope publicado pela instancia de origem
volta para todas as instancias (incluindo ela mesma), que entregam aos seus proprios
clientes; o filtro do remetente fica em `app.routers.websocket.deliver`.

Nao ha garantia de entrega nem persistencia: Pub/Sub perde mensagens se uma instancia
estiver desconectada. E proposital para a comparacao com RabbitMQ.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

logger = logging.getLogger(__name__)

MessageHandler = Callable[[str], Awaitable[object]]
ReconnectErrors = (RedisConnectionError, RedisTimeoutError)


class RedisBus:
    """Publica e assina um canal Redis, repassando cada mensagem ao handler."""

    def __init__(self, url: str, channel: str, *, client: Redis | None = None) -> None:
        self._url = url
        self._channel = channel
        self._client = client
        self._pubsub = None
        self._listener: asyncio.Task[None] | None = None
        self._handler: MessageHandler | None = None

    async def start(self, handler: MessageHandler) -> None:
        """Conecta, assina o canal e inicia a task de escuta."""
        self._handler = handler
        if self._client is None:
            self._client = Redis.from_url(
                self._url,
                decode_responses=True,
                socket_keepalive=True,
                health_check_interval=10,
            )
        await self._client.ping()
        await self._subscribe()
        self._listener = asyncio.create_task(self._listen(), name="redis-bus-listener")
        logger.info("barramento Redis ativo em %s (canal %s)", self._url, self._channel)

    async def publish(self, message: str) -> None:
        """Publica o envelope serializado no canal, reconectando uma vez se preciso."""
        if self._client is None:
            raise RuntimeError("barramento Redis nao iniciado")
        try:
            await self._client.publish(self._channel, message)
        except ReconnectErrors as exc:
            # A conexao em pool pode estar morta (ex.: restart do Redis); descarta e tenta de novo.
            logger.warning("publish falhou (%s); reconectando e tentando novamente", exc)
            await self._client.connection_pool.disconnect()
            await self._client.publish(self._channel, message)

    async def close(self) -> None:
        """Para a escuta e fecha pubsub/cliente."""
        if self._listener is not None:
            self._listener.cancel()
            await asyncio.gather(self._listener, return_exceptions=True)
            self._listener = None
        await self._close_pubsub()
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("barramento Redis encerrado")

    async def _subscribe(self) -> None:
        """Cria um pubsub novo e assina o canal (usado no start e na reconexao)."""
        assert self._client is not None
        await self._close_pubsub()
        self._pubsub = self._client.pubsub()
        await self._pubsub.subscribe(self._channel)

    async def _close_pubsub(self) -> None:
        if self._pubsub is None:
            return
        try:
            await self._pubsub.aclose()
        except Exception as exc:  # conexao ja quebrada nao deve impedir a reconexao
            logger.debug("falha ao fechar pubsub: %s", exc)
        self._pubsub = None

    async def _listen(self) -> None:
        """Le o canal indefinidamente, reassinando quando a conexao cair."""
        while True:
            try:
                if self._pubsub is None:
                    await self._subscribe()
                assert self._pubsub is not None
                async for message in self._pubsub.listen():
                    if message["type"] != "message":
                        continue
                    data = message["data"]
                    if isinstance(data, bytes):
                        data = data.decode()
                    if self._handler is not None:
                        await self._handler(data)
                logger.warning("subscribe Redis encerrou; reassinando")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # conexao caiu: tenta reassinar
                logger.warning("falha no subscribe Redis (%s); reassinando em 1s", exc)
            await self._close_pubsub()
            await asyncio.sleep(1)
