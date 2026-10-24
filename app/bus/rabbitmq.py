"""Barramento de eventos usando RabbitMQ (exchange fanout + fila por instancia).

Cada instancia declara um exchange fanout durable e uma fila durable propria
(``complexas.<INSTANCE_ID>``) ligada a ele. Todas as instancias (inclusive a de
origem) recebem uma copia do evento; o filtro do remetente fica em
`app.routers.websocket.deliver`.

Semantica: ack manual apos a entrega local (`prefetch_count=1` preserva a ordem);
se o handler falhar a mensagem volta para a fila (at-least-once, com possibilidade
de duplicar a entrega local). Contrasta com o Redis Pub/Sub (at-most-once).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, IncomingMessage, Message
from aio_pika.exceptions import AMQPError

logger = logging.getLogger(__name__)

ReconnectErrors = (AMQPError, ConnectionError, OSError)


class RabbitMQBus:
    """Publica em um exchange fanout e consome da fila desta instancia."""

    def __init__(
        self,
        url: str,
        exchange: str,
        queue: str,
        *,
        connection: Any | None = None,
    ) -> None:
        self._url = url
        self._exchange_name = exchange
        self._queue_name = queue
        self._connection = connection
        self._channel = None
        self._exchange = None
        self._queue = None
        self._consumer_tag: str | None = None
        self._handler: Any = None

    async def start(self, handler: Any) -> None:
        """Conecta, declara topologia e inicia o consumo da fila."""
        self._handler = handler
        if self._connection is None:
            self._connection = await aio_pika.connect_robust(self._url)
        # `channel()` retorna um canal com `__await__`: sem aguardar, `_on_open` nao roda.
        self._channel = await self._connection.channel(publisher_confirms=True)
        await self._channel.set_qos(prefetch_count=1)
        self._exchange = await self._channel.declare_exchange(
            self._exchange_name, ExchangeType.FANOUT, durable=True
        )
        self._queue = await self._channel.declare_queue(self._queue_name, durable=True)
        await self._queue.bind(self._exchange, routing_key="")
        self._consumer_tag = await self._queue.consume(self._on_message, no_ack=False)
        logger.info(
            "barramento RabbitMQ ativo em %s (exchange=%s fila=%s)",
            self._url,
            self._exchange_name,
            self._queue_name,
        )

    async def publish(self, message: str) -> None:
        """Publica o envelope no exchange (retransmite uma vez se o canal cair)."""
        if self._exchange is None:
            raise RuntimeError("barramento RabbitMQ nao iniciado")
        payload = Message(body=message.encode("utf-8"), delivery_mode=DeliveryMode.PERSISTENT)
        try:
            await self._exchange.publish(payload, routing_key="")
        except ReconnectErrors as exc:
            logger.warning("publish falhou (%s); tentando novamente", exc)
            await asyncio.sleep(0.5)
            await self._exchange.publish(payload, routing_key="")

    async def close(self) -> None:
        """Cancela o consumidor e fecha canal/conexao (nesta ordem)."""
        if self._queue is not None and self._consumer_tag is not None:
            try:
                await self._queue.cancel(self._consumer_tag)
            except Exception as exc:
                logger.debug("falha ao cancelar consumidor: %s", exc)
        if self._channel is not None:
            try:
                await self._channel.close()
            except Exception as exc:
                logger.debug("falha ao fechar canal: %s", exc)
        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception as exc:
                logger.debug("falha ao fechar conexao: %s", exc)
        self._consumer_tag = None
        self._queue = None
        self._exchange = None
        self._channel = None
        self._connection = None
        logger.info("barramento RabbitMQ encerrado")

    async def _on_message(self, message: IncomingMessage) -> None:
        """Entrega ao handler local; ack em sucesso, requeue em falha."""
        try:
            body = message.body.decode("utf-8") if isinstance(message.body, bytes) else message.body
            if self._handler is not None:
                await self._handler(body)
        except Exception as exc:  # CancelledError (shutdown) nao entra aqui
            logger.warning("falha ao entregar mensagem do RabbitMQ (%s); reentregando", exc)
            await message.nack(requeue=True)
            return
        await message.ack()
