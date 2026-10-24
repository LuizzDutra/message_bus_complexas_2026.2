"""Configuracoes da aplicacao (variaveis de ambiente / arquivo .env)."""

from __future__ import annotations

from typing import Literal
from uuid import uuid4

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_instance_id() -> str:
    """Id efemero desta instancia (processo) do servidor."""
    return f"instance-{uuid4().hex[:8]}"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Complexas - WebSocket Message Bus Benchmark"
    app_version: str = "0.1.0"

    # Identifica a instancia/conteiner. Sobrescreva via INSTANCE_ID no docker-compose
    # (ex.: "instance-1", "instance-2") para conseguir distinguir a origem dos eventos.
    instance_id: str = Field(default_factory=_default_instance_id)

    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "info"

    # "local" entrega apenas para as conexoes deste processo (default, usado nos testes).
    # "redis" publica no Redis Pub/Sub; "rabbitmq" publica em um exchange fanout.
    # Em ambos os casos cada instancia entrega aos seus proprios clientes.
    bus_backend: Literal["local", "redis", "rabbitmq"] = "local"
    redis_url: str = "redis://127.0.0.1:6379/0"
    redis_channel: str = "complexas:ws"
    rabbitmq_url: str = "amqp://localhost:5672/"
    rabbitmq_exchange: str = "complexas"
    rabbitmq_queue_prefix: str = "complexas."


settings = Settings()
