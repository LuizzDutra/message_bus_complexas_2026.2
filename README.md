# Complexas - WebSocket Message Bus Benchmark

Protótipo de um servidor **FastAPI** com suporte a **WebSockets** usado para comparar
**Redis Pub/Sub** e **RabbitMQ** como barramento de eventos (*message bus*) entre instâncias
horizontalmente escaladas.

O problema investigado: cada instância guarda apenas as suas próprias conexões WebSocket, então
um evento gerado por um usuário na instância A precisa chegar ao usuário na instância B. Nesta
etapa o servidor entrega apenas localmente — o `publish` é o ponto onde o barramento entrará.

## Requisitos

- [uv](https://docs.astral.sh/uv/) (gerencia Python, dependências e virtualenv)

## Executar

```bash
uv sync                        # cria/atualiza o .venv a partir do uv.lock
uv run fastapi dev app/main.py # desenvolvimento (reload) em http://127.0.0.1:8000
uv run python -m app.main      # usa HOST/PORT do .env
```

Docs interativas: `http://127.0.0.1:8000/docs` (o `/ws/message` não aparece lá, WebSocket não é
documentável no OpenAPI).

## Endpoints

| Método | Rota                 | Descrição                                                              |
| ------ | -------------------- | ---------------------------------------------------------------------- |
| GET    | `/api/health`        | Status do servidor: versão, `instance_id` e uptime                     |
| GET    | `/api/count/users`   | Quantos usuários estão conectados **nesta instância**                  |
| WS     | `/ws/message`        | Recebe a carga e repassa para **todos os outros** usuários conectados  |

## Protocolo do WebSocket

Ao conectar, o cliente recebe um aviso de boas-vindas com o seu id e a instância de origem:

```json
{"type": "connected", "connection_id": "…", "instance_id": "instance-…",
 "connected_users": 2, "server_timestamp": 1790641796.22}
```

Depois disso, **todo** frame de texto enviado é repassado aos demais clientes como:

```json
{"type": "message", "connection_id": "…", "instance_id": "instance-…",
 "server_timestamp": 1790641796.23, "payload": {"sala": "geral", "texto": "ola"}}
```

- Se o frame for JSON, o valor decodificado vai inteiro em `payload`.
- Se for texto puro, o próprio texto vira `payload` (`"payload": "ping"`).
- O remetente não recebe a própria mensagem.
- Frames binários são ignorados neste protótipo.
- Nota de implementação: não há backpressure por conexão — um cliente lento acumula frames no
  buffer de saída do próprio WebSocket. Isso deve ser levado em conta nos testes de carga.

## Estrutura

```
app/
  main.py               # cria a aplicação, lifespan e routers
  config.py             # Settings (pydantic-settings): INSTANCE_ID, HOST, PORT, LOG_LEVEL
  routers/
    health.py           # GET /api/health
    users.py            # GET /api/count/users
    websocket.py        # WS /ws/message + função publish() -> ponto de extensão do barramento
  ws/
    manager.py          # ConnectionManager: registro das conexões locais, send/broadcast/disconnect
tests/
  test_api.py           # testes de fumaça (HTTP + broadcast via WebSocket)
```

## Testes e lint

```bash
uv run pytest
uv run ruff check .
uv run ruff format .
```

## Próximos passos

1. Implementar a variante **Redis Pub/Sub** (`publish` → `PUBLISH` + task de `SUBSCRIBE` por
   instância).
2. Implementar a variante **RabbitMQ** (exchange fanout + fila durável por instância, com acks).
3. Subir N instâncias com `docker compose` (`INSTANCE_ID=instance-1`, `instance-2`, …).
4. Script de carga com **Locust** para gerar tráfego WebSocket e medir latência (p50/p95/p99),
   throughput, perda de mensagens, CPU e memória.
