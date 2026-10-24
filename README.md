# Complexas - WebSocket Message Bus Benchmark

Protótipo de um servidor **FastAPI** com suporte a **WebSockets** usado para comparar
**Redis Pub/Sub** e **RabbitMQ** como barramento de eventos (*message bus*) entre instâncias
horizontalmente escaladas.

O problema investigado: cada instância guarda apenas as suas próprias conexões WebSocket, então
um evento gerado por um usuário na instância A precisa chegar ao usuário na instância B. As duas
variantes de barramento estão implementadas: `BUS_BACKEND=redis` (Pub/Sub, at-most-once) e
`BUS_BACKEND=rabbitmq` (fanout + fila por instância, at-least-once com acks). Sem barramento o
servidor entrega apenas localmente (`BUS_BACKEND=local`, default).

## Requisitos

- [uv](https://docs.astral.sh/uv/) (gerencia Python, dependências e virtualenv)
- Redis 7+ e/ou RabbitMQ 4+ apenas para rodar com o barramento (ou use o Docker Compose)

## Executar

```bash
uv sync                        # cria/atualiza o .venv a partir do uv.lock
uv run fastapi dev app/main.py # desenvolvimento (reload) em http://127.0.0.1:8000
uv run python -m app.main      # usa HOST/PORT do .env
```

Por padrão a entrega é local. Para usar um dos barramentos:

```bash
docker run -d --name complexas-redis -p 6379:6379 redis:7-alpine
BUS_BACKEND=redis uv run python -m app.main      # REDIS_URL/REDIS_CHANNEL opcionais

docker run -d --name complexas-rabbitmq -p 5672:5672 -e RABBITMQ_DEFAULT_USER=complexas \
  -e RABBITMQ_DEFAULT_PASS=complexas rabbitmq:4-management
BUS_BACKEND=rabbitmq uv run python -m app.main   # RABBITMQ_URL/RABBITMQ_EXCHANGE opcionais
```

No PowerShell: `$env:BUS_BACKEND="redis"; uv run python -m app.main`. Confira o backend ativo em
`GET /api/health` (campo `bus_backend`).

Docs interativas: `http://127.0.0.1:8000/docs` (o `/ws/message` não aparece lá, WebSocket não é
documentável no OpenAPI).

## Docker Compose

`compose.yaml` sobe 3 instâncias do servidor e o Redis; o backend é escolhido por variável de
ambiente (default `redis`):

```bash
docker compose up --build          # variante Redis: portas 8000, 8001 e 8002
BUS_BACKEND=rabbitmq docker compose --profile rabbitmq up --build   # variante RabbitMQ
docker compose up --build -d       # em background
docker compose ps                  # status/healthcheck de cada instância
docker compose logs -f instance-1
docker compose down
```

| Serviço      | Porta local | `INSTANCE_ID`  |
| ------------ | ----------- | -------------- |
| `instance-1` | 8000        | `instance-1`   |
| `instance-2` | 8001        | `instance-2`   |
| `instance-3` | 8002        | `instance-3`   |
| `redis`      | 6379        | —              |
| `rabbitmq`   | 5672 / 15672 | — (profile)   |

A imagem (`Dockerfile`) usa a base do `uv` com Python 3.11, instala a partir do `uv.lock` e roda
como usuário não-root. O `healthcheck` consulta `/api/health` dentro do container; as instâncias só
sobem depois do `redis` ficar saudável.

O RabbitMQ só sobe com o profile `rabbitmq` (console em <http://localhost:15672>, user/pass
`complexas`). Como o profile impede `depends_on`, na variante RabbitMQ as instâncias podem reiniciar
algumas vezes até o broker ficar pronto — normal em um primeiro `up`:

```bash
docker compose --profile rabbitmq up -d rabbitmq   # só o broker (espera ficar saudável)
BUS_BACKEND=rabbitmq docker compose --profile rabbitmq up --build -d
```

Com um barramento ativo, mensagens atravessam instâncias. Abra um cliente em cada porta e envie pela
conn1 de uma delas; o texto deve aparecer no cliente da outra instância (e não volta para quem
enviou):

```bash
uv run python scripts/ws_client.py -u ws://127.0.0.1:8000/ws/message -n 2
uv run python scripts/ws_client.py -u ws://127.0.0.1:8001/ws/message -n 2
```

## Endpoints

| Método | Rota                 | Descrição                                                              |
| ------ | -------------------- | ---------------------------------------------------------------------- |
| GET    | `/api/health`        | Status do servidor: versão, `instance_id`, `bus_backend` e uptime      |
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
- O remetente não recebe a própria mensagem, inclusive quando a entrega passa pelo barramento.
- Com barramento ativo, o envelope publicado pela instância de origem volta para **todas** as
  instâncias (a de origem apenas filtra o remetente); o `instance_id` no envelope indica onde a
  conexão original está.
- Frames binários são ignorados neste protótipo.
- Se o barramento estiver fora do ar (ex.: reiniciando Redis/RabbitMQ), a instância **descarta o
  evento e registra um warning**, mantendo a conexão WebSocket dos usuários viva — a
  semântica de perda já é a esperada do modo escolhido (at-most-once / at-least-once).
- Nota de implementação: não há backpressure por conexão — um cliente lento acumula frames no
  buffer de saída do próprio WebSocket. Isso deve ser levado em conta nos testes de carga.
- Semântica por variante (é o que se compara):
  - **Redis Pub/Sub** — at-most-once: mensagens não são persistidas e instâncias desconectadas
    perdem eventos; após uma queda, mensagens publicadas na janela de reconexão também se perdem.
  - **RabbitMQ** — at-least-once: fila durável por instância (`complexas.<INSTANCE_ID>`) com ack
    manual após a entrega local; se a entrega local falhar a mensagem volta para a fila
    (`prefetch_count=1` preserva a ordem).

## Cliente WebSocket para teste manual

`scripts/ws_client.py` abre conexões no `/ws/message`, imprime o que chega e envia o que você
digitar — sem depender do pacote `app`.

```bash
uv run python scripts/ws_client.py                              # 1 conexao, modo interativo
uv run python scripts/ws_client.py -n 2                         # 2 conexoes: o teclado envia pela conn1
uv run python scripts/ws_client.py -m '{"texto":"oi"}' -i 0.5   # envia sozinho a cada 0,5 s
uv run python scripts/ws_client.py -u ws://127.0.0.1:8001/ws/message
```

No modo interativo, digite e pressione Enter para enviar; `exit` ou Ctrl+C encerram. Com `-n 2`
dá para ver que a mensagem enviada pela conn1 chega na conn2 e **não** volta para quem enviou:

```
conn1/917b3988 conectada (instance=instance-…, usuarios nesta instancia=1)
conn2/a48d9a46 conectada (instance=instance-…, usuarios nesta instancia=2)
conn1/917b3988 -> ola mundo
conn2/a48d9a46 <- payload="ola mundo" de=917b3988 instance=instance-…
```

Para testar o cenário entre instâncias, aponte um cliente para cada porta em terminais separados
(`-u ws://127.0.0.1:8000/ws/message` e `-u ws://127.0.0.1:8001/ws/message`). Com um barramento
ativo (`BUS_BACKEND=redis` ou `BUS_BACKEND=rabbitmq`), o que for enviado em uma instância chega aos
clientes da outra.

## Estrutura

```
app/
  main.py               # cria a aplicação, lifespan (start/close do barramento) e routers
  config.py             # Settings: INSTANCE_ID, HOST/PORT, LOG_LEVEL, BUS_BACKEND, REDIS_*, RABBITMQ_*
  bus/
    __init__.py         # create_bus(): Redis/RabbitMQ conforme BUS_BACKEND, senão entrega local
    redis.py            # RedisBus: PUBLISH + task de SUBSCRIBE com reconexão
    rabbitmq.py         # RabbitMQBus: exchange fanout, fila por instância, ack/requeue (aio-pika)
  routers/
    health.py           # GET /api/health
    users.py            # GET /api/count/users
    websocket.py        # WS /ws/message + publish() e deliver() (barramento)
  ws/
    manager.py          # ConnectionManager: registro das conexões locais, send/broadcast/disconnect
scripts/
  ws_client.py          # cliente WebSocket de linha de comando (teste manual)
tests/
  test_api.py           # testes de fumaça (HTTP + broadcast via WebSocket)
  test_bus.py           # publish/deliver e RedisBus com fakeredis (sem Redis externo)
  test_rabbitmq_bus.py  # RabbitMQBus com fakes de AMQP (sem broker externo)
compose.yaml            # 3 instâncias; backend por BUS_BACKEND, RabbitMQ no profile "rabbitmq"
Dockerfile              # imagem da aplicação (Python 3.11 + uv)
```

## Testes e lint

```bash
uv run pytest
uv run ruff check .
uv run ruff format .
```

Os testes não precisam de serviços externos: `tests/test_bus.py` usa `fakeredis`,
`tests/test_rabbitmq_bus.py` usa fakes de AMQP e o modo default é `BUS_BACKEND=local`. Para
exercitar os barramentos de verdade, use o compose.

## Próximos passos

1. Script de carga com **Locust** para gerar tráfego WebSocket e medir latência (p50/p95/p99),
   throughput, perda de mensagens, CPU e memória — comparando `redis` × `rabbitmq`.
