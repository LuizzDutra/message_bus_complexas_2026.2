"""Cliente WebSocket de linha de comando para teste manual rapido.

Nao depende de nada do pacote `app`: e so um cliente que abre conexoes no
endpoint /ws/message, imprime o que recebe e envia o que voce digitar.

Exemplos:
    uv run python scripts/ws_client.py                      # 1 conexao, modo interativo
    uv run python scripts/ws_client.py -n 2                 # 2 conexoes: o teclado envia pela conn1
    uv run python scripts/ws_client.py -m '{"texto":"oi"}' -i 0.5
    uv run python scripts/ws_client.py -u ws://127.0.0.1:8001/ws/message

No modo interativo digite e pressione Enter para enviar. Com `-n 2` voce ve que a
mensagem enviada pela conn1 chega na conn2 (e nao volta para quem enviou).
Use Ctrl+C (ou digite `exit`) para encerrar.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass, field

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

DEFAULT_URL = "ws://127.0.0.1:8000/ws/message"

# Ao encerrar, espera um pouco antes de cortar os receivers para nao perder
# mensagens enviadas nos ultimos milissegundos (comum ao usar stdin via pipe/EOF).
_DRAIN_SECONDS = 0.5


@dataclass
class Peer:
    """Uma conexao WebSocket e o que sabemos sobre ela."""

    index: int
    ws: ClientConnection
    connection_id: str = "?"
    instance_id: str = "?"
    open: bool = field(default=True)

    @property
    def tag(self) -> str:
        return f"conn{self.index}/{self.connection_id[:8]}"

    async def receive_loop(self) -> None:
        try:
            async for raw in self.ws:
                print(f"{self.tag} <- {_render(raw)}", flush=True)
        except ConnectionClosed:
            pass
        finally:
            self.open = False
            print(f"{self.tag} conexao encerrada", flush=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cliente WebSocket manual para o prototipo de message bus.",
    )
    parser.add_argument(
        "-u",
        "--url",
        default=DEFAULT_URL,
        help=f"URL do WebSocket (default: {DEFAULT_URL})",
    )
    parser.add_argument(
        "-n",
        "--connections",
        type=int,
        default=1,
        help="numero de conexoes simultaneas; apenas a conn1 recebe input do teclado (default: 1)",
    )
    parser.add_argument(
        "-m",
        "--message",
        default=None,
        help="envia esta mensagem repetidamente em vez de ler do teclado",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=float,
        default=1.0,
        help="intervalo em segundos entre envios no modo --message (default: 1.0)",
    )
    return parser.parse_args(argv)


async def main(args: argparse.Namespace) -> int:
    url = _normalize_url(args.url)
    if args.connections < 1:
        print("--connections deve ser >= 1", file=sys.stderr)
        return 2

    try:
        peers = await _open_peers(url, args.connections)
    except (TimeoutError, OSError, ConnectionClosed, InvalidStatus) as exc:
        print(f"nao foi possivel conectar em {url}: {exc}", file=sys.stderr)
        return 1

    print(f"conectado em {url} com {len(peers)} conexao(oes).")
    if args.message:
        print(f"enviando {args.message!r} a cada {args.interval}s pela conn1 (Ctrl+C para sair).")
        sender = asyncio.create_task(_message_loop(peers[0], args.message, args.interval))
    else:
        print("digite a mensagem e pressione Enter para enviar (Ctrl+C ou 'exit' para sair).")
        sender = asyncio.create_task(_input_loop(peers[0]))

    receivers = [asyncio.create_task(peer.receive_loop()) for peer in peers]
    try:
        await sender
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await _shutdown(peers, receivers)
    return 0


async def _open_peers(url: str, count: int) -> list[Peer]:
    """Abre todas as conexoes de uma vez (falha rapido se o servidor nao responder)."""
    sockets = await asyncio.gather(
        *(connect(url, open_timeout=5) for _ in range(count)),
        return_exceptions=True,
    )

    failures = [result for result in sockets if isinstance(result, BaseException)]
    if failures:
        for result in sockets:
            if isinstance(result, ClientConnection):
                await result.close()
        raise failures[0]

    peers: list[Peer] = []
    for index, ws in enumerate(sockets, start=1):
        assert isinstance(ws, ClientConnection)
        peer = Peer(index=index, ws=ws)
        try:
            hello = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
            peer.connection_id = str(hello.get("connection_id", "?"))
            peer.instance_id = str(hello.get("instance_id", "?"))
            print(
                f"{peer.tag} conectada (instance={peer.instance_id}, "
                f"usuarios nesta instancia={hello.get('connected_users')})",
                flush=True,
            )
        except (TimeoutError, ConnectionClosed, json.JSONDecodeError, AttributeError):
            print(f"{peer.tag} conectada (sem frame de boas-vindas reconhecivel)", flush=True)
        peers.append(peer)
    return peers


async def _input_loop(peer: Peer) -> None:
    """Le linhas do teclado e envia pela conexao informada."""
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:  # EOF (stdin fechado ou Ctrl+Z no Windows)
            return
        text = line.rstrip("\n")
        if text.strip().lower() in {"exit", "quit", "/exit", "/quit"}:
            return
        if not text.strip():
            continue
        await peer.ws.send(text)
        print(f"{peer.tag} -> {text}", flush=True)


async def _message_loop(peer: Peer, message: str, interval: float) -> None:
    while True:
        await peer.ws.send(message)
        print(f"{peer.tag} -> {message}", flush=True)
        await asyncio.sleep(interval)


async def _shutdown(peers: list[Peer], receivers: list[asyncio.Task[None]]) -> None:
    # deixa as mensagens ja enviadas chegarem antes de derrubar as conexoes
    _finished, pending = await asyncio.wait(receivers, timeout=_DRAIN_SECONDS)
    for task in pending:
        task.cancel()
    await asyncio.gather(*receivers, return_exceptions=True)
    await asyncio.gather(*(peer.ws.close() for peer in peers), return_exceptions=True)


def _render(raw: str) -> str:
    """Versao compacta do envelope recebido: mostra o payload e a origem."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if not isinstance(data, dict) or "payload" not in data:
        return raw
    payload = json.dumps(data["payload"], ensure_ascii=False)
    origin = str(data.get("connection_id", "?"))[:8]
    return f"payload={payload} de={origin} instance={data.get('instance_id', '?')}"


def _normalize_url(url: str) -> str:
    if url.startswith(("ws://", "wss://")):
        return url
    return f"ws://{url}"


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main(parse_args())))
    except KeyboardInterrupt:
        print("\nencerrado pelo usuario")
        sys.exit(130)
