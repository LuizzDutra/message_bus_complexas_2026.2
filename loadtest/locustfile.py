"""Locustfile do teste de carga WebSocket (`/ws/message`).

Uso:
    uv run locust -f loadtest/locustfile.py --list
    uv run locust -f loadtest/locustfile.py --headless -u 4 -r 4 -t 20s --only-summary
    COMPLEXAS_WS_URLS=ws://127.0.0.1:8000,... uv run locust -f loadtest/locustfile.py ...

A classe de usuario vive em `loadtest/ws_user.py`; importe apenas ela aqui. Importar
`User` do Locust neste arquivo o registraria como uma segunda classe de usuario.
"""

from __future__ import annotations

from loadtest.ws_user import ComplexasUser

__all__ = ["ComplexasUser"]
