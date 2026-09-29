"""GET /api/count/users - usuarios conectados nesta instancia do servidor."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from app.config import settings
from app.ws.manager import manager

router = APIRouter(prefix="/api/count", tags=["users"])


@router.get("/users", summary="Usuarios WebSocket conectados nesta instancia")
async def count_users() -> dict[str, Any]:
    return {
        "instance_id": settings.instance_id,
        "connected_users": manager.count,
    }
