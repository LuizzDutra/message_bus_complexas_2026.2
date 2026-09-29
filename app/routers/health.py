"""GET /api/health - check basico de status do servidor."""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter

from app.config import settings

router = APIRouter(prefix="/api", tags=["health"])

_STARTED_AT = time.monotonic()


@router.get("/health", summary="Status do servidor")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "app": settings.app_name,
        "version": settings.app_version,
        "instance_id": settings.instance_id,
        "uptime_seconds": round(time.monotonic() - _STARTED_AT, 3),
    }
