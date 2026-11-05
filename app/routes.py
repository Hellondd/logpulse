"""Ручки API."""
import redis
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from .deps import get_db
from .redis_store import get_stats, latest_events
from .schemas import (ErrorResponse, EventsResponse, HealthResponse,
                      IngestRequest, IngestResponse, StatsResponse)
from .service import ingest_lines

router = APIRouter(responses={
    422: {"model": ErrorResponse, "description": "Ошибка структуры запроса"},
    500: {"model": ErrorResponse, "description": "Внутренняя ошибка сервера"},
    503: {"model": ErrorResponse, "description": "Redis недоступен"},
})


@router.get("/health", response_model=HealthResponse, tags=["system"],
            responses={503: {"model": HealthResponse, "description": "Redis недоступен"}})
def health(r=Depends(get_db)):
    """Живость сервиса: API отвечает и Redis отвечает на PING."""
    try:
        r.ping()
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        return JSONResponse(status_code=503, content={"status": "degraded", "redis": "down"})
    return {"status": "ok", "redis": "ok"}


@router.post("/ingest", response_model=IngestResponse, tags=["logs"])
def ingest(body: IngestRequest, r=Depends(get_db)):
    """Принимает строки лога: валидные уходят в очередь, невалидные - в список errors."""
    return ingest_lines(r, body.lines)


@router.get("/stats", response_model=StatsResponse, tags=["logs"])
def stats(r=Depends(get_db)):
    """Статистика из Redis: принято / обработано / очередь, уровни, сервисы, коды, топ медленных."""
    return get_stats(r)


@router.get("/events", response_model=EventsResponse, tags=["logs"])
def events(limit: int = Query(10, ge=1, le=50, description="Сколько событий вернуть (1-50)"),
           r=Depends(get_db)):
    """Последние принятые события (новые первыми)."""
    items = latest_events(r, limit)
    return {"count": len(items), "events": items}
