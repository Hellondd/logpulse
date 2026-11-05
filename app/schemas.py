"""Pydantic-модели запросов и ответов API."""
from typing import Annotated, Dict, List, Optional

from pydantic import BaseModel, Field

EXAMPLE_LINE = "2026-10-20T12:34:56Z INFO auth 192.0.2.10 POST /api/login 200 43ms rid=ab12cd34"


# Каждая строка: от 1 до 500 символов
LogLine = Annotated[str, Field(min_length=1, max_length=500)]


class IngestRequest(BaseModel):
    lines: List[LogLine] = Field(
        min_length=1, max_length=100,
        description="Строки лога: от 1 до 100 штук за запрос, длина каждой от 1 до 500 символов.",
    )
    model_config = {"json_schema_extra": {"examples": [{"lines": [EXAMPLE_LINE, "not a log line"]}]}}


class LineError(BaseModel):
    line_no: int = Field(description="Номер строки в запросе, с 1")
    reason: str
    line: str = Field(description="Сама строка (секреты заменены на ***)")


class IngestResponse(BaseModel):
    accepted: int = Field(description="Сколько строк принято в очередь")
    rejected: int = Field(description="Сколько строк не прошло валидацию")
    errors: List[LineError]


class TopSlow(BaseModel):
    rid: str
    duration_ms: int


class StatsResponse(BaseModel):
    accepted: int
    processed: int
    backlog: int
    pending_unacked: int
    by_level: Dict[str, int]
    by_service: Dict[str, int]
    by_status: Dict[str, int]
    top_slow: List[TopSlow]


class EventOut(BaseModel):
    id: str = Field(description="ID записи в Redis Stream")
    timestamp: str
    level: str
    service: str
    ip: str
    method: str
    path: str
    status: int
    duration_ms: int
    rid: str


class EventsResponse(BaseModel):
    count: int
    events: List[EventOut]


class HealthResponse(BaseModel):
    status: str
    redis: str


class ErrorDetail(BaseModel):
    field: str = Field(description="Путь к полю, например body.lines.0")
    message: str


class ErrorBody(BaseModel):
    code: str = Field(description="validation_error | redis_unavailable | not_found | method_not_allowed | internal_error ...")
    message: str
    details: Optional[List[ErrorDetail]] = None


class ErrorResponse(BaseModel):
    """Единый формат ошибок API."""
    error: ErrorBody
