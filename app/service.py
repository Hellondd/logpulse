"""Бизнес-логика приёма: строки -> парсер -> очередь. Её же будет вызывать API."""
from __future__ import annotations

from typing import Iterable

from .parser import parse_lines
from .redis_store import publish_events


def ingest_lines(r, lines: Iterable[str]) -> dict:
    """Валидные строки уходят в Redis Stream, невалидные - только в отчёт об ошибках."""
    result = parse_lines(lines)
    publish_events(r, result.valid)     # если Redis лёг - исключение (API вернёт 503)
    return {
        "accepted": len(result.valid),
        "rejected": len(result.errors),
        "errors": [e.to_dict() for e in result.errors],
    }
