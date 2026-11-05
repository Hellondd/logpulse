"""Работа с Redis: очередь (Stream) и статистика.

Ключи:
  logpulse:events          Stream      очередь событий (XADD -> XREADGROUP -> XACK)
  logpulse:stats:level     Hash        уровень   -> счётчик
  logpulse:stats:service   Hash        сервис    -> счётчик
  logpulse:stats:status    Hash        HTTP-код  -> счётчик
  logpulse:slow            Sorted Set  member = rid, score = duration_ms (>= 500)
  logpulse:processed       Set         rid уже учтённых событий (идемпотентность)
  logpulse:stats:accepted  String      сколько событий ПРИНЯТО в очередь (API)
  logpulse:stats:processed String      сколько событий ОБРАБОТАНО worker'ом
"""
from __future__ import annotations

import logging
from typing import Iterable

import redis

from . import config
from .parser import SLOW_THRESHOLD_MS

log = logging.getLogger("logpulse.redis")

STREAM = "logpulse:events"
GROUP = "logpulse-workers"
STATS_LEVEL = "logpulse:stats:level"
STATS_SERVICE = "logpulse:stats:service"
STATS_STATUS = "logpulse:stats:status"
SLOW = "logpulse:slow"
PROCESSED_RIDS = "logpulse:processed"
CNT_ACCEPTED = "logpulse:stats:accepted"
CNT_PROCESSED = "logpulse:stats:processed"
ALL_KEYS = (STREAM, STATS_LEVEL, STATS_SERVICE, STATS_STATUS, SLOW,
            PROCESSED_RIDS, CNT_ACCEPTED, CNT_PROCESSED)

STREAM_MAXLEN = 100_000          # ограничиваем рост стрима (примерно, "~")
REQUIRED_FIELDS = ("timestamp", "level", "service", "ip", "method",
                   "path", "status", "duration_ms", "rid")
INT_FIELDS = ("status", "duration_ms")


def get_redis(url: str | None = None) -> "redis.Redis":
    """Клиент Redis. socket_timeout ДОЛЖЕН быть больше BLOCK у XREADGROUP."""
    return redis.Redis.from_url(
        url or config.redis_url(),
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=5,
        health_check_interval=30,
    )


def ensure_group(r) -> None:
    """Создаёт consumer group (и стрим, если его нет). Повторный вызов безопасен."""
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def publish_events(r, events: Iterable[dict]) -> list[str]:
    """XADD для каждого валидного события + счётчик 'принято' (одной транзакцией)."""
    events = list(events)
    if not events:
        return []
    pipe = r.pipeline(transaction=True)
    for ev in events:
        pipe.xadd(STREAM, {k: str(v) for k, v in ev.items()},
                  maxlen=STREAM_MAXLEN, approximate=True)
    pipe.incrby(CNT_ACCEPTED, len(events))
    return pipe.execute()[:-1]


def event_from_fields(fields: dict) -> dict:
    """Сообщение стрима (все значения - строки) -> событие. KeyError/ValueError при порче."""
    event = {k: fields[k] for k in REQUIRED_FIELDS}
    for k in INT_FIELDS:
        event[k] = int(event[k])
    return event


def apply_event(r, event: dict) -> bool:
    """Обновляет статистику. True - событие учтено, False - дубликат (rid уже был).

    Идемпотентность: если сообщение доставили повторно (worker упал после
    обновления статистики, но до XACK), rid уже лежит в logpulse:processed и
    счётчики второй раз не растут. Все обновления - одна транзакция MULTI/EXEC.
    """
    rid = event["rid"]
    if r.sismember(PROCESSED_RIDS, rid):
        return False
    pipe = r.pipeline(transaction=True)
    pipe.sadd(PROCESSED_RIDS, rid)
    pipe.hincrby(STATS_LEVEL, event["level"], 1)
    pipe.hincrby(STATS_SERVICE, event["service"], 1)
    pipe.hincrby(STATS_STATUS, str(event["status"]), 1)
    pipe.incr(CNT_PROCESSED)
    if event["duration_ms"] >= SLOW_THRESHOLD_MS:
        pipe.zadd(SLOW, {rid: event["duration_ms"]})
    pipe.execute()
    return True


def pending_count(r) -> int:
    """Сколько сообщений прочитано worker'ом, но ещё не подтверждено (XPENDING)."""
    try:
        return int(r.xpending(STREAM, GROUP)["pending"])
    except redis.exceptions.ResponseError:   # группы ещё нет
        return 0


def _ints(h: dict) -> dict:
    return {k: int(v) for k, v in h.items()}


def get_stats(r, top: int = 5) -> dict:
    """Агрегированная статистика - всё читается из Redis, а не из памяти."""
    accepted = int(r.get(CNT_ACCEPTED) or 0)
    processed = int(r.get(CNT_PROCESSED) or 0)
    slow = r.zrevrange(SLOW, 0, top - 1, withscores=True)
    return {
        "accepted": accepted,                      # принято в очередь
        "processed": processed,                    # обработано worker'ом
        "backlog": max(accepted - processed, 0),   # ещё ждёт обработки
        "pending_unacked": pending_count(r),       # прочитано, но без XACK
        "by_level": _ints(r.hgetall(STATS_LEVEL)),
        "by_service": _ints(r.hgetall(STATS_SERVICE)),
        "by_status": _ints(r.hgetall(STATS_STATUS)),
        "top_slow": [{"rid": rid, "duration_ms": int(score)} for rid, score in slow],
    }
