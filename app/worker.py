"""Worker: читает Redis Stream через consumer group и считает статистику.

Запуск:  python -m app.worker
Правило: XACK только ПОСЛЕ успешного обновления статистики (at-least-once).
"""
from __future__ import annotations

import logging
import signal
import time
from typing import Callable

import redis

from . import config
from .redis_store import (GROUP, STREAM, apply_event, ensure_group,
                          event_from_fields, get_redis)

log = logging.getLogger("logpulse.worker")


def _messages(resp) -> list:
    """Ответ XREADGROUP -> плоский список (id, fields)."""
    out = []
    for _stream, msgs in resp or []:
        out.extend(msgs)
    return out


def handle_messages(r, messages: list) -> int:
    """Обрабатывает сообщения и подтверждает их. Возвращает число обработанных."""
    for msg_id, fields in messages:
        if not fields:                      # запись удалена из стрима (trim)
            r.xack(STREAM, GROUP, msg_id)
            continue
        try:
            event = event_from_fields(fields)
        except (KeyError, ValueError) as exc:
            # "Ядовитое" сообщение: без XACK оно зацикливалось бы навсегда.
            log.warning("битое сообщение %s пропущено: %r", msg_id, exc)
            r.xack(STREAM, GROUP, msg_id)
            continue
        apply_event(r, event)               # при ошибке Redis сюда XACK не дойдёт
        r.xack(STREAM, GROUP, msg_id)       # подтверждаем только после успеха
    return len(messages)


def drain_pending(r, consumer: str, count: int = 100) -> int:
    """Дообрабатывает сообщения, прочитанные ранее, но не подтверждённые (id='0')."""
    total = 0
    while True:
        msgs = _messages(r.xreadgroup(GROUP, consumer, {STREAM: "0"}, count=count))
        if not msgs:
            return total
        total += handle_messages(r, msgs)


def consume_once(r, consumer: str, count: int = 100, block_ms: int | None = 2000) -> int:
    """Один проход: читаем НОВЫЕ сообщения (id='>'), обрабатываем, подтверждаем."""
    resp = r.xreadgroup(GROUP, consumer, {STREAM: ">"}, count=count, block=block_ms)
    return handle_messages(r, _messages(resp))


def run(r, consumer: str, *, count: int = 100, block_ms: int | None = 2000,
        should_stop: Callable[[], bool] = lambda: False,
        sleep: Callable[[float], None] = time.sleep, max_backoff: float = 10) -> None:
    """Основной цикл. Ошибки связи с Redis не валят процесс: ждём и пробуем снова."""
    backoff = 1.0
    need_recovery = True            # при старте и после любой ошибки связи
    while not should_stop():
        try:
            if need_recovery:
                ensure_group(r)
                n = drain_pending(r, consumer, count)
                if n:
                    log.info("дообработано неподтверждённых сообщений: %d", n)
                need_recovery = False
            consume_once(r, consumer, count, block_ms)
            backoff = 1.0
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
            log.warning("Redis недоступен (%s), повтор через %.0f c", exc, backoff)
            need_recovery = True
            sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        except redis.exceptions.ResponseError as exc:
            # например NOGROUP: Redis перезапустился без данных - пересоздадим группу
            log.warning("ошибка ответа Redis (%s), пересоздаю группу", exc)
            need_recovery = True
            sleep(backoff)


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    stop = {"flag": False}

    def _stop(*_):                  # docker stop шлёт SIGTERM
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    consumer = config.worker_name()
    log.info("worker '%s' стартовал, Redis: %s", consumer, config.redis_url())
    run(get_redis(), consumer, count=config.worker_batch(),
        block_ms=config.worker_block_ms(), should_stop=lambda: stop["flag"])
    log.info("worker остановлен")


if __name__ == "__main__":
    main()
