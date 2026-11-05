"""Тесты очереди/worker'а на fakeredis: настоящий Redis не нужен, рабочие данные не трогаем."""
import fakeredis
import pytest
import redis

from app.parser import parse_lines
from app.redis_store import (PROCESSED_RIDS, SLOW, STREAM, apply_event, ensure_group,
                             get_stats, latest_events, pending_count, publish_events)
from app.service import ingest_lines
from app.worker import GROUP, consume_once, drain_pending, handle_messages, run

GOOD = "2026-10-20T12:34:56Z INFO auth 192.0.2.10 POST /api/login 200 43ms rid={rid}"


@pytest.fixture
def r():
    return fakeredis.FakeStrictRedis(server=fakeredis.FakeServer(), decode_responses=True)


def ev(rid, level="INFO", service="auth", status=200, dur=10):
    return {"timestamp": "2026-10-20T12:00:00Z", "level": level, "service": service,
            "ip": "192.0.2.10", "method": "GET", "path": "/api/x",
            "status": status, "duration_ms": dur, "rid": rid}


def work_until_empty(r, consumer="w1"):
    ensure_group(r)
    while consume_once(r, consumer, block_ms=None):
        pass


def test_ensure_group_is_idempotent(r):
    ensure_group(r)
    ensure_group(r)          # BUSYGROUP не должен всплыть наружу
    assert r.xlen(STREAM) == 0


def test_accepted_is_not_processed_until_worker_runs(r):
    publish_events(r, [ev("00000001"), ev("00000002"), ev("00000003")])
    s = get_stats(r)
    assert (s["accepted"], s["processed"], s["backlog"]) == (3, 0, 3)
    assert r.xlen(STREAM) == 3
    work_until_empty(r)
    s = get_stats(r)
    assert (s["accepted"], s["processed"], s["backlog"]) == (3, 3, 0)


def test_counters_accumulate_after_processing(r):
    publish_events(r, [
        ev("00000001", "INFO", "auth", 200), ev("00000002", "INFO", "auth", 200),
        ev("00000003", "ERROR", "orders", 503), ev("00000004", "WARN", "payment", 429),
    ])
    work_until_empty(r)
    s = get_stats(r)
    assert s["by_level"] == {"INFO": 2, "ERROR": 1, "WARN": 1}
    assert s["by_service"] == {"auth": 2, "orders": 1, "payment": 1}
    assert s["by_status"] == {"200": 2, "503": 1, "429": 1}


def test_top_slow_sorted_desc_and_threshold_500(r):
    publish_events(r, [ev("00000001", dur=499), ev("00000002", dur=500),
                       ev("00000003", dur=742), ev("00000004", dur=1200)])
    work_until_empty(r)
    top = get_stats(r)["top_slow"]
    assert [t["rid"] for t in top] == ["00000004", "00000003", "00000002"]
    assert [t["duration_ms"] for t in top] == [1200, 742, 500]   # 499 в рейтинг не попал


def test_top_slow_is_limited_to_five(r):
    publish_events(r, [ev(f"0000000{i}", dur=500 + i) for i in range(1, 8)])
    work_until_empty(r)
    top = get_stats(r)["top_slow"]
    assert len(top) == 5 and top[0]["duration_ms"] == 507


def test_message_is_acked_only_after_processing(r):
    publish_events(r, [ev("00000001")])
    ensure_group(r)
    r.xreadgroup(GROUP, "w1", {STREAM: ">"})            # прочитали, но НЕ подтвердили
    assert pending_count(r) == 1
    assert get_stats(r)["processed"] == 0


def test_unacked_message_is_redelivered_after_worker_crash(r):
    publish_events(r, [ev("00000001"), ev("00000002")])
    ensure_group(r)
    r.xreadgroup(GROUP, "w1", {STREAM: ">"})            # "worker упал" до обработки
    assert pending_count(r) == 2
    assert drain_pending(r, "w1") == 2                  # после рестарта дообработал
    s = get_stats(r)
    assert (s["processed"], s["pending_unacked"]) == (2, 0)


def test_redelivery_does_not_duplicate_stats(r):
    publish_events(r, [ev("00000001", dur=900)])
    ensure_group(r)
    msgs = r.xreadgroup(GROUP, "w1", {STREAM: ">"})[0][1]
    handle_messages(r, msgs)
    handle_messages(r, msgs)                            # повторная доставка того же сообщения
    s = get_stats(r)
    assert s["processed"] == 1 and s["by_level"] == {"INFO": 1}
    assert r.zcard(SLOW) == 1
    assert apply_event(r, ev("00000001")) is False      # дубликат по rid


def test_poison_message_is_acked_and_ignored(r):
    ensure_group(r)
    r.xadd(STREAM, {"foo": "bar"})                      # нет обязательных полей
    publish_events(r, [ev("00000001")])
    work_until_empty(r)
    s = get_stats(r)
    assert s["processed"] == 1 and s["pending_unacked"] == 0


def test_ingest_mixed_batch_3_valid_2_invalid(r):
    lines = [GOOD.format(rid="aaaaaaa1"), GOOD.format(rid="aaaaaaa2"),
             GOOD.format(rid="aaaaaaa3"),
             GOOD.format(rid="aaaaaaa4").replace("INFO", "DEBUG"),
             "это не лог"]
    out = ingest_lines(r, lines)
    assert (out["accepted"], out["rejected"]) == (3, 2)
    assert [e["line_no"] for e in out["errors"]] == [4, 5]
    assert r.xlen(STREAM) == 3                          # битые в очередь не попали


class FlakyRedis:
    """Обёртка: первые N вызовов XREADGROUP падают как при недоступном Redis."""
    def __init__(self, real, failures):
        self.real, self.failures = real, failures

    def xreadgroup(self, *a, **k):
        if self.failures > 0:
            self.failures -= 1
            raise redis.exceptions.ConnectionError("redis is down")
        return self.real.xreadgroup(*a, **k)

    def __getattr__(self, name):
        return getattr(self.real, name)


def test_worker_survives_redis_connection_errors(r):
    publish_events(r, [ev("00000001"), ev("00000002")])
    flaky, sleeps, calls = FlakyRedis(r, failures=2), [], {"n": 0}

    def should_stop():
        calls["n"] += 1
        return calls["n"] > 6

    run(flaky, "w1", block_ms=None, should_stop=should_stop, sleep=sleeps.append)
    assert sleeps == [1.0, 2.0]                         # экспоненциальная пауза, без падения
    assert get_stats(r)["processed"] == 2               # после "восстановления" всё обработано


def test_sample_log_end_to_end(r):
    lines = open("data/sample.log", encoding="utf-8").read().splitlines()
    expected = parse_lines(lines)
    out = ingest_lines(r, lines)
    assert (out["accepted"], out["rejected"]) == (len(expected.valid), len(expected.errors))
    work_until_empty(r)
    s = get_stats(r)
    assert s["processed"] == len(expected.valid)
    assert sum(s["by_level"].values()) == sum(s["by_service"].values()) == len(expected.valid)
    slow = sorted((e["duration_ms"] for e in expected.valid if e["duration_ms"] >= 500), reverse=True)
    assert [t["duration_ms"] for t in s["top_slow"]] == slow[:5]


def test_latest_events_newest_first_and_limited(r):
    publish_events(r, [ev(f"0000000{i}") for i in range(1, 5)])
    items = latest_events(r, limit=3)
    assert [e["rid"] for e in items] == ["00000004", "00000003", "00000002"]
    assert items[0]["status"] == 200 and "id" in items[0]
