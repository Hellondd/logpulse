"""Дополнительные API-тесты: границы, пустое состояние, сквозные сценарии, 503 и HTTP-ошибки."""
from pathlib import Path

import pytest
import redis

from app.deps import get_db
from app.main import app
from app.parser import parse_lines

SAMPLE = Path(__file__).resolve().parent.parent / "data" / "sample.log"


def rid(n: int) -> str:
    return f"{n:08x}"


def line(n: int, dur: int = 43, level: str = "INFO", status: int = 200) -> str:
    return (f"2026-10-20T12:34:56Z {level} auth 192.0.2.10 POST /api/login "
            f"{status} {dur}ms rid={rid(n)}")


class FailingRedis:
    """Redis, который на любой вызов бросает заданную ошибку (ConnectionError / TimeoutError)."""
    def __init__(self, exc):
        self.exc = exc

    def __getattr__(self, name):
        def fail(*a, **k):
            raise self.exc
        return fail


# ---------- пустое состояние ----------
def test_stats_on_empty_redis_returns_zeros(client):
    resp = client.get("/stats")
    assert resp.status_code == 200
    assert resp.json() == {
        "accepted": 0, "processed": 0, "backlog": 0, "pending_unacked": 0,
        "by_level": {}, "by_service": {}, "by_status": {}, "top_slow": [],
    }


def test_events_on_empty_redis(client):
    resp = client.get("/events")
    assert resp.status_code == 200
    assert resp.json() == {"count": 0, "events": []}


# ---------- /ingest: границы и особые случаи ----------
def test_ingest_limit_100_lines_ok_101_rejected(client):
    ok = client.post("/ingest", json={"lines": [line(i) for i in range(1, 101)]})
    assert ok.status_code == 200 and ok.json()["accepted"] == 100
    too_many = client.post("/ingest", json={"lines": [line(i) for i in range(1, 102)]})
    assert too_many.status_code == 422
    assert too_many.json()["error"]["code"] == "validation_error"


def test_ingest_line_length_limits(client):
    assert client.post("/ingest", json={"lines": ["a" * 500]}).status_code == 200   # ровно 500 - можно
    assert client.post("/ingest", json={"lines": ["a" * 501]}).status_code == 422   # 501 - отказ
    assert client.post("/ingest", json={"lines": [""]}).status_code == 422          # пустая строка - отказ


def test_ingest_errors_keep_request_line_numbers(client):
    body = {"lines": ["мусор", line(1), "ещё мусор", line(2), "   "]}
    data = client.post("/ingest", json=body).json()
    assert (data["accepted"], data["rejected"]) == (2, 3)
    assert [e["line_no"] for e in data["errors"]] == [1, 3, 5]


def test_ingest_response_does_not_leak_secrets(client):
    resp = client.post("/ingest", json={"lines": ["garbage password=supersecret token: abc123"]})
    assert resp.status_code == 200
    assert "supersecret" not in resp.text and "abc123" not in resp.text
    assert resp.json()["rejected"] == 1


def test_ingest_accepts_line_with_tabs_and_extra_spaces(client):
    messy = "2026-10-20T12:34:56Z\tINFO   auth 192.0.2.10 POST /api/login 200 43ms rid=ab12cd34\r"
    assert client.post("/ingest", json={"lines": [messy]}).json()["accepted"] == 1


def test_ingest_rejects_unicode_digits(client):
    sneaky = line(1).replace(" 200 ", " ٢٠٠ ")
    data = client.post("/ingest", json={"lines": [sneaky]}).json()
    assert (data["accepted"], data["rejected"]) == (0, 1)


# ---------- /events ----------
def test_events_default_limit_is_10(client):
    client.post("/ingest", json={"lines": [line(i) for i in range(1, 26)]})
    assert client.get("/events").json()["count"] == 10


@pytest.mark.parametrize("limit", [1, 50])
def test_events_limit_boundaries_are_allowed(client, limit):
    client.post("/ingest", json={"lines": [line(i) for i in range(1, 4)]})
    resp = client.get("/events", params={"limit": limit})
    assert resp.status_code == 200
    assert resp.json()["count"] == min(limit, 3)


# ---------- сквозные сценарии: API -> очередь -> worker -> /stats ----------
def test_duplicate_rid_is_accepted_twice_but_processed_once(client, run_worker):
    client.post("/ingest", json={"lines": [line(1)]})
    client.post("/ingest", json={"lines": [line(1)]})        # тот же rid ещё раз
    run_worker()
    s = client.get("/stats").json()
    assert s["accepted"] == 2 and s["processed"] == 1
    assert s["by_level"] == {"INFO": 1}


def test_top_slow_through_api(client, run_worker):
    client.post("/ingest", json={"lines": [line(0xA1, dur=499), line(0xA2, dur=500),
                                           line(0xA3, dur=1200)]})
    run_worker()
    assert client.get("/stats").json()["top_slow"] == [
        {"rid": rid(0xA3), "duration_ms": 1200},
        {"rid": rid(0xA2), "duration_ms": 500},       # 499 не дотянул до порога
    ]


def test_stats_response_has_exact_fields(client, run_worker):
    client.post("/ingest", json={"lines": [line(1, dur=900)]})
    run_worker()
    s = client.get("/stats").json()
    assert set(s) == {"accepted", "processed", "backlog", "pending_unacked",
                      "by_level", "by_service", "by_status", "top_slow"}
    assert set(s["top_slow"][0]) == {"rid", "duration_ms"}


def test_sample_log_end_to_end_through_api(client, run_worker):
    lines = SAMPLE.read_text(encoding="utf-8").splitlines()
    expected = parse_lines(lines)
    data = client.post("/ingest", json={"lines": lines}).json()
    assert (data["accepted"], data["rejected"]) == (len(expected.valid), len(expected.errors))
    run_worker()
    s = client.get("/stats").json()
    assert s["processed"] == len(expected.valid) and s["backlog"] == 0
    assert sum(s["by_status"].values()) == len(expected.valid)
    slow = sorted((e["duration_ms"] for e in expected.valid if e["duration_ms"] >= 500), reverse=True)
    assert [t["duration_ms"] for t in s["top_slow"]] == slow[:5]
    assert client.get("/events", params={"limit": 50}).json()["count"] == len(expected.valid)


# ---------- 503: упавший Redis и таймауты ----------
@pytest.mark.parametrize("method, path, body", [
    ("GET", "/health", None),
    ("GET", "/stats", None),
    ("GET", "/events", None),
    ("POST", "/ingest", {"lines": [line(1)]}),
])
@pytest.mark.parametrize("exc", [redis.exceptions.ConnectionError("down"),
                                 redis.exceptions.TimeoutError("timeout")])
def test_every_endpoint_returns_503_when_redis_fails(client, method, path, body, exc):
    app.dependency_overrides[get_db] = lambda: FailingRedis(exc)
    assert client.request(method, path, json=body).status_code == 503


def test_503_body_is_friendly_json_not_traceback(client):
    app.dependency_overrides[get_db] = lambda: FailingRedis(redis.exceptions.ConnectionError("down"))
    resp = client.get("/stats")
    assert resp.status_code == 503
    assert resp.json() == {"error": {"code": "redis_unavailable",
                                     "message": "Redis недоступен, попробуйте позже"}}


def test_api_recovers_after_redis_comes_back(client, r):
    app.dependency_overrides[get_db] = lambda: FailingRedis(redis.exceptions.ConnectionError("down"))
    assert client.get("/health").status_code == 503
    app.dependency_overrides[get_db] = lambda: r              # Redis "поднялся"
    assert client.get("/health").status_code == 200
    assert client.post("/ingest", json={"lines": [line(1)]}).json()["accepted"] == 1


# ---------- обычные HTTP-ошибки и документация ----------
@pytest.mark.parametrize("method, path", [("GET", "/ingest"), ("POST", "/stats"),
                                          ("POST", "/events"), ("DELETE", "/health")])
def test_wrong_method_returns_405(client, method, path):
    assert client.request(method, path).status_code == 405


def test_unknown_path_returns_404(client):
    assert client.get("/nope").status_code == 404


def test_openapi_lists_all_endpoints(client):
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    assert set(resp.json()["paths"]) == {"/health", "/ingest", "/stats", "/events"}


def test_swagger_docs_page_is_available(client):
    assert client.get("/docs").status_code == 200


# ---------- единый формат ошибок ----------
def test_422_uses_unified_error_format(client):
    resp = client.post("/ingest", json={"lines": "abc"})
    assert resp.status_code == 422
    err = resp.json()["error"]
    assert err["code"] == "validation_error"
    assert err["details"][0]["field"] == "body.lines"
    assert "input" not in resp.text                      # значение из запроса обратно не отдаём


def test_422_does_not_echo_secrets_from_request(client):
    resp = client.post("/ingest", json={"lines": [{"password": "hunter2"}]})
    assert resp.status_code == 422
    assert "hunter2" not in resp.text


def test_404_uses_unified_error_format(client):
    resp = client.get("/nope")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


def test_unexpected_error_returns_500_without_traceback(r):
    from fastapi.testclient import TestClient
    app.dependency_overrides[get_db] = lambda: FailingRedis(RuntimeError("secret internals /srv/app/file.py"))
    try:
        with TestClient(app, raise_server_exceptions=False) as c:
            resp = c.get("/stats")
    finally:
        app.dependency_overrides.clear()
    assert resp.status_code == 500
    assert resp.json() == {"error": {"code": "internal_error", "message": "Внутренняя ошибка сервера"}}
    assert "secret internals" not in resp.text and "Traceback" not in resp.text
