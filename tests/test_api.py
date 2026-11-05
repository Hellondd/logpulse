"""Тесты API: TestClient + fakeredis через dependency_overrides (настоящий Redis не нужен)."""
import fakeredis
import pytest
import redis
from fastapi.testclient import TestClient

from app.deps import get_db
from app.main import app
from app.redis_store import ensure_group
from app.worker import consume_once

GOOD = "2026-10-20T12:34:56Z INFO auth 192.0.2.10 POST /api/login 200 43ms rid={rid}"


@pytest.fixture
def r():
    return fakeredis.FakeStrictRedis(server=fakeredis.FakeServer(), decode_responses=True)


@pytest.fixture
def client(r):
    app.dependency_overrides[get_db] = lambda: r
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


class DownRedis:
    """Имитация упавшего Redis: любой вызов -> ConnectionError."""
    def __getattr__(self, name):
        def fail(*a, **k):
            raise redis.exceptions.ConnectionError("redis is down")
        return fail


@pytest.fixture
def client_down():
    app.dependency_overrides[get_db] = lambda: DownRedis()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def lines(*rids):
    return [GOOD.format(rid=rid) for rid in rids]


# ---------- /health ----------
def test_health_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "redis": "ok"}


def test_health_503_when_redis_down(client_down):
    resp = client_down.get("/health")
    assert resp.status_code == 503
    assert resp.json() == {"status": "degraded", "redis": "down"}


# ---------- /ingest ----------
def test_ingest_accepts_valid_and_reports_invalid(client):
    body = {"lines": lines("aaaaaaa1", "aaaaaaa2") + ["это не лог", GOOD.format(rid="bad").replace("INFO", "DEBUG")]}
    resp = client.post("/ingest", json=body)
    assert resp.status_code == 200
    data = resp.json()
    assert (data["accepted"], data["rejected"]) == (2, 2)
    assert [e["line_no"] for e in data["errors"]] == [3, 4]
    assert client.get("/events").json()["count"] == 2      # битые в очередь не попали


@pytest.mark.parametrize("payload", [
    {},                          # нет поля lines
    {"lines": "abc"},            # не список
    {"lines": []},               # пустой список
    {"lines": [123]},            # не строки
    {"lines": None},
])
def test_ingest_422_on_wrong_structure(client, payload):
    assert client.post("/ingest", json=payload).status_code == 422


def test_ingest_422_on_broken_json(client):
    resp = client.post("/ingest", content="{not json", headers={"Content-Type": "application/json"})
    assert resp.status_code == 422


def test_ingest_503_when_redis_down(client_down):
    resp = client_down.post("/ingest", json={"lines": lines("aaaaaaa1")})
    assert resp.status_code == 503


# ---------- /stats ----------
def test_stats_shows_backlog_before_worker(client):
    client.post("/ingest", json={"lines": lines("aaaaaaa1", "aaaaaaa2", "aaaaaaa3")})
    s = client.get("/stats").json()
    assert (s["accepted"], s["processed"], s["backlog"]) == (3, 0, 3)


def test_stats_after_worker_processed(client, r):
    client.post("/ingest", json={"lines": lines("aaaaaaa1", "aaaaaaa2", "aaaaaaa3")})
    ensure_group(r)
    while consume_once(r, "w1", block_ms=None):
        pass
    s = client.get("/stats").json()
    assert (s["accepted"], s["processed"], s["backlog"]) == (3, 3, 0)
    assert s["by_level"] == {"INFO": 3} and s["by_service"] == {"auth": 3}
    assert s["by_status"] == {"200": 3}


def test_stats_503_when_redis_down(client_down):
    assert client_down.get("/stats").status_code == 503


# ---------- /events ----------
def test_events_newest_first_and_limit(client):
    client.post("/ingest", json={"lines": lines("aaaaaaa1", "aaaaaaa2", "aaaaaaa3")})
    data = client.get("/events", params={"limit": 2}).json()
    assert data["count"] == 2
    assert [e["rid"] for e in data["events"]] == ["aaaaaaa3", "aaaaaaa2"]
    assert data["events"][0]["status"] == 200


@pytest.mark.parametrize("limit", [0, -1, 51, 100, "abc"])
def test_events_422_on_bad_limit(client, limit):
    assert client.get("/events", params={"limit": limit}).status_code == 422
