"""Общие фикстуры для API-тестов."""
import fakeredis
import pytest
from fastapi.testclient import TestClient

from app.deps import get_db
from app.main import app
from app.redis_store import ensure_group
from app.worker import consume_once


@pytest.fixture
def r():
    """Чистый in-memory Redis на каждый тест."""
    return fakeredis.FakeStrictRedis(server=fakeredis.FakeServer(), decode_responses=True)


@pytest.fixture
def client(r):
    """TestClient, у которого зависимость get_db подменена на fakeredis."""
    app.dependency_overrides[get_db] = lambda: r
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def run_worker(r):
    """Прогоняет worker, пока очередь не опустеет (без блокировки и без отдельного процесса)."""
    def _run():
        ensure_group(r)
        while consume_once(r, "w1", block_ms=None):
            pass
    return _run
