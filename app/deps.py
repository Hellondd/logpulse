"""Зависимости FastAPI. В тестах get_db подменяют через app.dependency_overrides."""
from .redis_store import get_redis

_client = None


def get_db():
    """Один клиент Redis на процесс (внутри него пул соединений)."""
    global _client
    if _client is None:
        _client = get_redis()
    return _client
