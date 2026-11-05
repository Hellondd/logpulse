"""Настройки из переменных окружения (в Docker их задаёт compose)."""
import os


def redis_url() -> str:
    # В контейнерах хост будет "redis" (DNS-имя сервиса), локально - localhost.
    return os.getenv("REDIS_URL", "redis://localhost:6379/0")


def worker_name() -> str:
    # Имя консьюмера должно быть СТАБИЛЬНЫМ между перезапусками, иначе
    # неподтверждённые сообщения останутся висеть у "старого" консьюмера.
    return os.getenv("WORKER_NAME", "worker-1")


def worker_batch() -> int:
    return int(os.getenv("WORKER_BATCH", "100"))


def worker_block_ms() -> int:
    return int(os.getenv("WORKER_BLOCK_MS", "2000"))
