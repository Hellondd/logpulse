"""Точка входа:  uvicorn app.main:app --reload"""
from fastapi import FastAPI

from .errors import register_error_handlers
from .routes import router

app = FastAPI(
    title="LogPulse",
    version="1.0.0",
    description="Приём логов, разбор, очередь в Redis Streams и статистика.",
)
app.include_router(router)
register_error_handlers(app)
