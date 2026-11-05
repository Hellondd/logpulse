"""Единый формат ошибок: {"error": {"code": ..., "message": ..., "details": [...]}}.

Наружу не уходят трассировки, внутренние тексты исключений и значения из запроса
(в стандартном 422 FastAPI эхом возвращает "input", а там может быть что угодно, в том числе секреты).
"""
import logging

import redis
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

log = logging.getLogger("logpulse.api")

HTTP_CODES = {404: "not_found", 405: "method_not_allowed"}


def error_response(status: int, code: str, message: str, details=None) -> JSONResponse:
    body = {"error": {"code": code, "message": message}}
    if details:
        body["error"]["details"] = details
    return JSONResponse(status_code=status, content=body)


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        details = [{"field": ".".join(str(part) for part in err.get("loc", ())),
                    "message": str(err.get("msg", ""))} for err in exc.errors()]
        return error_response(422, "validation_error", "Некорректная структура запроса", details)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request, exc):
        return error_response(exc.status_code, HTTP_CODES.get(exc.status_code, "http_error"),
                              str(exc.detail))

    @app.exception_handler(redis.exceptions.ConnectionError)
    @app.exception_handler(redis.exceptions.TimeoutError)
    async def redis_unavailable(request, exc):
        return error_response(503, "redis_unavailable", "Redis недоступен, попробуйте позже")

    @app.exception_handler(Exception)
    async def unexpected(request, exc):
        log.exception("необработанная ошибка в %s %s", request.method, request.url.path)  # трассировка только в лог
        return error_response(500, "internal_error", "Внутренняя ошибка сервера")
