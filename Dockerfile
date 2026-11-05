# Один образ на два сервиса: api (uvicorn) и worker. Команда задаётся в compose.yaml.
# В образ входят тесты и dev-зависимости, чтобы работало `docker compose exec api pytest -q`.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Не запускаем процессы от root.
RUN useradd --create-home --uid 1000 appuser && chown appuser:appuser /app

# Сначала зависимости: этот слой кэшируется, пока requirements*.txt не менялись.
COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

COPY --chown=appuser:appuser app ./app
COPY --chown=appuser:appuser tests ./tests
COPY --chown=appuser:appuser data ./data
COPY --chown=appuser:appuser pytest.ini ./

USER appuser
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
