FROM python:3.12-slim

WORKDIR /srv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY docs ./docs

# Режим «между организациями»: 1 — включён (образ v2), 0 — скрыт (образ v1)
ARG CROSS_ORG_ENABLED=1
ENV CROSS_ORG_ENABLED=${CROSS_ORG_ENABLED}

# Целевой темп запросов к API Диска, запросов/с. Документированный потолок — 40
# (Условия использования API Диска, п. 2.2), по умолчанию берём половину.
# Ловите 429 в логе — снизьте; можно задать и на запуске: -e DISK_API_RPS=10
ARG DISK_API_RPS=20
ENV DISK_API_RPS=${DISK_API_RPS}

# Таймаут чтения ответа, с (таймаут соединения фиксирован — 10 с).
ARG HTTP_TIMEOUT_SEC=120
ENV HTTP_TIMEOUT_SEC=${HTTP_TIMEOUT_SEC}

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
