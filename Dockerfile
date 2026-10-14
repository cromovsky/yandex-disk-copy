FROM python:3.12-slim

WORKDIR /srv

ARG VERSION=3.0.0
ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="Yandex Disk Copy" \
    org.opencontainers.image.version="${VERSION}" \
    org.opencontainers.image.revision="${VCS_REF}" \
    org.opencontainers.image.source="https://github.com/cromovsky/yandex-disk-copy" \
    org.opencontainers.image.licenses="MIT"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY docs/INSTRUCTION.md docs/ONPREM.md docs/UPDATE.md ./docs/

# Режим «между организациями»: 1 — включён (образ v2), 0 — скрыт (образ v1)
ARG CROSS_ORG_ENABLED=1
ENV CROSS_ORG_ENABLED=${CROSS_ORG_ENABLED}

# Общий бюджет утилиты ниже опубликованного потолка SaaS 40 запросов/с.
# HTTP 429 приостанавливает все потоки; при необходимости снижайте параметры.
ARG DISK_API_TOTAL_RPS=30
ARG DISK_API_READ_RPS=30
ARG DISK_API_WRITE_RPS=3
ENV DISK_API_READ_RPS=${DISK_API_READ_RPS} \
    DISK_API_WRITE_RPS=${DISK_API_WRITE_RPS} \
    DISK_API_TOTAL_RPS=${DISK_API_TOTAL_RPS}

# Таймаут чтения ответа, с (таймаут соединения фиксирован — 10 с).
ARG HTTP_TIMEOUT_SEC=120
ENV HTTP_TIMEOUT_SEC=${HTTP_TIMEOUT_SEC}

# Локальное сохранение. Каталог внутри контейнера; на хост его выводит том
# ./downloads:/downloads. LOCAL_SAVE_HOST_DIR — путь в логе для человека.
ENV LOCAL_SAVE_DIR=/downloads \
    LOCAL_SAVE_HOST_DIR=./downloads \
    ONPREM_ENABLED=1 \
    ONPREM_DATA_DIR=/var/lib/disk-copy

RUN mkdir -p /var/lib/disk-copy/staging \
    && chmod 700 /var/lib/disk-copy /var/lib/disk-copy/staging

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
