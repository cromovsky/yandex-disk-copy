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

# Лимит API Диска на один IP и клиент: 40 rps чтение, 5 rps изменение.
# Ловите 429 в логе — снизьте; можно задать на запуске:
#   -e DISK_API_READ_RPS=20 -e DISK_API_WRITE_RPS=2
ARG DISK_API_READ_RPS=40
ARG DISK_API_WRITE_RPS=5
ENV DISK_API_READ_RPS=${DISK_API_READ_RPS} \
    DISK_API_WRITE_RPS=${DISK_API_WRITE_RPS}

# Таймаут чтения ответа, с (таймаут соединения фиксирован — 10 с).
ARG HTTP_TIMEOUT_SEC=120
ENV HTTP_TIMEOUT_SEC=${HTTP_TIMEOUT_SEC}

# Локальное сохранение. Каталог внутри контейнера; на хост его выводит том
# ./downloads:/downloads. LOCAL_SAVE_HOST_DIR — путь в логе для человека.
ENV LOCAL_SAVE_DIR=/downloads \
    LOCAL_SAVE_HOST_DIR=./downloads

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
