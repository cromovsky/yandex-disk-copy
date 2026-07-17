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

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
