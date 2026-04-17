FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ /app/

# Data directory for SQLite DB
RUN mkdir -p /data

ENV PYTHONUNBUFFERED=1
ENV SEULGI_CONFIG=/config/config.yaml

CMD ["python", "main.py"]
