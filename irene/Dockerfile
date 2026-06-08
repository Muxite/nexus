FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.test.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ /app/app/
COPY scripts/ /app/scripts/
# capture_schedule.py is a root-level module imported bare by app/config.py
# (runs with irene/ on sys.path). Added in c4f7489; must be copied explicitly.
COPY capture_schedule.py /app/

RUN mkdir -p /data

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV IRENE_CONFIG=/config/config.yaml

CMD ["python", "-m", "app"]
