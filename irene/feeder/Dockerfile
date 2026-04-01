FROM python:3.12-slim

WORKDIR /app

COPY shared/ /build/shared/
RUN pip install --no-cache-dir /build/shared/

COPY feeder/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Feeder imports Embedder from scanner — include scanner/app on PYTHONPATH
COPY scanner/app/ /scanner_app/
COPY feeder/app/ /app/

ENV PYTHONPATH="/scanner_app:${PYTHONPATH}"

CMD ["python", "main.py"]
