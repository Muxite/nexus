FROM python:3.12-slim

WORKDIR /app

COPY shared/ /build/shared/
RUN pip install --no-cache-dir /build/shared/

COPY monitor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY monitor/app/ /app/
COPY frontend/ /app/static/

CMD ["python", "main.py"]
