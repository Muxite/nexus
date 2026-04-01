FROM python:3.12-slim

WORKDIR /app

COPY shared/ /build/shared/
RUN pip install --no-cache-dir /build/shared/

COPY trader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY trader/app/ /app/

CMD ["python", "main.py"]
