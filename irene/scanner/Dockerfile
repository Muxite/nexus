FROM python:3.12-slim

WORKDIR /app

# Install shared package first (cached layer)
COPY shared/ /build/shared/
RUN pip install --no-cache-dir /build/shared/

# Install scanner dependencies
COPY scanner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scanner source
COPY scanner/app/ /app/

CMD ["python", "main.py"]
