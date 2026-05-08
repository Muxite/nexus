FROM python:3.12-slim

WORKDIR /app

# Install shared package first
COPY shared/ /build/shared/
RUN pip install --no-cache-dir /build/shared/

# Install service packages so imports work
COPY feeder/requirements.txt /tmp/feeder_req.txt
COPY trader/requirements.txt /tmp/trader_req.txt
COPY scanner/requirements.txt /tmp/scanner_req.txt
RUN pip install --no-cache-dir \
    -r /tmp/feeder_req.txt \
    -r /tmp/trader_req.txt \
    -r /tmp/scanner_req.txt

# Install integration test requirements
COPY integration_tests/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all app source trees so cross-service imports work
COPY scanner/app/ /scanner_app/
COPY feeder/app/ /feeder_app/
COPY trader/app/ /trader_app/
COPY monitor/app/ /monitor_app/

# Copy integration tests
COPY integration_tests/ /app/

ENV PYTHONPATH="/scanner_app:/feeder_app:/trader_app:/monitor_app:${PYTHONPATH}"
ENV IRENE_INTEGRATION_TESTS=true

CMD ["python", "-m", "pytest", "/app/", "-v", "--tb=short", "-m", "integration", "--asyncio-mode=auto"]
