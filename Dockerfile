FROM python:3.11-slim

# Install Chromium and dependencies for Pyppeteer
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdrm2 \
    libgbm1 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    && rm -rf /var/lib/apt/lists/*

# Set Chromium path for Pyppeteer
ENV CHROMIUM_PATH=/usr/bin/chromium
ENV PYPPETEER_CHROMIUM_REVISION=0
ENV PYPPETEER_HOME=/tmp/pyppeteer

WORKDIR /app

# Copy and install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]" 2>/dev/null || pip install --no-cache-dir .

# Copy application code
COPY . .

EXPOSE 8080

# Run FastAPI API server (Streamlit UI available separately)
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8080"]
