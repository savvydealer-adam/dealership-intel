FROM python:3.11-slim

# Install Google Chrome (not Chromium - needed for nodriver Cloudflare bypass)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
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
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Set Chrome path for nodriver and pyppeteer
ENV CHROMIUM_PATH=/usr/bin/google-chrome-stable
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
