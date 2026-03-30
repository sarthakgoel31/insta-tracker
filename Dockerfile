FROM python:3.13-slim

# Install system deps: tesseract for OCR, chromium deps for Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && python -m playwright install chromium --with-deps

COPY . .

CMD uvicorn server:app --host 0.0.0.0 --port ${PORT:-8501}
