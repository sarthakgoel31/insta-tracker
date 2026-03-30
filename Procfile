web: apt-get update -qq && apt-get install -y -qq tesseract-ocr > /dev/null 2>&1; playwright install chromium --with-deps && uvicorn server:app --host 0.0.0.0 --port $PORT
