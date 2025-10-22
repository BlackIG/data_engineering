# ops/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Africa/Lagos \
    PORT=8080

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first for caching
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy app code (use a .dockerignore to keep context lean)
COPY . /app

EXPOSE 8080

# Bind gunicorn to $PORT so Compose/Cloud Run can override
CMD ["sh", "-c", "gunicorn -b 0.0.0.0:${PORT} -k gthread --threads 8 --timeout 3600 --access-logfile - --log-level info app:app"]
