# Multi-arch Python slim — works on Pi 4/5 (arm64) and Pi 3/Zero (arm/v7)
FROM python:3.12-slim

# Keeps Python from buffering stdout/stderr (important for docker logs)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first (separate layer — only rebuilds on requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py db.py ./
COPY static/ static/

# Data directory for SQLite DB and logs (mounted as a volume)
RUN mkdir -p /data

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
