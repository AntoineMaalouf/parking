# Pin exact patch version for reproducible builds (#22)
FROM python:3.12.10-slim

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

# Health check — Docker can restart the container if the app hangs (#20)
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
