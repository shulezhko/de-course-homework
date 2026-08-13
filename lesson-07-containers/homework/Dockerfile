FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN useradd --create-home --uid 1000 appuser

WORKDIR /app

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/gh_ingest.py .

RUN mkdir /cache && chown appuser:appuser /cache

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD ["python", "-c", "import sys; sys.exit(0)"]

ENTRYPOINT ["python", "gh_ingest.py"]
