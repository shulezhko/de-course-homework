FROM python:3.12-slim

# Залежності окремим шаром (зміна коду не тригерить переустановку pip)
COPY app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Non-root user
RUN useradd -m -u 1000 appuser \
    && mkdir -p /cache && chown appuser:appuser /cache

# Код додаємо після pip install (cache-friendly)
COPY app/ /app/

ENV PYTHONUNBUFFERED=1

USER appuser

# Реальний healthcheck: перевіряємо чи Postgres досяжний
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD python -c "import psycopg, os; psycopg.connect(host=os.environ.get('PGHOST','postgres'), port=os.environ.get('PGPORT','5432'), user=os.environ['PGUSER'], password=os.environ['PGPASSWORD'], dbname=os.environ['PGDATABASE']).close()" || exit 1

ENTRYPOINT ["python", "/app/gh_ingest.py"]
