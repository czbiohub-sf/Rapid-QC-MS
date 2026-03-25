FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --no-cache-dir -e .

EXPOSE 8050

# In Argus, POSTGRES_USER, POSTGRES_PASSWORD, and __ARGUS_STACK_NAME are injected
# at runtime, so RAPIDQCMS_DB_URL is assembled from those parts by the shell.
# Outside Argus (local dev / tests), RAPIDQCMS_DB_URL can be set directly;
# if neither is present, SQLite is used as a fallback.
CMD ["/bin/sh", "-c", \
     "export RAPIDQCMS_DB_URL=${RAPIDQCMS_DB_URL:-postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${__ARGUS_STACK_NAME}-stack-postgres:5432/rapidqcms} && \
      exec gunicorn rapidqcms.dashboard.app:server \
        --workers 2 \
        --bind 0.0.0.0:8050 \
        --timeout 120 \
        --access-logfile - \
        --error-logfile -"]
