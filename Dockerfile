FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --no-cache-dir -e .

ENV RAPIDQCMS_DB_URL=sqlite:///data/rapidqcms.db

VOLUME ["/app/data"]

EXPOSE 8050

# Production: gunicorn WSGI server.
# The ECS task definition overrides this command with worker count tuned to CPU.
CMD ["gunicorn", "rapidqcms.dashboard.app:server", \
     "--workers", "2", \
     "--bind", "0.0.0.0:8050", \
     "--timeout", "120", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]
