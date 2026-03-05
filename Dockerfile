FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --no-cache-dir -e .

ENV RAPIDQCMS_DB_URL=sqlite:///data/rapidqcms.db

VOLUME ["/app/data"]

EXPOSE 8050

CMD ["rapidqcms", "serve", "--no-browser"]
