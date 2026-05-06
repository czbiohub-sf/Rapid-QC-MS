FROM python:3.9-slim

LABEL maintainer="Anthony Goering"
LABEL description="Python tools for the MS-DIAL annotation pipeline"

RUN pip install --no-cache-dir \
    numpy \
    openpyxl \
    pandas

ENTRYPOINT ["python3"]
