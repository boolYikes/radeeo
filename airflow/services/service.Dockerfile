FROM python:3.12-slim-bookworm

WORKDIR /workspace

COPY requirements.txt .
COPY .env .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "/bin/bash" ]
