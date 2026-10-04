FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY backend ./backend

RUN --mount=type=secret,id=registry_ca,target=/tmp/machina-registry-ca.crt,required=false \
    if [ -s /tmp/machina-registry-ca.crt ]; then \
      cat /tmp/machina-registry-ca.crt >> /etc/ssl/certs/ca-certificates.crt \
      && export PIP_CERT=/tmp/machina-registry-ca.crt; \
    fi \
    && pip install --no-cache-dir --default-timeout=120 --retries=5 . \
    && groupadd --system --gid 10001 machina \
    && useradd --system --uid 10001 --gid machina --no-create-home machina

USER 10001:10001
EXPOSE 8000

CMD ["uvicorn", "machina_stream.api:app", "--host", "0.0.0.0", "--port", "8000"]
