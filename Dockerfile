# ─────────────────────────────────────────────────────────────────────────────
#  CalvinBot Dockerfile
#  Python 3.11 slim — instala dependencias desde requirements.txt
#  ISSUE #15: Incluye HEALTHCHECK para Kubernetes/Docker Compose
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema (necesarias para py-clob-client / web3)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements primero para aprovechar cache de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY *.py ./
COPY .env* ./

# Crear directorios de datos y logs
RUN mkdir -p /app/data /app/logs

# ISSUE #15: HEALTHCHECK para Docker Compose y Kubernetes
#  Verifica que el bot está escribiendo al log (latido) cada 300s máximo
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD if [ -f "calvin5.log" ] && [ $(( $(date +%s) - $(stat -c %Y calvin5.log) )) -lt 300 ]; then exit 0; else exit 1; fi || \
        if [ -f "executor.log" ] && [ $(( $(date +%s) - $(stat -c %Y executor.log) )) -lt 60 ]; then exit 0; else exit 1; fi || \
        exit 1

# No hay CMD aquí — cada servicio en docker-compose especifica su comando
