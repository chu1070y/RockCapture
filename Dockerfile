FROM python:3.11-slim-bookworm

ARG PREFETCH_SPARK_PACKAGES=1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_HOSTNAME=localhost \
    SPARK_LOCAL_IP=127.0.0.1 \
    SPARK_TEMP_DIR=/app/tmp \
    SPARK_JARS_IVY=/app/.ivy2 \
    LOG_DIR=/app/logs \
    METADATA_DIR=/app/metadata

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        tini \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

RUN mkdir -p /app/logs /app/tmp /app/metadata /app/.ivy2 \
    && if [ "$PREFETCH_SPARK_PACKAGES" = "1" ]; then python scripts/prefetch_spark_packages.py; fi

EXPOSE 8000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "main.py"]
