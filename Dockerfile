FROM python:3.11-slim-bookworm

ARG PREFETCH_SPARK_PACKAGES=1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    TZ=Asia/Seoul \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_HOSTNAME=localhost \
    SPARK_LOCAL_IP=127.0.0.1 \
    SPARK_TEMP_DIR=/app/tmp \
    SPARK_JARS_IVY=/app/.ivy2 \
    SPARK_BUNDLED_JARS_DIR=/app/spark-jars \
    LOG_DIR=/app/logs \
    METADATA_DIR=/app/metadata

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        tzdata \
        tini \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

RUN mkdir -p /app/logs /app/tmp /app/metadata /app/.ivy2 /app/spark-jars \
    && if [ "$PREFETCH_SPARK_PACKAGES" = "1" ]; then python -m scripts.prefetch_spark_packages; fi

EXPOSE 8000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "main.py"]
