# RockCapture

RockCapture는 MySQL 또는 PostgreSQL 데이터를 스냅샷 방식으로 읽어 MinIO 기반 Apache Iceberg 테이블로 적재하는 FastAPI 기반 Lakehouse 적재 서비스입니다.

전체 데이터베이스 스냅샷 적재와 조건 기반 단일 테이블 스냅샷을 모두 지원하며, 실행/상태 조회/취소 같은 작업 제어를 HTTP API로 제공합니다.

## Features

- MySQL, PostgreSQL 전체 스냅샷 적재
- FastAPI 기반 실행 API 제공
- 백그라운드 Job 실행 및 상태 추적
- Spark JDBC 병렬 읽기
- 대용량 테이블 배치 적재
- MySQL 문자열 PK 대상 해시 배치 처리
- PostgreSQL 숫자 PK 부재 시 `ctid` 기반 적재 fallback
- Iceberg 테이블 저장 및 적재 후 compaction 수행
- 스냅샷 메타데이터를 MinIO에 저장
- Docker 환경 실행 지원

## Overview

RockCapture는 먼저 원본 DB의 스냅샷 시점을 고정한 뒤, 테이블 통계와 PK 정보를 수집해서 테이블별 적재 전략을 결정합니다.

- MySQL
  - `FLUSH TABLES WITH READ LOCK` 사용
  - binlog 위치 저장
  - 워커 연결에서 `START TRANSACTION WITH CONSISTENT SNAPSHOT` 사용
- PostgreSQL
  - `REPEATABLE READ` 사용
  - `pg_export_snapshot()`으로 스냅샷 공유
  - WAL LSN 위치 저장

이후 Spark JDBC로 테이블을 읽고, MinIO의 Iceberg 경로에 적재합니다.

## Tech Stack

- Python 3.11
- FastAPI
- Uvicorn
- PySpark 4.1.1
- Apache Iceberg Spark Runtime
- MinIO(S3A)
- PyMySQL
- psycopg2

## Project Structure

```text
RockCapture/
|- main.py
|- requirements.txt
|- pipeline.yaml.example
|- Dockerfile
|- docker-compose.yml
|- core/
|- connectors/
|- drivers/
`- logs/
```

## Configuration

프로젝트 루트에 `pipeline.yaml` 파일을 만들어야 합니다.

```bash
cp pipeline.yaml.example pipeline.yaml
```

예시:

```yaml
minio:
  endpoint: http://10.65.50.111:9000
  access_key: admin
  secret_key: your-secret
  bucket: gwchu-data
  region: us-east-1
  ssl_enabled: false

spark:
  app_name: LakeHouse
  log_level: INFO
  driver_memory: 3g
  ivy_cache_dir: /app/.ivy2
  extra_jars: []

iceberg:
  catalog_name: lakehouse
  warehouse: s3a://gwchu-data/iceberg-warehouse

logging:
  level: INFO

pipeline:
  num_threads: 4
  chunk_size: 100000
  large_table_threshold: 1000000
  large_table_batch_size: 1000000
  large_table_workers: 3
  stats_max_age_seconds: 86400
```

주요 옵션 설명:

- `num_threads`: 일반 크기 테이블 적재 워커 수
- `chunk_size`: Spark JDBC 분할 수 계산 기준 단위
- `large_table_threshold`: 대용량 테이블로 분류할 row 수 기준
- `large_table_batch_size`: 대용량 테이블 배치 적재 단위
- `large_table_workers`: 대용량 테이블 전용 워커 수
- `stats_max_age_seconds`: 통계 정보가 오래된 경우 `ANALYZE`를 다시 수행하는 기준 시간
- `ivy_cache_dir`: Spark가 외부 패키지 캐시를 저장할 경로
- `extra_jars`: `spark.jars`에 함께 포함할 추가 JAR 목록

## Local Run

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Check JDBC Drivers

프로젝트에 필요한 JDBC 드라이버가 이미 포함되어 있습니다.

- `drivers/mysql-connector-j-9.2.0.jar`
- `drivers/postgresql-42.7.9.jar`
- `drivers/mariadb-java-client-3.5.3.jar`

### 3. Start API Server

```bash
python main.py
```

실행 후 확인 경로:

- `http://localhost:8000`
- `http://localhost:8000/docs`
- `http://localhost:8000/health`

## Docker Run

### Build Image

```bash
docker build -t rockcapture .
```

리눅스 서버에서 컨테이너 실행 시 외부 Maven 접근 없이 바로 뜨게 하려면, 기본값 그대로 빌드하는 것을 권장합니다. 이 이미지는 빌드 단계에서 Spark 패키지를 미리 받아오도록 구성되어 있습니다.

### Run with Docker

```bash
docker run --rm \
  -p 8000:8000 \
  -v ${PWD}/pipeline.yaml:/app/pipeline.yaml:ro \
  -v ${PWD}/logs:/app/logs \
  -v ${PWD}/tmp:/app/tmp \
  -v ${PWD}/metadata:/app/metadata \
  --name rockcapture \
  rockcapture
```

### Run with Docker Compose

```bash
docker compose up --build
```

참고:

- `pipeline.yaml`은 이미지에 포함하지 않고 호스트에서 마운트합니다.
- PySpark 실행을 위해 Java 17이 이미지에 포함됩니다.
- `logs/`, `tmp/`, `metadata/`는 컨테이너 외부와 연결되어 로그와 임시 파일을 유지합니다.
- Docker 이미지 내부에서는 `SPARK_LOCAL_HOSTNAME=localhost`, `SPARK_LOCAL_IP=127.0.0.1`를 기본 설정해 리눅스 컨테이너 환경에서 Spark hostname 이슈를 줄였습니다.

## API

### `POST /pipeline/run`

전체 파이프라인 Job을 시작합니다.

요청 예시:

```json
{
  "db_type": "mysql",
  "host": "10.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "secret",
  "pg_database": "postgres",
  "mysql_jdbc_jar": "drivers/mysql-connector-j-9.2.0.jar",
  "pg_jdbc_jar": "drivers/postgresql-42.7.9.jar",
  "config_file": "pipeline.yaml",
  "single_shot": false
}
```

설명:

- `db_type`은 `mysql`, `postgresql`을 지원합니다.
- `pg_database`는 PostgreSQL일 때만 사용합니다.
- `single_shot=true`이면 대용량 테이블도 배치 분할 없이 한 번에 읽습니다.

### `POST /pipeline/run-filtered`

조건 기반 단일 테이블 스냅샷 Job을 시작합니다.

요청 예시:

```json
{
  "db_type": "postgresql",
  "host": "10.0.0.2",
  "port": 5432,
  "user": "postgres",
  "password": "secret",
  "pg_database": "appdb",
  "config_file": "pipeline.yaml",
  "source_db": "public",
  "source_table": "orders",
  "filter_group": {
    "logic": "and",
    "conditions": [
      { "field": "status", "op": "eq", "value": "paid" },
      { "field": "created_at", "op": "gte", "value": "2026-01-01" }
    ],
    "groups": []
  }
}
```

지원하는 필터 연산자:

- `eq`
- `ne`
- `gt`
- `gte`
- `lt`
- `lte`
- `like`
- `in`
- `between`
- `is_null`
- `is_not_null`

### `GET /pipeline/status?job_id=...`

특정 Job의 상태를 조회합니다.

### `GET /pipeline/jobs`

현재까지 등록된 Job 목록을 조회합니다.

### `POST /pipeline/cancel?job_id=...`

실행 중인 Job에 취소 요청을 전달합니다.

### `GET /health`

기본 헬스체크 응답을 반환합니다.

## Load Strategy

RockCapture는 테이블별 row 수와 PK 정보에 따라 읽기 전략을 다르게 선택합니다.

- 숫자형 PK가 있는 경우
  - Spark JDBC partition read 사용
  - 대용량 테이블이면 PK 범위 기준 배치 적재 수행
- MySQL에서 숫자형 PK는 없지만 문자열 PK가 있는 경우
  - 해시 버킷 방식으로 분할 적재
- PostgreSQL에서 숫자형 PK가 없는 경우
  - `ctid` 페이지 범위 기반 적재 수행
  - 큰 테이블은 여러 `ctid` 배치로 나누어 적재

배치 적재가 발생한 테이블은 후속으로 Iceberg compaction을 수행합니다.

## Iceberg Layout

Iceberg 테이블은 설정한 warehouse 경로 아래에 다음 구조로 저장됩니다.

- MySQL
  - `warehouse/mysql/<database>/<table>`
- PostgreSQL
  - `warehouse/postgresql/<pg_database>/<schema>/<table>`

## Logs

- 실행 로그는 `logs/` 디렉터리에 저장됩니다.
- 애플리케이션 실행마다 로그 파일이 새로 생성됩니다.
- Spark, Py4J 로그는 과도한 출력이 줄어들도록 조정되어 있습니다.

## Current Limits

- Job 상태는 프로세스 메모리에만 저장되므로 서버 재시작 시 이력이 사라집니다.
- `pipeline.yaml`은 로컬 파일 또는 Docker 볼륨 마운트로 제공해야 합니다.
- 현재 구조는 단일 API 인스턴스 기준이며 분산 제어 저장소는 포함하지 않습니다.
