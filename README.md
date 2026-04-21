# RockCapture

MySQL / PostgreSQL 데이터베이스의 전체 스냅숏을 일관된 시점으로 캡처하여, Apache Spark를 통해 MinIO(S3 호환 오브젝트 스토리지)에 Apache Iceberg 포맷으로 적재하는 데이터 파이프라인 시스템입니다.

FastAPI 기반 REST API 서버로 동작하며, 파이프라인 실행·상태 조회·중단을 HTTP 요청으로 제어할 수 있습니다.

## 주요 기능

- MySQL / PostgreSQL 전체 스냅숏을 일관된 시점으로 캡처
  - MySQL: `FLUSH TABLES WITH READ LOCK` + binlog 위치 기록
  - PostgreSQL: `REPEATABLE READ` + `pg_export_snapshot()` + WAL LSN 기록
- Spark JDBC 멀티스레드 병렬 적재 (소형·대형 테이블 동시 실행)
- 대형 테이블 자동 분류 및 배치 전략 선택
  - 숫자형 PK: PK 범위 분할 배치
  - 텍스트형 PK (MySQL): CRC32 해시 배치
  - PK 없음 (PostgreSQL): ctid 페이지 범위 배치
- Apache Iceberg `rewrite_data_files` compaction으로 소형 파일 병합
- 스냅숏 메타데이터(binlog 위치 / WAL LSN) 자동 MinIO 저장
- REST API를 통한 비동기 Job 관리 (실행 / 상태 조회 / 취소)

## 기술 스택

| 구성 요소 | 버전 | 역할 |
|-----------|------|------|
| Python | 3.10+ | 파이프라인 오케스트레이션 |
| FastAPI + uvicorn | 0.135.3 / 0.44.0 | REST API 서버 |
| PySpark | 4.1.1 | JDBC 병렬 데이터 적재 엔진 |
| Apache Iceberg | 1.10.1 | 테이블 포맷 (스키마 진화, 타임트래블 지원) |
| MinIO | S3A | S3 호환 오브젝트 스토리지 |
| MySQL / MariaDB | - | 원본 데이터 소스 (PyMySQL) |
| PostgreSQL | - | 원본 데이터 소스 (psycopg2) |
| hadoop-aws | 3.4.1 | S3A 파일시스템 연동 |

## 빠른 시작

> Python 3.10 이상이 필요합니다.

**1. 의존성 설치**
```bash
pip install -r requirements.txt
```

**2. JDBC 드라이버 확인**

`drivers/` 디렉터리에 아래 JAR 파일이 있어야 합니다.
```
drivers/
├── mysql-connector-j-9.2.0.jar
└── postgresql-42.7.9.jar
```

**3. 설정 파일 작성**
```bash
cp pipeline.yaml.example pipeline.yaml
# pipeline.yaml 에서 MinIO 접속 정보 수정
```

**4. API 서버 실행**
```bash
python main.py
# → http://localhost:8000 에서 서버 시작
# → http://localhost:8000/docs 에서 Swagger UI 확인
```

**5. 파이프라인 실행 (API 호출 예시)**
```bash
# MySQL
curl -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{
    "db_type": "mysql",
    "host": "10.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "secret"
  }'

# PostgreSQL
curl -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{
    "db_type": "postgresql",
    "host": "10.0.0.2",
    "port": 5432,
    "user": "postgres",
    "password": "secret",
    "pg_database": "mydb"
  }'
```

## REST API

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/pipeline/run` | 파이프라인 실행 (백그라운드, job_id 반환) |
| `GET` | `/pipeline/status?job_id=` | Job 상태 조회 |
| `GET` | `/pipeline/jobs` | 전체 Job 목록 조회 |
| `POST` | `/pipeline/cancel?job_id=` | 실행 중인 Job graceful 중단 |
| `GET` | `/health` | 헬스체크 |

Job 상태값: `pending` → `running` → `success` / `failed` / `cancelled`

`/pipeline/run` 주요 요청 파라미터:

| 파라미터 | 필수 | 기본값 | 설명 |
|----------|------|--------|------|
| `db_type` | ✅ | - | `"mysql"` 또는 `"postgresql"` |
| `host` | ✅ | - | DB 서버 호스트 |
| `port` | ✅ | - | DB 포트 번호 |
| `user` | ✅ | - | DB 접속 계정 |
| `password` | ✅ | - | DB 접속 비밀번호 |
| `pg_database` | - | `"postgres"` | PostgreSQL 전용 데이터베이스명 |
| `config_file` | - | `"pipeline.yaml"` | 설정 YAML 경로 |
| `single_shot` | - | `false` | `true`이면 대형 테이블도 배치 분할 없이 한 번에 적재 |

## 설정 (`pipeline.yaml`)

```yaml
minio:
  endpoint: http://10.0.0.1:9000   # MinIO 서버 주소
  access_key: admin
  secret_key: password
  bucket: my-bucket
  region: us-east-1
  ssl_enabled: false

spark:
  app_name: LakeHouse
  log_level: INFO                   # DEBUG / INFO / WARNING / ERROR
  driver_memory: 3g

iceberg:
  catalog_name: lakehouse
  warehouse: s3a://my-bucket/iceberg-warehouse

logging:
  level: INFO

pipeline:
  num_threads: 4                    # 소형 테이블 동시 적재 워커 수
  chunk_size: 100000                # JDBC 파티션당 행 수
  large_table_threshold: 1000000   # 이 행 수 이상이면 대형 테이블로 분류
  large_table_batch_size: 1000000  # 대형 테이블 배치 단위 크기
  large_table_workers: 3           # 대형 테이블 동시 처리 워커 수
```

## Iceberg 저장 경로

```
s3a://<bucket>/iceberg-warehouse/
├── mysql/
│   └── <database>/
│       └── <table>/
└── postgresql/
    └── <pg_database>/
        └── <schema>/
            └── <table>/
```

## 프로젝트 구조

```
RockCapture/
├── main.py                     # FastAPI 서버, Job 관리, API 엔드포인트
├── pipeline.yaml               # 실행 설정 (gitignore 권장)
├── pipeline.yaml.example       # 설정 예시
├── requirements.txt
├── core/
│   ├── config.py               # 설정 데이터클래스 (MinIO / Spark / Iceberg / Pipeline / DB)
│   ├── lakehouse.py            # LakehousePipeline — 파이프라인 5단계 오케스트레이션
│   ├── logger.py               # 로깅 설정
│   └── pipeline_runner.py      # Spark JDBC 병렬 적재 엔진 (TableTask, 배치 전략, compaction)
├── connectors/
│   ├── base_connector.py       # DB 커넥터 추상 기반 클래스
│   ├── mysql_connector.py      # MySQL 스냅숏 캡처 및 통계 수집
│   ├── postgres_connector.py   # PostgreSQL 스냅숏 캡처 및 통계 수집
│   ├── minio_connector.py      # MinIO Iceberg 테이블 적재 (write / append / compact)
│   ├── spark_session.py        # SparkSession 생성 및 생명주기 관리
│   └── metadata_writer.py      # 스냅숏 메타데이터 MinIO 저장
└── drivers/
    ├── mysql-connector-j-9.2.0.jar
    └── postgresql-42.7.9.jar
```
