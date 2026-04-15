# LakeHouse

MySQL 데이터를 MinIO(S3 호환) 오브젝트 스토리지에 Apache Iceberg 형식으로 적재하는 데이터 파이프라인입니다.

## 주요 기능

- MySQL 전체 스냅숏을 일관된 시점(FTWRL + binlog 위치)으로 캡처
- Spark JDBC를 이용한 멀티스레드 병렬 적재
- 대형 테이블 PK 범위 분할 및 청크 단위 처리
- Apache Iceberg 카탈로그로 MinIO에 저장
- 스냅숏 메타데이터 및 binlog 위치 자동 기록

## 기술 스택

| 구성 요소 | 역할 |
|-----------|------|
| Python 3.10+ | 파이프라인 오케스트레이션 |
| Apache Spark | JDBC 병렬 데이터 적재 |
| Apache Iceberg | 테이블 포맷 (스키마 진화, 타임트래블 지원) |
| MinIO | S3 호환 오브젝트 스토리지 |
| MySQL / MariaDB | 원본 데이터 소스 |

## 빠른 시작

> Python 3.10 이상이 필요합니다. (개발 환경: Python 3.10.11)

**1. 의존성 설치**
```bash
pip install -r requirements.txt
```

**2. 설정 파일 작성**
```bash
cp pipeline.yaml.example pipeline.yaml
# pipeline.yaml 에서 MySQL, MinIO 접속 정보 수정
```

**3. 파이프라인 실행**
```bash
python main.py
```

## 설정 (`pipeline.yaml`)

| 항목 | 설명 |
|------|------|
| `mysql` | 호스트, 포트, 인증 정보, JDBC 드라이버 경로 |
| `minio` | 엔드포인트, 인증 정보, 버킷 이름 |
| `spark` | 앱 이름, 드라이버 메모리 |
| `iceberg` | 카탈로그 이름, 웨어하우스 경로 |
| `pipeline` | 스레드 수, 청크 크기, 대형 테이블 임계값 |

## 프로젝트 구조

```
LakeHouse/
├── main.py               # 진입점
├── pipeline.yaml         # 실행 설정 (gitignore 권장)
├── pipeline.yaml.example # 설정 예시
├── core/
│   ├── config.py         # 설정 데이터 클래스
│   ├── logger.py         # 로깅 설정
│   └── pipeline_runner.py# 병렬 적재 로직
├── connectors/
│   ├── mysql_connector.py # MySQL 스냅숏 및 통계 수집
│   ├── minio_connector.py # MinIO 업로드
│   ├── spark_session.py  # Spark + Iceberg 세션 관리
│   └── metadata_writer.py# 메타데이터 저장
└── drivers/              # JDBC 드라이버 JAR
```
