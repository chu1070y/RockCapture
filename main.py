"""
main.py — LakeHouse Pipeline FastAPI 서버

엔드포인트:
    POST /pipeline/run           파이프라인 실행 (백그라운드, job_id 반환)
    GET  /pipeline/status/{id}   실행 상태 조회
    GET  /pipeline/jobs          전체 job 목록 조회
    GET  /health                 헬스체크
"""
import threading
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field

from core.lakehouse import LakehousePipeline
from core.logger import get_logger

log = get_logger(__name__)

# ── FastAPI 앱 ────────────────────────────────────────────────────

app = FastAPI(
    title="LakeHouse Pipeline API",
    description="MySQL / PostgreSQL → MinIO(Iceberg) 파이프라인 실행 API",
    version="1.0.0",
)

# ── Job 상태 관리 ─────────────────────────────────────────────────


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED  = "failed"


_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()


# ── 요청 / 응답 모델 ──────────────────────────────────────────────


class PipelineRequest(BaseModel):
    # ── DB 접속 정보 (필수) ──────────────────────────────────────
    db_type:  str = Field(..., description="DB 종류: 'mysql' 또는 'postgresql'")
    host:     str = Field(..., description="DB 서버 호스트 (IP 또는 도메인)")
    port:     int = Field(..., description="DB 포트 번호")
    user:     str = Field(..., description="DB 접속 계정")
    password: str = Field(..., description="DB 접속 비밀번호")

    # ── PostgreSQL 전용 ───────────────────────────────────────────
    pg_database: str = Field(
        "postgres",
        description="접속할 PostgreSQL 데이터베이스 이름 (postgresql 전용)",
    )

    # ── JDBC JAR 경로 ─────────────────────────────────────────────
    mysql_jdbc_jar: str = Field(
        "drivers/mysql-connector-j-9.2.0.jar",
        description="MySQL JDBC 드라이버 JAR 경로",
    )
    pg_jdbc_jar: str = Field(
        "drivers/postgresql-42.7.9.jar",
        description="PostgreSQL JDBC 드라이버 JAR 경로",
    )

    # ── 설정 파일 ─────────────────────────────────────────────────
    config_file: str = Field(
        "pipeline.yaml",
        description="MinIO / Spark / Iceberg / Pipeline 설정 YAML 경로",
    )


class RunResponse(BaseModel):
    job_id:  str
    message: str


class JobInfo(BaseModel):
    job_id:     str
    status:     JobStatus
    db_type:    str
    host:       str
    started_at: str
    elapsed:    Optional[str] = None
    error:      Optional[str] = None


# ── 백그라운드 실행 함수 ──────────────────────────────────────────


def _run_pipeline_job(job_id: str, req: PipelineRequest) -> None:
    with _jobs_lock:
        _jobs[job_id]["status"] = JobStatus.RUNNING

    try:
        pipeline = LakehousePipeline(
            db_type=req.db_type,
            host=req.host,
            port=req.port,
            user=req.user,
            password=req.password,
            pg_database=req.pg_database,
            mysql_jdbc_jar=req.mysql_jdbc_jar,
            pg_jdbc_jar=req.pg_jdbc_jar,
            config_file=req.config_file,
        )
        elapsed_str = pipeline.run()

        with _jobs_lock:
            _jobs[job_id]["status"]  = JobStatus.SUCCESS
            _jobs[job_id]["elapsed"] = elapsed_str

    except Exception as e:
        log.exception("Job %s 실패: %s", job_id, e)
        with _jobs_lock:
            _jobs[job_id]["status"] = JobStatus.FAILED
            _jobs[job_id]["error"]  = str(e)


# ── 엔드포인트 ────────────────────────────────────────────────────


@app.post(
    "/pipeline/run",
    response_model=RunResponse,
    summary="파이프라인 실행",
    description="DB → MinIO(Iceberg) 파이프라인을 백그라운드로 실행하고 job_id를 반환합니다.",
)
async def run_pipeline(req: PipelineRequest, background_tasks: BackgroundTasks):
    if req.db_type.lower() not in LakehousePipeline.SUPPORTED_DB_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"지원하지 않는 db_type: '{req.db_type}'. 사용 가능: mysql, postgresql",
        )

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id":     job_id,
            "status":     JobStatus.PENDING,
            "db_type":    req.db_type.lower(),
            "host":       req.host,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "elapsed":    None,
            "error":      None,
        }

    background_tasks.add_task(_run_pipeline_job, job_id, req)

    log.info("Job 등록  (job_id=%s, db=%s, host=%s)", job_id, req.db_type, req.host)
    return RunResponse(job_id=job_id, message="파이프라인이 백그라운드에서 시작되었습니다.")


@app.get(
    "/pipeline/status/{job_id}",
    response_model=JobInfo,
    summary="Job 상태 조회",
)
async def get_status(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"job_id '{job_id}' 를 찾을 수 없습니다.")
    return JobInfo(**job)


@app.get(
    "/pipeline/jobs",
    response_model=list[JobInfo],
    summary="전체 Job 목록 조회",
)
async def list_jobs():
    with _jobs_lock:
        return [JobInfo(**j) for j in _jobs.values()]


@app.get("/health", summary="헬스체크")
async def health():
    return {"status": "ok"}


# ── 실행 진입점 ───────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
