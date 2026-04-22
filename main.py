"""
FastAPI entrypoint for the LakeHouse pipeline service.

Endpoints
    POST /pipeline/run
    POST /pipeline/cancel?job_id=
    GET  /pipeline/status?job_id=
    GET  /pipeline/jobs
    GET  /health
"""

import multiprocessing as mp
import threading
import traceback
import uuid
from datetime import datetime
from enum import Enum
from queue import Empty
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from core.lakehouse import LakehousePipeline
from core.logger import get_logger

log = get_logger(__name__)

app = FastAPI(
    title="LakeHouse Pipeline API",
    description="MySQL / PostgreSQL to MinIO(Iceberg) pipeline API",
    version="1.0.0",
)

_MP_CONTEXT = mp.get_context("spawn")


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


_jobs: dict[str, dict[str, Any]] = {}
_jobs_lock = threading.Lock()
_cancel_events: dict[str, Any] = {}
_job_processes: dict[str, Any] = {}


class PipelineRequest(BaseModel):
    db_type: str = Field(..., description="Database type: mysql or postgresql")
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port")
    user: str = Field(..., description="Database user")
    password: str = Field(..., description="Database password")
    pg_database: str = Field(
        "postgres",
        description="PostgreSQL database name",
    )
    mysql_jdbc_jar: str = Field(
        "drivers/mysql-connector-j-9.2.0.jar",
        description="MySQL JDBC driver jar path",
    )
    pg_jdbc_jar: str = Field(
        "drivers/postgresql-42.7.9.jar",
        description="PostgreSQL JDBC driver jar path",
    )
    config_file: str = Field(
        "pipeline.yaml",
        description="Pipeline YAML config path",
    )
    single_shot: bool = Field(
        False,
        description="Load large tables without batch splitting",
    )


class RunResponse(BaseModel):
    job_id: str
    message: str


class JobInfo(BaseModel):
    job_id: str
    status: JobStatus
    db_type: str
    host: str
    started_at: str
    elapsed: Optional[str] = None
    error: Optional[str] = None
    cancelled_at: Optional[str] = None


class FilteredSnapshotRequest(PipelineRequest):
    source_db: str = Field(
        ...,
        description="MySQL database name or PostgreSQL schema name",
    )
    source_table: str = Field(..., description="Source table name")
    where_clause: str = Field(..., description="SQL WHERE clause without the WHERE keyword")


def _build_pipeline(req_data: dict[str, Any]) -> LakehousePipeline:
    return LakehousePipeline(
        db_type=req_data["db_type"],
        host=req_data["host"],
        port=req_data["port"],
        user=req_data["user"],
        password=req_data["password"],
        pg_database=req_data["pg_database"],
        mysql_jdbc_jar=req_data["mysql_jdbc_jar"],
        pg_jdbc_jar=req_data["pg_jdbc_jar"],
        config_file=req_data["config_file"],
        single_shot=req_data["single_shot"],
    )


def _run_pipeline_process(
    req_data: dict[str, Any],
    cancel_event: Any,
    result_queue: Any,
) -> None:
    try:
        pipeline = _build_pipeline(req_data)
        job_type = req_data.get("job_type", "full")
        if job_type == "filtered_snapshot":
            elapsed_str = pipeline.run_filtered_snapshot(
                source_db=req_data["source_db"],
                source_table=req_data["source_table"],
                where_clause=req_data["where_clause"],
                cancel_event=cancel_event,
            )
        else:
            elapsed_str = pipeline.run(cancel_event=cancel_event)

        if cancel_event.is_set():
            result_queue.put({"status": JobStatus.CANCELLED.value})
        else:
            result_queue.put(
                {
                    "status": JobStatus.SUCCESS.value,
                    "elapsed": elapsed_str,
                }
            )
    except BaseException as exc:  # noqa: BLE001
        if cancel_event.is_set():
            result_queue.put({"status": JobStatus.CANCELLED.value})
            return

        result_queue.put(
            {
                "status": JobStatus.FAILED.value,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise
    finally:
        result_queue.close()
        result_queue.join_thread()


def _finalize_job(job_id: str, result: dict[str, Any], exit_code: int | None) -> None:
    status = result.get("status")
    error = result.get("error")

    if status is None:
        if exit_code == 0:
            status = JobStatus.SUCCESS.value
        else:
            status = JobStatus.FAILED.value
            error = error or f"Worker process exited with code {exit_code}"

    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            return

        if status == JobStatus.SUCCESS.value:
            job["status"] = JobStatus.SUCCESS
            job["elapsed"] = result.get("elapsed")
            job["error"] = None
        elif status == JobStatus.CANCELLED.value:
            job["status"] = JobStatus.CANCELLED
        else:
            job["status"] = JobStatus.FAILED
            job["error"] = error


def _watch_pipeline_job(job_id: str, process: Any, result_queue: Any) -> None:
    result: dict[str, Any] = {}

    try:
        process.join()

        try:
            result = result_queue.get_nowait()
        except Empty:
            result = {}

        if result.get("traceback"):
            log.error("Job %s failed in worker process\n%s", job_id, result["traceback"])

        _finalize_job(job_id, result, process.exitcode)
    finally:
        with _jobs_lock:
            _cancel_events.pop(job_id, None)
            _job_processes.pop(job_id, None)

        try:
            result_queue.close()
        except Exception:  # noqa: BLE001
            pass

        try:
            process.close()
        except Exception:  # noqa: BLE001
            pass


def _start_pipeline_job(job_id: str, req_data: dict[str, Any]) -> None:
    cancel_event = _MP_CONTEXT.Event()
    result_queue = _MP_CONTEXT.Queue()
    process = _MP_CONTEXT.Process(
        target=_run_pipeline_process,
        args=(req_data, cancel_event, result_queue),
        name=f"lakehouse-job-{job_id}",
    )

    with _jobs_lock:
        _jobs[job_id]["status"] = JobStatus.RUNNING
        _cancel_events[job_id] = cancel_event
        _job_processes[job_id] = process

    try:
        process.start()
    except Exception as exc:  # noqa: BLE001
        with _jobs_lock:
            _jobs[job_id]["status"] = JobStatus.FAILED
            _jobs[job_id]["error"] = str(exc)
            _cancel_events.pop(job_id, None)
            _job_processes.pop(job_id, None)

        try:
            result_queue.close()
        except Exception:  # noqa: BLE001
            pass
        raise

    watcher = threading.Thread(
        target=_watch_pipeline_job,
        args=(job_id, process, result_queue),
        name=f"job-watcher-{job_id}",
        daemon=True,
    )
    watcher.start()


@app.post(
    "/pipeline/run",
    response_model=RunResponse,
    summary="Run pipeline",
)
async def run_pipeline(req: PipelineRequest):
    if req.db_type.lower() not in LakehousePipeline.SUPPORTED_DB_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported db_type: '{req.db_type}'. Use mysql or postgresql.",
        )

    job_id = str(uuid.uuid4())
    req_data = req.model_dump()
    req_data["job_type"] = "full"

    with _jobs_lock:
        running = next(
            (
                job for job in _jobs.values()
                if job["status"] in (JobStatus.PENDING, JobStatus.RUNNING)
                and job["db_type"] == req.db_type.lower()
                and job["host"] == req.host
            ),
            None,
        )
        if running:
            raise HTTPException(
                status_code=409,
                detail=f"A job is already running for that target. (job_id={running['job_id']})",
            )

        _jobs[job_id] = {
            "job_id": job_id,
            "status": JobStatus.PENDING,
            "db_type": req.db_type.lower(),
            "host": req.host,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "elapsed": None,
            "error": None,
            "cancelled_at": None,
        }

    _start_pipeline_job(job_id, req_data)

    log.info("Job registered  (job_id=%s, db=%s, host=%s)", job_id, req.db_type, req.host)
    return RunResponse(job_id=job_id, message="Pipeline started in a background process.")


@app.post(
    "/pipeline/run-filtered",
    response_model=RunResponse,
    summary="Run filtered table snapshot",
)
async def run_filtered_snapshot(req: FilteredSnapshotRequest):
    if req.db_type.lower() not in LakehousePipeline.SUPPORTED_DB_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported db_type: '{req.db_type}'. Use mysql or postgresql.",
        )

    job_id = str(uuid.uuid4())
    req_data = req.model_dump()
    req_data["job_type"] = "filtered_snapshot"

    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id,
            "status": JobStatus.PENDING,
            "db_type": req.db_type.lower(),
            "host": req.host,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "elapsed": None,
            "error": None,
            "cancelled_at": None,
        }

    _start_pipeline_job(job_id, req_data)

    log.info(
        "Filtered snapshot job registered  (job_id=%s, db=%s, source=%s.%s)",
        job_id,
        req.db_type,
        req.source_db,
        req.source_table,
    )
    return RunResponse(job_id=job_id, message="Filtered snapshot started in a background process.")


@app.get(
    "/pipeline/status",
    response_model=JobInfo,
    summary="Get job status",
)
async def get_status(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"job_id '{job_id}' was not found.")

    return JobInfo(**job)


@app.get(
    "/pipeline/jobs",
    response_model=list[JobInfo],
    summary="List jobs",
)
async def list_jobs():
    with _jobs_lock:
        return [JobInfo(**job) for job in _jobs.values()]


@app.post(
    "/pipeline/cancel",
    summary="Cancel job",
)
async def cancel_job(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
        event = _cancel_events.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"job_id '{job_id}' was not found.")

    if job["status"] != JobStatus.RUNNING:
        raise HTTPException(
            status_code=409,
            detail=f"job_id '{job_id}' is '{job['status']}'. Only RUNNING jobs can be cancelled.",
        )

    if event is None:
        raise HTTPException(status_code=500, detail="Cancel event was not found for this job.")

    event.set()
    with _jobs_lock:
        _jobs[job_id]["cancelled_at"] = datetime.now().isoformat(timespec="seconds")

    log.info("Job cancel requested  (job_id=%s)", job_id)
    return {"job_id": job_id, "message": "Cancel request delivered."}


@app.get("/health", summary="Health check")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
