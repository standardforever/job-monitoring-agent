from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile

from models.process import JobProcessRequest
from services.file_input_service import FileInputService
from services.job_process_service import JobProcessService
from utils.logging import get_logger, log_event

router = APIRouter()
job_process_service = JobProcessService()
file_input_service = FileInputService()
logger = get_logger("process_routes")


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    log_event(logger, "info", "healthcheck_requested", domain="api")
    return {"status": "ok"}


@router.post("/processes")
async def create_process(
    request: JobProcessRequest,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    log_event(
        logger,
        "info",
        "process_create_requested url_count=%s agent_count=%s",
        len(request.urls),
        request.agent_count,
        domain=request.urls[0] if request.urls else "unknown",
        url_count=len(request.urls),
        agent_count=request.agent_count,
    )
    process_document = await job_process_service.submit_process(request)
    background_tasks.add_task(
        job_process_service.execute_existing_process,
        process_document["process_id"],
    )
    log_event(
        logger,
        "info",
        "process_created process_id=%s status=%s",
        process_document["process_id"],
        process_document["status"],
        domain=request.urls[0] if request.urls else "unknown",
        process_id=process_document["process_id"],
        status=process_document["status"],
    )
    return {
        "process_id": process_document["process_id"],
        "status": process_document["status"],
    }


@router.post("/processes/upload")
async def create_process_from_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    agent_count: int = Form(1),
    grid_url: str | None = Form(None),
    ats_check: bool = Form(True),
    task_id: str | None = Form(None),
) -> dict[str, str]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Uploaded file must have a filename")

    content = await file.read()
    try:
        urls = file_input_service.extract_domains(file.filename, content)
    except ValueError as exc:
        log_event(
            logger,
            "warning",
            "process_upload_invalid_file filename=%s error=%s",
            file.filename,
            str(exc),
            domain=file.filename,
            filename=file.filename,
            error=str(exc),
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    request = JobProcessRequest(
        urls=urls,
        agent_count=agent_count,
        grid_url=grid_url,
        ats_check=ats_check,
        task_id=task_id,
    )
    log_event(
        logger,
        "info",
        "process_upload_requested filename=%s url_count=%s agent_count=%s",
        file.filename,
        len(urls),
        agent_count,
        domain=urls[0] if urls else file.filename,
        filename=file.filename,
        url_count=len(urls),
        agent_count=agent_count,
        ats_check=ats_check,
    )
    process_document = await job_process_service.submit_process(request)
    background_tasks.add_task(
        job_process_service.execute_existing_process,
        process_document["process_id"],
    )
    log_event(
        logger,
        "info",
        "process_upload_created process_id=%s status=%s",
        process_document["process_id"],
        process_document["status"],
        domain=urls[0] if urls else file.filename,
        process_id=process_document["process_id"],
        status=process_document["status"],
    )
    return {
        "process_id": process_document["process_id"],
        "status": process_document["status"],
    }


@router.get("/processes/{process_id}")
async def get_process(process_id: str) -> dict:
    log_event(
        logger,
        "info",
        "process_fetch_requested process_id=%s",
        process_id,
        domain="api",
        process_id=process_id,
    )
    process = await job_process_service.get_process(process_id)
    if process is None:
        log_event(
            logger,
            "warning",
            "process_not_found process_id=%s",
            process_id,
            domain="api",
            process_id=process_id,
        )
        raise HTTPException(status_code=404, detail="Process not found")
    log_event(
        logger,
        "info",
        "process_fetch_completed process_id=%s status=%s",
        process_id,
        process.get("status"),
        domain=(((process.get("request") or {}).get("urls") or ["unknown"])[0]),
        process_id=process_id,
        status=process.get("status"),
    )
    return process
