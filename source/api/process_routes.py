from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from models.process import JobProcessRequest
from services.file_input_service import FileInputService
from services.job_process_service import JobProcessService
from utils.logging import get_logger, log_event

router = APIRouter()
job_process_service = JobProcessService()
file_input_service = FileInputService()
logger = get_logger("process_routes")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _downloadable_json_response(payload: Any, filename: str) -> StreamingResponse:
    content = json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default)
    return StreamingResponse(
        iter([content.encode("utf-8")]),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _downloadable_csv_response(rows: list[dict[str, Any]], filename: str) -> StreamingResponse:
    output = io.StringIO()
    fieldnames = list(rows[0].keys()) if rows else []
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    if fieldnames:
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _json_default(value) for key, value in row.items()})
    content = output.getvalue()
    output.close()
    return StreamingResponse(
        iter([content.encode("utf-8")]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _safe_filename(value: str) -> str:
    return "".join(character if character.isalnum() or character in {"-", "_", "."} else "_" for character in value)


def _flatten_csv_list(values: Any) -> str:
    flattened: list[str] = []
    for item in list(values or []):
        if isinstance(item, (dict, list)):
            flattened.append(json.dumps(item, ensure_ascii=False, default=_json_default))
        elif item is not None:
            flattened.append(str(item))
    return " | ".join(value for value in flattened if value)


def _extract_ats_reason(ats_detection: dict[str, Any]) -> str | None:
    return (
        ats_detection.get("reasoning")
        or ats_detection.get("non_ats_reason")
        or ats_detection.get("detection_reason")
    )


def _build_client_summary_export(client_name: str, overview: dict[str, Any]) -> dict[str, Any]:
    process_runs = list(overview.get("process_runs") or [])
    return {
        "client_key": overview.get("client_key"),
        "client_name": client_name,
        "process_count": len(process_runs),
        "runs": [
            {
                "process_id": process.get("process_id"),
                "client_key": process.get("client_key"),
                "client_name": process.get("client_name"),
                "status": process.get("status"),
                "summary": process.get("summary") or {},
                "metadata": process.get("metadata") or {},
                "created_at": process.get("created_at"),
                "updated_at": process.get("updated_at"),
                "started_at": process.get("started_at"),
                "completed_at": process.get("completed_at"),
            }
            for process in process_runs
        ],
    }


def _build_process_item_important_export(process: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    result_payload = dict(item.get("result_payload") or {})
    career_page_result = dict(result_payload.get("career_page_result") or {})
    ats_detection = dict(result_payload.get("ats_detection") or {})
    return {
        "process_id": process.get("process_id"),
        "client_key": process.get("client_key"),
        "client_name": process.get("client_name"),
        "raw_url": item.get("raw_url"),
        "domain_key": item.get("domain_key"),
        "requested_capability": item.get("requested_capability"),
        "status": item.get("status"),
        "error": item.get("error"),
        "agent_index": item.get("agent_index"),
        "result_summary": item.get("result_summary") or {},
        "career_page_overview": career_page_result.get("overview") or {},
        "ats_summary": {
            "ats_detected": ats_detection.get("ats_detected"),
            "ats_provider": ats_detection.get("ats_provider"),
            "confidence": ats_detection.get("confidence"),
            "detection_method": ats_detection.get("detection_method"),
            "reason": _extract_ats_reason(ats_detection),
        },
    }


def _build_process_important_export(process: dict[str, Any]) -> dict[str, Any]:
    items = list(process.get("items") or [])
    return {
        "process_id": process.get("process_id"),
        "client_key": process.get("client_key"),
        "client_name": process.get("client_name"),
        "status": process.get("status"),
        "summary": process.get("summary") or {},
        "metadata": process.get("metadata") or {},
        "created_at": process.get("created_at"),
        "updated_at": process.get("updated_at"),
        "started_at": process.get("started_at"),
        "completed_at": process.get("completed_at"),
        "domains": [_build_process_item_important_export(process, item) for item in items],
    }


def _build_process_important_csv_rows(process: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in list(process.get("items") or []):
        result_payload = dict(item.get("result_payload") or {})
        career_page_result = dict(result_payload.get("career_page_result") or {})
        ats_detection = dict(result_payload.get("ats_detection") or {})
        result_summary = dict(item.get("result_summary") or {})
        career_overview = dict(career_page_result.get("overview") or {})
        rows.append(
            {
                "process_id": process.get("process_id"),
                "client_name": process.get("client_name"),
                "raw_url": item.get("raw_url"),
                "requested_capability": item.get("requested_capability"),
                "status": item.get("status"),
                "result_summary_career_url_status": result_summary.get("career_url_status"),
                "result_summary_career_url_count": result_summary.get("career_url_count"),
                "career_page_overview_outcome": career_overview.get("outcome"),
                "career_page_overview_outcome_reason": career_overview.get("outcome_reason"),
                "career_page_overview_jobs_found": career_overview.get("jobs_found"),
                "career_page_overview_total_jobs_found": career_overview.get("total_jobs_found"),
                "career_page_overview_job_alert": career_overview.get("job_alert"),
                "career_page_overview_job_alert_note": career_overview.get("job_alert_note"),
                "career_page_overview_career_page_confirmed": career_overview.get("career_page_confirmed"),
                "career_page_overview_total_urls_processed": career_overview.get("total_urls_processed"),
                "career_page_overview_job_urls": _flatten_csv_list(career_overview.get("job_urls")),
                "career_page_overview_job_found_on_urls": _flatten_csv_list(career_overview.get("job_found_on_urls")),
                "career_page_overview_job_alert_urls": _flatten_csv_list(career_overview.get("job_alert_urls")),
                "ats_detected": ats_detection.get("ats_detected"),
                "ats_provider": ats_detection.get("ats_provider"),
                "ats_confidence": ats_detection.get("confidence"),
                "ats_reason": _extract_ats_reason(ats_detection),
            }
        )
    return rows


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
        client_name=request.client_name,
        url_count=len(request.urls),
        agent_count=request.agent_count,
        job_monitoring=request.job_monitoring,
        ats_check=request.ats_check,
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
    client_name: str = Form("default_client"),
    agent_count: int = Form(1),
    grid_url: str | None = Form(None),
    ats_check: bool = Form(True),
    job_monitoring: bool = Form(False),
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
            upload_filename=file.filename,
            error=str(exc),
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    request = JobProcessRequest(
        client_name=client_name,
        urls=urls,
        agent_count=agent_count,
        grid_url=grid_url,
        ats_check=ats_check,
        job_monitoring=job_monitoring,
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
        upload_filename=file.filename,
        client_name=client_name,
        url_count=len(urls),
        agent_count=agent_count,
        ats_check=ats_check,
        job_monitoring=job_monitoring,
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
async def get_process(process_id: str) -> StreamingResponse:
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
    return _downloadable_json_response(process, f"process_{_safe_filename(process_id)}.json")


@router.get("/clients/{client_name}")
async def get_client_overview(client_name: str) -> StreamingResponse:
    log_event(
        logger,
        "info",
        "client_overview_requested client_name=%s",
        client_name,
        domain=client_name,
        client_name=client_name,
    )
    overview = await job_process_service.get_client_overview(client_name)
    return _downloadable_json_response(
        overview,
        f"client_{_safe_filename(client_name)}.json",
    )


@router.get("/clients/{client_name}/summary")
async def get_client_summary(client_name: str) -> StreamingResponse:
    log_event(
        logger,
        "info",
        "client_summary_requested client_name=%s",
        client_name,
        domain=client_name,
        client_name=client_name,
    )
    overview = await job_process_service.get_client_overview(client_name)
    summary_payload = _build_client_summary_export(client_name, overview)
    return _downloadable_json_response(
        summary_payload,
        f"client_{_safe_filename(client_name)}_summary.json",
    )


@router.get("/processes/{process_id}/important")
async def get_process_important_json(process_id: str) -> StreamingResponse:
    log_event(
        logger,
        "info",
        "process_important_report_requested process_id=%s",
        process_id,
        domain="api",
        process_id=process_id,
    )
    process = await job_process_service.get_process(process_id)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    export_payload = _build_process_important_export(process)
    return _downloadable_json_response(
        export_payload,
        f"process_{_safe_filename(process_id)}_important.json",
    )


@router.get("/processes/{process_id}/important.csv")
async def get_process_important_csv(process_id: str) -> StreamingResponse:
    log_event(
        logger,
        "info",
        "process_important_csv_requested process_id=%s",
        process_id,
        domain="api",
        process_id=process_id,
    )
    process = await job_process_service.get_process(process_id)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    rows = _build_process_important_csv_rows(process)
    return _downloadable_csv_response(
        rows,
        f"process_{_safe_filename(process_id)}_important.csv",
    )
