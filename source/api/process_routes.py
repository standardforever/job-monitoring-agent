from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from core.config import get_settings
from models.process import ClientRegistrationRequest, ClientUpdateRequest, JobProcessRequest
from services.file_input_service import FileInputService
from services.job_process_service import JobProcessService
from utils.logging import get_logger, log_event

router = APIRouter()
settings = get_settings()
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


def _validate_admin_password(x_registration_password: str | None) -> None:
    configured_password = str(settings.client_registration_password or "").strip()
    provided_password = str(x_registration_password or "").strip()
    
    
    if not configured_password:
        raise HTTPException(status_code=500, detail="Client registration password is not configured")

    if provided_password != configured_password:
        raise HTTPException(status_code=401, detail="Invalid registration password")


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


def _build_client_jobs_csv_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in list(payload.get("jobs") or []):
        job_data = dict(item.get("job_data") or {})
        location = dict(job_data.get("location") or {})
        salary = dict(job_data.get("salary") or {})
        hours = dict(job_data.get("hours") or {})
        closing_date = dict(job_data.get("closing_date") or {})
        interview_date = dict(job_data.get("interview_date") or {})
        start_date = dict(job_data.get("start_date") or {})
        post_date = dict(job_data.get("post_date") or {})
        contact = dict(job_data.get("contact") or {})
        application_method = dict(job_data.get("application_method") or {})

        rows.append(
            {
                "client_name": payload.get("client_name"),
                "domain_key": item.get("domain_key"),
                "raw_url": item.get("raw_url"),
                "process_id": item.get("process_id"),
                "job_key": item.get("job_key"),
                "source_type": item.get("source_type"),
                "source_url": item.get("source_url"),
                "title": item.get("title") or job_data.get("title"),
                "company_name": item.get("company_name") or job_data.get("company_name"),
                "is_job_page": job_data.get("is_job_page"),
                "confidence_reason": job_data.get("confidence_reason"),
                "holiday": job_data.get("holiday"),
                "location_address": location.get("address"),
                "location_city": location.get("city"),
                "location_region": location.get("region"),
                "location_postcode": location.get("postcode"),
                "location_country": location.get("country"),
                "salary_min": salary.get("min"),
                "salary_max": salary.get("max"),
                "salary_currency": salary.get("currency"),
                "salary_period": salary.get("period"),
                "salary_actual": salary.get("actual_salary"),
                "salary_raw_text": salary.get("raw_text_salary"),
                "job_type": job_data.get("job_type"),
                "contract_type": job_data.get("contract_type"),
                "remote_option": job_data.get("remote_option"),
                "hours_weekly": hours.get("weekly"),
                "hours_daily": hours.get("daily"),
                "hours_details": hours.get("details"),
                "closing_date_iso": closing_date.get("iso_format"),
                "closing_date_raw": closing_date.get("raw_text"),
                "interview_date_iso": interview_date.get("iso_format"),
                "interview_date_raw": interview_date.get("raw_text"),
                "start_date_iso": start_date.get("iso_format"),
                "start_date_raw": start_date.get("raw_text"),
                "post_date_iso": post_date.get("iso_format"),
                "post_date_raw": post_date.get("raw_text"),
                "contact_name": contact.get("name"),
                "contact_email": contact.get("email"),
                "contact_phone": contact.get("phone"),
                "job_reference": job_data.get("job_reference"),
                "description": job_data.get("description"),
                "responsibilities": _flatten_csv_list(job_data.get("responsibilities")),
                "requirements": _flatten_csv_list(job_data.get("requirements")),
                "benefits": _flatten_csv_list(job_data.get("benefits")),
                "company_info": job_data.get("company_info"),
                "how_to_apply": job_data.get("how_to_apply"),
                "application_method_type": application_method.get("type"),
                "application_method_url": application_method.get("url"),
                "application_method_email": application_method.get("email"),
                "application_method_instructions": application_method.get("instructions"),
                "additional_sections": json.dumps(
                    job_data.get("additional_sections") or {},
                    ensure_ascii=False,
                    default=_json_default,
                ),
                "page_fingerprint": item.get("page_fingerprint"),
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
            }
        )
    return rows


def _build_process_jobs_csv_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return _build_client_jobs_csv_rows(payload)


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


@router.post("/clients")
async def register_client(
    request: ClientRegistrationRequest,
    x_registration_password: str | None = Header(default=None),
) -> dict[str, Any]:
    _validate_admin_password(x_registration_password)
    log_event(
        logger,
        "info",
        "client_registration_requested client_name=%s model=%s",
        request.client_name,
        request.model,
        domain=request.client_name,
        client_name=request.client_name,
        model=request.model,
    )
    try:
        client = await job_process_service.register_client(
            client_name=request.client_name,
            api_key=request.api_key,
            model=request.model,
            grid_url=request.grid_url,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "status": "ready",
        "client": client,
        "message": "API key is active and ready to use.",
    }


@router.get("/clients")
async def list_clients(
    x_registration_password: str | None = Header(default=None),
) -> dict[str, Any]:
    _validate_admin_password(x_registration_password)
    log_event(logger, "info", "client_list_requested", domain="admin")
    return await job_process_service.list_clients()


@router.patch("/clients/{client_name}/config")
async def update_client(
    client_name: str,
    request: ClientUpdateRequest,
    x_registration_password: str | None = Header(default=None),
) -> dict[str, Any]:
    _validate_admin_password(x_registration_password)
    log_event(
        logger,
        "info",
        "client_update_requested client_name=%s",
        client_name,
        domain=client_name,
        client_name=client_name,
    )
    try:
        client = await job_process_service.update_client(
            client_name,
            new_client_name=request.client_name,
            api_key=request.api_key,
            model=request.model,
            grid_url=request.grid_url,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "status": "updated",
        "client": client,
    }


@router.get("/clients/{client_name}/config")
async def get_client_config(client_name: str) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "client_config_requested client_name=%s",
        client_name,
        domain=client_name,
        client_name=client_name,
    )
    try:
        return await job_process_service.get_client_configuration(client_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


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
        job_extract=request.job_extract,
        ats_check=request.ats_check,
    )
    try:
        process_document = await job_process_service.submit_process(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
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


@router.get("/processes")
async def list_processes(client_name: str, page: int = 1, page_size: int = 10) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "process_list_requested client_name=%s page=%s page_size=%s",
        client_name,
        page,
        page_size,
        domain=client_name,
        client_name=client_name,
        page=page,
        page_size=page_size,
    )
    try:
        return await job_process_service.list_processes(
            client_name=client_name,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/processes/upload")
async def create_process_from_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    client_name: str = Form("default_client"),
    agent_count: int = Form(1),
    ats_check: bool = Form(True),
    job_extract: bool = Form(False),
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
        ats_check=ats_check,
        job_extract=job_extract,
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
        job_extract=job_extract,
        job_monitoring=job_monitoring,
    )
    try:
        process_document = await job_process_service.submit_process(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
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


@router.post("/processes/{process_id}/rerun")
async def rerun_process(
    process_id: str,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    log_event(
        logger,
        "info",
        "process_rerun_requested process_id=%s",
        process_id,
        domain="api",
        process_id=process_id,
    )
    try:
        process_document = await job_process_service.submit_rerun_process(process_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    background_tasks.add_task(
        job_process_service.execute_rerun_process,
        process_document["process_id"],
    )
    return {
        "process_id": process_document["process_id"],
        "status": process_document["status"],
    }


@router.post("/processes/{process_id}/stop")
async def stop_process(process_id: str) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "process_stop_requested process_id=%s",
        process_id,
        domain="api",
        process_id=process_id,
    )
    try:
        return await job_process_service.stop_process(process_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


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
    try:
        overview = await job_process_service.get_client_overview(client_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
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
    try:
        overview = await job_process_service.get_client_overview(client_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    summary_payload = _build_client_summary_export(client_name, overview)
    return _downloadable_json_response(
        summary_payload,
        f"client_{_safe_filename(client_name)}_summary.json",
    )


@router.get("/clients/{client_name}/jobs")
async def get_client_jobs(client_name: str, limit: int = 500) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "client_jobs_requested client_name=%s limit=%s",
        client_name,
        limit,
        domain=client_name,
        client_name=client_name,
        limit=limit,
    )
    try:
        return await job_process_service.get_client_jobs(client_name, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/clients/{client_name}/jobs.json")
async def download_client_jobs_json(client_name: str, limit: int = 500) -> StreamingResponse:
    log_event(
        logger,
        "info",
        "client_jobs_json_requested client_name=%s limit=%s",
        client_name,
        limit,
        domain=client_name,
        client_name=client_name,
        limit=limit,
    )
    try:
        payload = await job_process_service.get_client_jobs(client_name, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _downloadable_json_response(
        payload,
        f"client_{_safe_filename(client_name)}_jobs.json",
    )


@router.get("/clients/{client_name}/jobs.csv")
async def download_client_jobs_csv(client_name: str, limit: int = 500) -> StreamingResponse:
    log_event(
        logger,
        "info",
        "client_jobs_csv_requested client_name=%s limit=%s",
        client_name,
        limit,
        domain=client_name,
        client_name=client_name,
        limit=limit,
    )
    try:
        payload = await job_process_service.get_client_jobs(client_name, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    rows = _build_client_jobs_csv_rows(payload)
    return _downloadable_csv_response(
        rows,
        f"client_{_safe_filename(client_name)}_jobs.csv",
    )


@router.get("/processes/{process_id}/jobs.json")
async def download_process_jobs_json(process_id: str, limit: int = 500) -> StreamingResponse:
    log_event(
        logger,
        "info",
        "process_jobs_json_requested process_id=%s limit=%s",
        process_id,
        limit,
        domain="api",
        process_id=process_id,
        limit=limit,
    )
    try:
        payload = await job_process_service.get_process_jobs(process_id, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _downloadable_json_response(
        payload,
        f"process_{_safe_filename(process_id)}_jobs.json",
    )


@router.get("/processes/{process_id}/jobs.csv")
async def download_process_jobs_csv(process_id: str, limit: int = 500) -> StreamingResponse:
    log_event(
        logger,
        "info",
        "process_jobs_csv_requested process_id=%s limit=%s",
        process_id,
        limit,
        domain="api",
        process_id=process_id,
        limit=limit,
    )
    try:
        payload = await job_process_service.get_process_jobs(process_id, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    rows = _build_process_jobs_csv_rows(payload)
    return _downloadable_csv_response(
        rows,
        f"process_{_safe_filename(process_id)}_jobs.csv",
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
