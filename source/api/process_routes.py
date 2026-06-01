from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from models.process import JobProcessRequest
from services.flow_safety import extract_domain
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
    content = _csv_content(rows)
    return StreamingResponse(
        iter([content.encode("utf-8")]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _csv_content(rows: list[dict[str, Any]]) -> str:
    output = io.StringIO()
    fieldnames = list(rows[0].keys()) if rows else []
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    if fieldnames:
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _json_default(value) for key, value in row.items()})
    content = output.getvalue()
    output.close()
    return content


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


def _build_jobs_csv_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
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
    return _build_jobs_csv_rows(payload)


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


def _build_career_outcome_reason_with_pagination(career_overview: dict[str, Any]) -> str | None:
    reason = str(career_overview.get("outcome_reason") or "").strip() or None
    listing_ui = career_overview.get("listing_ui")
    pagination_present = None
    if isinstance(listing_ui, dict):
        pagination_present = listing_ui.get("pagination_present")
    if pagination_present is True:
        suffix = " Pagination detected on the page: yes."
    elif pagination_present is False:
        suffix = " Pagination detected on the page: no."
    else:
        suffix = " Pagination detected on the page: unknown."
    return f"{reason or ''}{suffix}".strip() or None


def _build_process_important_csv_rows(process: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in list(process.get("items") or []):
        result_payload = dict(item.get("result_payload") or {})
        career_page_result = dict(result_payload.get("career_page_result") or {})
        result_summary = dict(item.get("result_summary") or {})
        career_overview = dict(career_page_result.get("overview") or {})
        ats_detection = dict(result_payload.get("ats_detection") or {})
        apply_url = dict(result_payload.get("apply_url_detection") or {})
        rows.append(
            {
                "status": item.get("status"),
                "raw_url": item.get("raw_url"),
                "provided_career_page_url": item.get("provided_career_page_url"),
                "resolved_career_page_url": item.get("resolved_career_page_url"),
                "result_summary.career_url_status": result_summary.get("career_url_status"),
                "career_page_result.overview.outcome": career_overview.get("outcome"),
                "career_page_result.overview.outcome_reason": _build_career_outcome_reason_with_pagination(career_overview),
                "career_page_result.overview.total_jobs_found": career_overview.get("total_jobs_found"),
                "career_page_result.overview.job_alert": career_overview.get("job_alert"),
                "career_page_result.overview.job_alert_note": career_overview.get("job_alert_note"),
                "ats_detection.ats_detected": ats_detection.get("ats_detected"),
                "ats_detection.ats_provider": ats_detection.get("ats_provider"),
                "ats_detection.confidence": ats_detection.get("confidence"),
                "ats_detection.detection_method": ats_detection.get("detection_method"),
                "ats_detection.reasoning": ats_detection.get("reasoning"),
                "ats_detection.non_ats_reason": ats_detection.get("non_ats_reason"),
                "ats_detection.apply_url": ats_detection.get("apply_url"),
                "apply_url_detection.status": apply_url.get("status"),
                "apply_url_detection.means_of_application": apply_url.get("means_of_application"),
                "apply_url_detection.apply_url": apply_url.get("apply_url"),
                "apply_url_detection.apply_email": apply_url.get("apply_email"),
                "apply_url_detection.apply_document_url": apply_url.get("apply_document_url"),
                "apply_url_detection.source_url": apply_url.get("source_url"),
                "apply_url_detection.confidence": apply_url.get("confidence"),
                "apply_url_detection.reasoning": apply_url.get("reasoning"),
            }
        )
    return rows


def _build_process_roles_csv_rows(process: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for item in list(process.get("items") or []):
        result_payload = dict(item.get("result_payload") or {})
        career_page_result = dict(result_payload.get("career_page_result") or {})
        for page in list(career_page_result.get("career_pages_analysis") or []):
            career_url = str(
                page.get("extracted_url")
                or page.get("current_url")
                or page.get("url")
                or page.get("navigation_url")
                or ""
            ).strip()
            llm_analysis = dict(page.get("llm_analysis") or {})
            for job in list(llm_analysis.get("jobs_listed_on_page") or []):
                if not isinstance(job, dict):
                    continue
                title = str(job.get("title") or "").strip() or None
                job_url = str(job.get("job_url") or "").strip() or None
                if not title and not job_url:
                    continue
                row = {
                    "company_url": item.get("raw_url"),
                    "career_url": career_url or None,
                    "job_url": job_url,
                    "title": title,
                }
                marker = (
                    str(row["company_url"] or ""),
                    str(row["career_url"] or ""),
                    str(row["job_url"] or ""),
                    str(row["title"] or ""),
                )
                if marker in seen:
                    continue
                seen.add(marker)
                rows.append(row)
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
    if job_process_service.is_process_running():
        raise HTTPException(
            status_code=409,
            detail=f"Process {job_process_service.get_active_process_id()} is already running. Only one process can run at a time.",
        )
    log_event(
        logger,
        "info",
        "process_create_requested url_count=%s agent_count=%s",
        len(request.urls),
        request.agent_count,
        domain=request.urls[0] if request.urls else "unknown",
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
async def list_processes(page: int = 1, page_size: int = 10) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "process_list_requested page=%s page_size=%s",
        page,
        page_size,
        domain="processes",
        page=page,
        page_size=page_size,
    )
    return await job_process_service.list_processes(
        page=page,
        page_size=page_size,
    )


@router.post("/processes/upload")
async def create_process_from_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    agent_count: int = Form(1),
    ats_check: bool = Form(True),
    job_extract: bool = Form(False),
    job_monitoring: bool = Form(False),
    task_id: str | None = Form(None),
) -> dict[str, str]:
    if job_process_service.is_process_running():
        raise HTTPException(
            status_code=409,
            detail=f"Process {job_process_service.get_active_process_id()} is already running. Only one process can run at a time.",
        )
    if not file.filename:
        raise HTTPException(status_code=400, detail="Uploaded file must have a filename")

    content = await file.read()
    try:
        upload_rows = file_input_service.extract_upload_rows(file.filename, content)
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

    urls = [row.domain for row in upload_rows]
    career_page_urls = {
        (extract_domain(row.domain) or row.domain.strip().lower().rstrip("/")): row.career_page_url
        for row in upload_rows
        if row.career_page_url
    }

    request = JobProcessRequest(
        urls=urls,
        career_page_urls=career_page_urls,
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
    if job_process_service.is_process_running():
        raise HTTPException(
            status_code=409,
            detail=f"Process {job_process_service.get_active_process_id()} is already running. Only one process can run at a time.",
        )
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
        process_id,
    )
    return {
        "process_id": process_id,
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


@router.get("/processes/{process_id}/important-roles.csv")
async def get_process_roles_csv(process_id: str) -> StreamingResponse:
    log_event(
        logger,
        "info",
        "process_roles_csv_requested process_id=%s",
        process_id,
        domain="api",
        process_id=process_id,
    )
    process = await job_process_service.get_process(process_id)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    rows = _build_process_roles_csv_rows(process)
    return _downloadable_csv_response(
        rows,
        f"process_{_safe_filename(process_id)}_roles.csv",
    )
