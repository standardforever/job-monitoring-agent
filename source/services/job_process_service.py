from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4

from models.process import (
    DomainProcessRecord,
    JobProcessRequest,
    ProcessRunDocument,
    ProcessRunItemDocument,
    RequestedCapability,
    WorkerProcessResult,
)
from nodes.apply_url_node import get_apply_url
from nodes.ats_check_node import detect_ats
from nodes.career_page_category import _build_career_page_overview, career_page_category_node
from nodes.session_bootstrap import bootstrap_browser_node
from nodes.url_extraction import career_url_extraction_node
from services.agent_allocator import allocate_urls_to_agents
from services.flow_safety import extract_domain
from services.grid_session import (
    attach_playwright_to_cdp,
    close_agent_tab,
    close_browser_attachment,
    close_shared_session_async,
    create_domain_tab,
    create_session_async,
    is_grid_session_active_async,
)
from services.job_extraction_service import JobExtractionService
from services.content_extraction import extract_page_content
from services.navigation import navigate_to_url
from services.openai_service import (
    reset_openai_runtime_config,
    set_openai_runtime_config,
)
from services.tab_manager import ensure_agent_tab
from services.mongodb_service import MongoDBService
from core.config import get_settings
from utils.logging import configure_logging, get_logger, log_event

logger = get_logger("job_process_service")
SINGLE_USER_KEY = "single_user"
SINGLE_USER_NAME = "single_user"


@dataclass(slots=True)
class SharedSessionRuntime:
    grid_url: str | None
    session_id: str
    cdp_url: str
    recovery_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class AgentSessionRecoveryNeeded(Exception):
    pass


class JobProcessService:
    def __init__(self, mongodb_service: MongoDBService | None = None) -> None:
        self._mongodb_service = mongodb_service or MongoDBService()
        self._job_extraction_service = JobExtractionService(self._mongodb_service)
        self._settings = get_settings()
        self._stop_requests: set[str] = set()
        self._rerun_snapshots: dict[str, dict[str, Any]] = {}
        self._active_process_id: str | None = None
        log_event(logger, "info", "job_process_service_initialized", domain="service")

    def is_process_running(self) -> bool:
        return self._active_process_id is not None

    def get_active_process_id(self) -> str | None:
        return self._active_process_id

    async def submit_process(self, request: JobProcessRequest) -> dict[str, Any]:
        client_key, client_name = SINGLE_USER_KEY, SINGLE_USER_NAME
        resolved_grid_url = self._settings.selenium_remote_url
        process_id = request.task_id or str(uuid4())
        requested_capability = self._requested_capability_for_request(request)
        now = datetime.utcnow()
        normalized_domain_map = {
            url: self._normalize_domain_key(url)
            for url in request.urls
        }
        latest_domain_items = await self._mongodb_service.get_latest_process_items_by_domain_keys(
            list(normalized_domain_map.values())
        )
        effective_career_page_urls: dict[str, str] = {}
        input_rows: list[dict[str, Any]] = []
        for url in request.urls:
            domain_key = normalized_domain_map[url]
            historical_item = latest_domain_items.get(domain_key) or {}
            provided_career_page_url = request.career_page_urls.get(domain_key)
            reused_career_page_url = (
                historical_item.get("resolved_career_page_url")
                or historical_item.get("provided_career_page_url")
            )
            effective_career_page_url = provided_career_page_url or reused_career_page_url
            if effective_career_page_url:
                effective_career_page_urls[domain_key] = str(effective_career_page_url).strip()

            input_rows.append(
                {
                    "domain": url,
                    "career_page_url": provided_career_page_url,
                    "effective_career_page_url": effective_career_page_url,
                    "reused_existing_domain": bool(historical_item),
                    "previous_process_id": historical_item.get("process_id"),
                }
            )

        effective_request = request.model_copy(update={"career_page_urls": effective_career_page_urls})
        assignments = allocate_urls_to_agents(effective_request.urls, effective_request.agent_count)

        run_document = ProcessRunDocument(
            process_id=process_id,
            client_key=client_key,
            client_name=client_name,
            status="queued",
            request=effective_request,
            assignments=assignments,
            queued_urls=list(effective_request.urls),
            metadata={
                "client_model": self._settings.openai_model,
                "client_grid_url": resolved_grid_url,
                "ats_check": effective_request.ats_check,
                "job_extract": effective_request.job_extract,
                "job_monitoring": effective_request.job_monitoring,
                "requested_capability": requested_capability,
                "input_rows": input_rows,
            },
            summary={
                "total_urls": len(effective_request.urls),
                "assigned_agent_count": effective_request.agent_count,
                "processed_url_count": 0,
                "completed_domain_count": 0,
                "failed_domain_count": 0,
                "queued_url_count": len(effective_request.urls),
                "running_url_count": 0,
                "stopped_url_count": 0,
            },
            created_at=now,
            updated_at=now,
        )

        run_items = [
            ProcessRunItemDocument(
                process_id=process_id,
                client_key=client_key,
                client_name=client_name,
                raw_url=url,
                domain_key=normalized_domain_map[url],
                provided_career_page_url=effective_career_page_urls.get(normalized_domain_map[url]),
                requested_capability=requested_capability,
                status="queued",
                created_at=now,
                updated_at=now,
            ).model_dump(mode="json")
            for url in effective_request.urls
        ]

        await self._mongodb_service.insert_process_run(run_document.model_dump(mode="json"))
        await self._mongodb_service.insert_process_run_items(run_items)

        log_event(
            logger,
            "info",
            "process_submission_completed process_id=%s client_key=%s url_count=%s",
            process_id,
            client_key,
            len(effective_request.urls),
            domain=effective_request.urls[0] if effective_request.urls else client_key,
            process_id=process_id,
            client_key=client_key,
            url_count=len(effective_request.urls),
        )
        return run_document.model_dump(mode="json")

    async def run_process(self, request: JobProcessRequest, process_id: str | None = None) -> dict[str, Any]:
        configure_logging()
        submitted_process = await self.submit_process(
            request.model_copy(update={"task_id": process_id or request.task_id})
        )
        return await self.execute_existing_process(submitted_process["process_id"])

    async def execute_existing_process(self, process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_run(process_id)
        if process is None:
            raise ValueError(f"Unknown process_id: {process_id}")

        if self._active_process_id is not None:
            raise ValueError(
                f"Process {self._active_process_id} is already running. Only one process can run at a time."
            )

        if process.get("status") in ("running", "stop_requested"):
            await self._mongodb_service.update_process_run(
                process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": ["Process interrupted (recovered from dead state)"],
                },
            )
            raise ValueError(
                f"Process {process_id} was stuck in '{process['status']}' state and has been marked failed. Use rerun to retry."
            )

        domain = (((process.get("request") or {}).get("urls") or ["unknown"])[0])
        request = JobProcessRequest(**process["request"])
        assignments = process.get("assignments", [])
        await self._mongodb_service.update_process_run(
            process_id,
            {
                "status": "running",
                "started_at": datetime.utcnow(),
                "errors": [],
            },
        )

        self._active_process_id = process_id
        log_event(
            logger,
            "info",
            "execute_existing_process_started process_id=%s client_key=%s",
            process_id,
            process.get("client_key"),
            domain=domain,
            process_id=process_id,
            client_key=process.get("client_key"),
        )

        runtime_tokens = set_openai_runtime_config(
            api_key=self._settings.openai_api_key,
            model=self._settings.openai_model,
        )
        try:
            result = await self._execute_process(
                process_id=process_id,
                request=request,
                assignments=assignments,
                client_key=process["client_key"],
                client_name=process["client_name"],
                grid_url=str((process.get("metadata") or {}).get("client_grid_url") or self._settings.selenium_remote_url),
            )
        except Exception as exc:
            await self._mongodb_service.update_process_run(
                process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [str(exc)],
                },
            )
            raise
        finally:
            self._stop_requests.discard(process_id)
            reset_openai_runtime_config(runtime_tokens)
            self._active_process_id = None

        await self._mongodb_service.update_process_run(
            process_id,
            {
                "status": result["status"],
                "completed_at": datetime.utcnow(),
                "errors": result["errors"],
                "summary": result["summary"],
            },
        )
        return result

    async def submit_rerun_process(self, original_process_id: str) -> dict[str, Any]:
        original_process = await self._mongodb_service.get_process_run_with_items(original_process_id)
        if original_process is None:
            raise ValueError("Process not found")
        original_status = str(original_process.get("status") or "").strip().lower()
        if original_status in {"running", "queued", "stop_requested"}:
            raise ValueError(
                f"Process {original_process_id} is currently {original_status} and cannot be rerun yet."
            )

        request = JobProcessRequest(**dict(original_process.get("request") or {}))
        assignments = allocate_urls_to_agents(request.urls, request.agent_count)
        self._rerun_snapshots[original_process_id] = original_process
        updated = await self._mongodb_service.reset_process_run_for_rerun(
            original_process_id,
            queued_urls=list(request.urls),
            assignments=assignments,
        )
        await self._mongodb_service.update_process_run(
            original_process_id,
            {
                "metadata": {
                    **dict(original_process.get("metadata") or {}),
                    "workflow_mode": "rerun",
                    "last_rerun_requested_at": datetime.utcnow(),
                }
            },
        )
        return updated or original_process

    async def execute_rerun_process(self, rerun_process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_run(rerun_process_id)
        if process is None:
            raise ValueError(f"Unknown process_id: {rerun_process_id}")
        original_process = self._rerun_snapshots.get(rerun_process_id)
        if original_process is None:
            raise ValueError("Rerun snapshot is not available")

        request = JobProcessRequest(**process["request"])
        assignments = process.get("assignments", [])

        await self._mongodb_service.update_process_run(
            rerun_process_id,
            {
                "status": "running",
                "started_at": datetime.utcnow(),
                "errors": [],
            },
        )

        if self._active_process_id is not None:
            raise ValueError(
                f"Process {self._active_process_id} is already running. Only one process can run at a time."
            )

        self._active_process_id = rerun_process_id
        runtime_tokens = set_openai_runtime_config(
            api_key=self._settings.openai_api_key,
            model=self._settings.openai_model,
        )
        try:
            result = await self._execute_rerun_process(
                process_id=rerun_process_id,
                request=request,
                assignments=assignments,
                client_key=process["client_key"],
                client_name=process["client_name"],
                grid_url=str((process.get("metadata") or {}).get("client_grid_url") or self._settings.selenium_remote_url),
                original_process=original_process,
            )
        except Exception as exc:
            await self._mongodb_service.update_process_run(
                rerun_process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [str(exc)],
                },
            )
            raise
        finally:
            self._stop_requests.discard(rerun_process_id)
            reset_openai_runtime_config(runtime_tokens)
            self._rerun_snapshots.pop(rerun_process_id, None)
            self._active_process_id = None

        await self._mongodb_service.update_process_run(
            rerun_process_id,
            {
                "status": result["status"],
                "completed_at": datetime.utcnow(),
                "errors": result["errors"],
                "summary": result["summary"],
            },
        )
        return result

    async def get_process(self, process_id: str) -> dict[str, Any] | None:
        return await self._mongodb_service.get_process_run_with_items(process_id)

    async def list_processes(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        normalized_page = max(1, int(page or 1))
        normalized_page_size = max(1, min(int(page_size or 20), 200))
        processes = await self._mongodb_service.list_all_process_runs(limit=normalized_page * normalized_page_size)
        total = len(processes)
        start_index = (normalized_page - 1) * normalized_page_size
        page_processes = processes[start_index:start_index + normalized_page_size]
        return {
            "client_key": SINGLE_USER_KEY,
            "client_name": SINGLE_USER_NAME,
            "count": len(page_processes),
            "total": total,
            "page": normalized_page,
            "page_size": normalized_page_size,
            "has_next": normalized_page * normalized_page_size < total,
            "has_previous": normalized_page > 1,
            "processes": page_processes,
        }

    async def get_process_jobs(self, process_id: str, limit: int = 500) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_run_with_items(process_id)
        if process is None:
            raise ValueError("Process not found")

        job_contexts: list[dict[str, Any]] = []
        job_keys_in_order: list[str] = []
        seen_job_keys: set[str] = set()
        for item in list(process.get("items") or []):
            result_payload = dict(item.get("result_payload") or {})
            jobs_extraction = dict(result_payload.get("jobs_extraction") or {})
            for job in list(jobs_extraction.get("jobs") or []):
                if not isinstance(job, dict):
                    continue
                job_key = str(job.get("job_key") or "").strip()
                if not job_key or job_key in seen_job_keys:
                    continue
                seen_job_keys.add(job_key)
                job_keys_in_order.append(job_key)
                job_contexts.append(
                    {
                        "job_key": job_key,
                        "domain_key": item.get("domain_key"),
                        "raw_url": item.get("raw_url"),
                        "process_id": process_id,
                        "source_type": job.get("source_type"),
                        "source_url": job.get("source_url"),
                        "page_fingerprint": job.get("page_fingerprint"),
                        "title": job.get("title"),
                        "company_name": job.get("company_name"),
                    }
                )

        canonical_jobs = await self._mongodb_service.list_jobs_by_keys(job_keys_in_order[:limit])
        canonical_job_map = {str(job.get("job_key")): job for job in canonical_jobs}
        jobs: list[dict[str, Any]] = []
        for context in job_contexts:
            if len(jobs) >= limit:
                break
            canonical_job = canonical_job_map.get(str(context["job_key"]))
            if canonical_job is None:
                continue
            jobs.append(
                {
                    **context,
                    "created_at": canonical_job.get("created_at"),
                    "updated_at": canonical_job.get("updated_at"),
                    "job_data": canonical_job.get("structured_job") or {},
                }
            )

        return {
            "process_id": process_id,
            "client_key": process.get("client_key"),
            "client_name": process.get("client_name"),
            "count": len(jobs),
            "jobs": jobs,
        }

    async def stop_process(self, process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_run(process_id)
        if process is None:
            raise ValueError("Process not found")

        current_status = str(process.get("status") or "")
        if current_status in {"completed", "failed", "stopped"}:
            return {
                "process_id": process_id,
                "status": current_status,
                "message": f"Process is already {current_status}.",
            }

        self._stop_requests.add(process_id)
        updated = await self._mongodb_service.mark_process_stop_requested(process_id)
        return {
            "process_id": process_id,
            "status": str((updated or process).get("status") or "stop_requested"),
            "message": "Stop requested. Running work will stop after the current URL finishes.",
        }

    async def _execute_process(
        self,
        *,
        process_id: str,
        request: JobProcessRequest,
        assignments: list[dict[str, Any]],
        client_key: str,
        client_name: str,
        grid_url: str,
    ) -> dict[str, Any]:
        log_event(
            logger,
            "info",
            "execute_process_started process_id=%s assignment_count=%s",
            process_id,
            len(assignments),
            domain=request.urls[0] if request.urls else client_key,
            process_id=process_id,
            assignment_count=len(assignments),
        )

        session_info = await create_session_async(grid_url=grid_url, reuse_existing=False)
        if session_info is None or not session_info.cdp_url:
            error = "Unable to establish shared Selenium/CDP session"
            await self._mongodb_service.update_process_run(
                process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [error],
                    "summary": {
                        "total_urls": len(request.urls),
                        "assigned_agent_count": request.agent_count,
                        "processed_url_count": len(request.urls),
                        "completed_domain_count": 0,
                        "failed_domain_count": len(request.urls),
                        "queued_url_count": 0,
                        "running_url_count": 0,
                        "stopped_url_count": 0,
                    },
                    "queued_urls": [],
                    "failed_urls": list(request.urls),
                },
            )
            for assignment in assignments:
                await self._mongodb_service.update_assignment_status(process_id, assignment["agent_index"], "completed")
                for url in assignment["urls"]:
                    await self._mongodb_service.mark_url_failed(
                        process_id,
                        url,
                        error,
                        result_payload={"status": "failed", "reason": error},
                        was_running=False,
                    )
            return {
                "process_id": process_id,
                "status": "failed",
                "errors": [error],
                "worker_results": [],
                "summary": {
                    "total_urls": len(request.urls),
                    "assigned_agent_count": request.agent_count,
                    "processed_url_count": len(request.urls),
                    "completed_domain_count": 0,
                    "failed_domain_count": len(request.urls),
                    "queued_url_count": 0,
                    "running_url_count": 0,
                    "stopped_url_count": 0,
                },
            }

        shared_runtime = SharedSessionRuntime(
            grid_url=grid_url,
            session_id=session_info.session_id,
            cdp_url=session_info.cdp_url,
        )

        worker_inputs = [
            {
                "process_id": process_id,
                "client_key": client_key,
                "client_name": client_name,
                "agent_index": assignment["agent_index"],
                "assigned_urls": assignment["urls"],
                "session_id": session_info.session_id,
                "cdp_url": session_info.cdp_url,
                "shared_runtime": shared_runtime,
                "metadata": {
                    "ats_check": request.ats_check,
                    "job_extract": request.job_extract,
                    "job_monitoring": request.job_monitoring,
                    "requested_capability": self._requested_capability_for_request(request),
                    "career_page_urls": dict(request.career_page_urls or {}),
                },
            }
            for assignment in assignments
        ]

        try:
            worker_results = await asyncio.gather(*[self._run_agent(worker_input) for worker_input in worker_inputs])
        finally:
            await close_shared_session_async(shared_runtime.session_id)

        errors = [error for worker in worker_results for error in worker["errors"]]
        completed_domain_count = sum(
            1
            for worker in worker_results
            for record in worker["domain_results"]
            if record["status"] == "completed"
        )
        failed_domain_count = sum(
            1
            for worker in worker_results
            for record in worker["domain_results"]
            if record["status"] != "completed"
        )
        stop_requested = self._is_stop_requested(process_id)
        status = "stopped" if stop_requested else ("completed" if not errors else "failed")
        process_run = await self._mongodb_service.get_process_run(process_id)
        self._stop_requests.discard(process_id)

        persisted_summary = dict((process_run or {}).get("summary") or {})
        return {
            "process_id": process_id,
            "status": status,
            "errors": errors,
            "worker_results": worker_results,
            "summary": {
                "total_urls": len(request.urls),
                "assigned_agent_count": request.agent_count,
                "processed_url_count": int(persisted_summary.get("processed_url_count", completed_domain_count + failed_domain_count)),
                "completed_domain_count": int(persisted_summary.get("completed_domain_count", completed_domain_count)),
                "failed_domain_count": int(persisted_summary.get("failed_domain_count", failed_domain_count)),
                "queued_url_count": int(persisted_summary.get("queued_url_count", 0)),
                "running_url_count": int(persisted_summary.get("running_url_count", 0)),
                "stopped_url_count": int(persisted_summary.get("stopped_url_count", 0)),
            },
        }

    async def _execute_rerun_process(
        self,
        *,
        process_id: str,
        request: JobProcessRequest,
        assignments: list[dict[str, Any]],
        client_key: str,
        client_name: str,
        grid_url: str,
        original_process: dict[str, Any],
    ) -> dict[str, Any]:
        session_info = await create_session_async(grid_url=grid_url, reuse_existing=True)
        if session_info is None or not session_info.cdp_url:
            error = "Unable to establish shared Selenium/CDP session"
            await self._mongodb_service.update_process_run(
                process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [error],
                    "summary": {
                        "total_urls": len(request.urls),
                        "assigned_agent_count": request.agent_count,
                        "processed_url_count": len(request.urls),
                        "completed_domain_count": 0,
                        "failed_domain_count": len(request.urls),
                        "queued_url_count": 0,
                        "running_url_count": 0,
                        "stopped_url_count": 0,
                    },
                    "queued_urls": [],
                    "failed_urls": list(request.urls),
                },
            )
            for assignment in assignments:
                await self._mongodb_service.update_assignment_status(process_id, assignment["agent_index"], "completed")
                for url in assignment["urls"]:
                    await self._mongodb_service.mark_url_failed(
                        process_id,
                        url,
                        error,
                        result_payload={"status": "failed", "reason": error},
                        was_running=False,
                    )
            return {
                "process_id": process_id,
                "status": "failed",
                "errors": [error],
                "worker_results": [],
                "summary": {
                    "total_urls": len(request.urls),
                    "assigned_agent_count": request.agent_count,
                    "processed_url_count": len(request.urls),
                    "completed_domain_count": 0,
                    "failed_domain_count": len(request.urls),
                    "queued_url_count": 0,
                    "running_url_count": 0,
                    "stopped_url_count": 0,
                },
            }

        shared_runtime = SharedSessionRuntime(
            grid_url=grid_url,
            session_id=session_info.session_id,
            cdp_url=session_info.cdp_url,
        )
        original_item_map = {
            self._normalize_domain_key(item.get("raw_url") or item.get("domain_key") or ""): item
            for item in list(original_process.get("items") or [])
        }

        worker_inputs = [
            {
                "process_id": process_id,
                "client_key": client_key,
                "client_name": client_name,
                "agent_index": assignment["agent_index"],
                "assigned_urls": assignment["urls"],
                "shared_runtime": shared_runtime,
                "metadata": {
                    "ats_check": request.ats_check,
                    "job_extract": request.job_extract,
                    "job_monitoring": request.job_monitoring,
                    "requested_capability": self._requested_capability_for_request(request),
                    "career_page_urls": dict(request.career_page_urls or {}),
                },
                "original_item_map": original_item_map,
            }
            for assignment in assignments
        ]

        try:
            worker_results = await asyncio.gather(*[self._run_agent_rerun(worker_input) for worker_input in worker_inputs])
        finally:
            # await close_shared_session_async(shared_runtime.session_id)
            pass

        errors = [error for worker in worker_results for error in worker["errors"]]
        completed_domain_count = sum(
            1 for worker in worker_results for record in worker["domain_results"] if record["status"] == "completed"
        )
        failed_domain_count = sum(
            1 for worker in worker_results for record in worker["domain_results"] if record["status"] != "completed"
        )
        stop_requested = self._is_stop_requested(process_id)
        status = "stopped" if stop_requested else ("completed" if not errors else "failed")
        process_run = await self._mongodb_service.get_process_run(process_id)
        self._stop_requests.discard(process_id)
        persisted_summary = dict((process_run or {}).get("summary") or {})
        return {
            "process_id": process_id,
            "status": status,
            "errors": errors,
            "worker_results": worker_results,
            "summary": {
                "total_urls": len(request.urls),
                "assigned_agent_count": request.agent_count,
                "processed_url_count": int(persisted_summary.get("processed_url_count", completed_domain_count + failed_domain_count)),
                "completed_domain_count": int(persisted_summary.get("completed_domain_count", completed_domain_count)),
                "failed_domain_count": int(persisted_summary.get("failed_domain_count", failed_domain_count)),
                "queued_url_count": int(persisted_summary.get("queued_url_count", 0)),
                "running_url_count": int(persisted_summary.get("running_url_count", 0)),
                "stopped_url_count": int(persisted_summary.get("stopped_url_count", 0)),
            },
        }

    async def _run_agent(self, graph_input: dict[str, Any]) -> dict[str, Any]:
        assigned_urls = list(graph_input.get("assigned_urls", []))
        process_id = str(graph_input["process_id"])
        client_key = str(graph_input["client_key"])
        client_name = str(graph_input["client_name"])
        agent_index = int(graph_input["agent_index"])
        shared_runtime: SharedSessionRuntime = graph_input["shared_runtime"]
        browser_session = None

        if assigned_urls:
            await self._mongodb_service.update_assignment_status(process_id, agent_index, "running")
        if self._is_stop_requested(process_id):
            await self._stop_remaining_agent_urls(process_id, agent_index, assigned_urls)
            return WorkerProcessResult(
                agent_index=agent_index,
                status="stopped",
                assigned_urls=assigned_urls,
                processed_urls=[],
                domain_results=[],
                errors=[],
                metadata={"stop_requested": True},
            ).model_dump(mode="json")

        bootstrap_result = await bootstrap_browser_node(state=graph_input)
        if not bootstrap_result.get("session_established"):
            log_event(
                logger,
                "warning",
                "agent_bootstrap_failed_attempting_recovery process_id=%s agent_index=%s",
                process_id,
                agent_index,
                domain=assigned_urls[0] if assigned_urls else client_key,
                process_id=process_id,
                agent_index=agent_index,
            )
            try:
                browser_session, agent_tab = await self._recover_agent_tab(
                    shared_runtime=shared_runtime,
                    browser_session=None,
                    agent_index=agent_index,
                    url=assigned_urls[0] if assigned_urls else client_key,
                )
                bootstrap_metadata = dict(bootstrap_result.get("metadata", {}))
                bootstrap_metadata["bootstrap_status"] = "recovered"
                bootstrap_result = {
                    **bootstrap_result,
                    "browser_session": browser_session,
                    "agent_tab": agent_tab,
                    "session_established": True,
                    "metadata": bootstrap_metadata,
                }
            except Exception as exc:
                errors = list(bootstrap_result.get("errors", []))
                error_text = str(exc) or "; ".join(errors) or "Agent bootstrap failed"
                for url in assigned_urls:
                    await self._mongodb_service.mark_url_failed(
                        process_id,
                        url,
                        error_text,
                        result_payload={"status": "failed", "reason": error_text},
                        was_running=False,
                    )
                if assigned_urls:
                    await self._mongodb_service.update_assignment_status(process_id, agent_index, "completed")
                return WorkerProcessResult(
                    agent_index=agent_index,
                    status="failed",
                    assigned_urls=assigned_urls,
                    processed_urls=[],
                    domain_results=[],
                    errors=errors + [error_text],
                    metadata=dict(bootstrap_result.get("metadata", {})),
                ).model_dump(mode="json")

        browser_session = bootstrap_result.get("browser_session")
        agent_tab = bootstrap_result.get("agent_tab", {})

        try:
            domain_results: list[dict[str, Any]] = []
            errors: list[str] = []
            processed_urls: list[str] = []

            for url in assigned_urls:
                if self._is_stop_requested(process_id):
                    remaining_urls = [pending_url for pending_url in assigned_urls if pending_url not in processed_urls]
                    await self._stop_remaining_agent_urls(process_id, agent_index, remaining_urls)
                    break
                recovery_attempt_count = 0
                mark_running = True
                while True:
                    try:
                        provided_career_url = str(
                            ((graph_input.get("metadata") or {}).get("career_page_urls") or {}).get(
                                self._normalize_domain_key(url)
                            )
                            or ""
                        ).strip() or None
                        record = await self._process_domain(
                            process_id=process_id,
                            client_key=client_key,
                            client_name=client_name,
                            url=url,
                            browser_session=browser_session,
                            agent_index=agent_index,
                            agent_tab=agent_tab,
                            ats_check=bool((graph_input.get("metadata") or {}).get("ats_check", True)),
                            job_extract=bool((graph_input.get("metadata") or {}).get("job_extract", False)),
                            job_monitoring=bool((graph_input.get("metadata") or {}).get("job_monitoring", False)),
                            requested_capability=str((graph_input.get("metadata") or {}).get("requested_capability", "career_page")),
                            provided_career_url=provided_career_url,
                            mark_running=mark_running,
                        )
                        break
                    except AgentSessionRecoveryNeeded as exc:
                        recovery_attempt_count += 1
                        if recovery_attempt_count > 2:
                            error_text = str(exc)
                            record = DomainProcessRecord(
                                domain=url,
                                main_domain=extract_domain(url),
                                status="failed",
                                error=error_text,
                            ).model_dump(mode="json")
                            await self._mongodb_service.mark_url_failed(
                                process_id,
                                url,
                                error_text,
                                result_payload=record,
                                was_running=True,
                            )
                            log_event(
                                logger,
                                "error",
                                "agent_recovery_exhausted process_id=%s agent_index=%s url=%s error=%s",
                                process_id,
                                agent_index,
                                url,
                                error_text,
                                domain=url,
                                process_id=process_id,
                                agent_index=agent_index,
                                url=url,
                                error=error_text,
                            )
                            break

                        browser_session, agent_tab = await self._recover_agent_tab(
                            shared_runtime=shared_runtime,
                            browser_session=browser_session,
                            agent_index=agent_index,
                            url=url,
                        )
                        mark_running = False

                domain_results.append(record)
                processed_urls.append(url)
                if record["status"] != "completed" and record.get("error"):
                    errors.append(str(record["error"]))

            return WorkerProcessResult(
                agent_index=agent_index,
                status="stopped" if self._is_stop_requested(process_id) else ("completed" if not errors else "failed"),
                assigned_urls=assigned_urls,
                processed_urls=processed_urls,
                domain_results=[DomainProcessRecord(**record) for record in domain_results],
                errors=errors,
                metadata=dict(bootstrap_result.get("metadata", {})),
            ).model_dump(mode="json")
        finally:
            if assigned_urls:
                assignment_status = "stopped" if self._is_stop_requested(process_id) else "completed"
                await self._mongodb_service.update_assignment_status(process_id, agent_index, assignment_status)
            await close_agent_tab(browser_session)

    async def _run_agent_rerun(self, graph_input: dict[str, Any]) -> dict[str, Any]:
        assigned_urls = list(graph_input.get("assigned_urls", []))
        process_id = str(graph_input["process_id"])
        client_key = str(graph_input["client_key"])
        client_name = str(graph_input["client_name"])
        agent_index = int(graph_input["agent_index"])
        shared_runtime: SharedSessionRuntime = graph_input["shared_runtime"]
        original_item_map = dict(graph_input.get("original_item_map") or {})
        browser_session = None

        if assigned_urls:
            await self._mongodb_service.update_assignment_status(process_id, agent_index, "running")
        if self._is_stop_requested(process_id):
            await self._stop_remaining_agent_urls(process_id, agent_index, assigned_urls)
            return WorkerProcessResult(
                agent_index=agent_index,
                status="stopped",
                assigned_urls=assigned_urls,
                processed_urls=[],
                domain_results=[],
                errors=[],
                metadata={"stop_requested": True, "rerun": True},
            ).model_dump(mode="json")

        bootstrap_result = await bootstrap_browser_node(state=graph_input)
        if not bootstrap_result.get("session_established"):
            try:
                browser_session, agent_tab = await self._recover_agent_tab(
                    shared_runtime=shared_runtime,
                    browser_session=None,
                    agent_index=agent_index,
                    url=assigned_urls[0] if assigned_urls else client_key,
                )
                bootstrap_result = {
                    **bootstrap_result,
                    "browser_session": browser_session,
                    "agent_tab": agent_tab,
                    "session_established": True,
                    "metadata": {**dict(bootstrap_result.get("metadata", {})), "bootstrap_status": "recovered"},
                }
            except Exception as exc:
                errors = list(bootstrap_result.get("errors", []))
                error_text = str(exc) or "; ".join(errors) or "Agent bootstrap failed"
                for url in assigned_urls:
                    await self._mongodb_service.mark_url_failed(
                        process_id,
                        url,
                        error_text,
                        result_payload={"status": "failed", "reason": error_text},
                        was_running=False,
                    )
                if assigned_urls:
                    await self._mongodb_service.update_assignment_status(process_id, agent_index, "completed")
                return WorkerProcessResult(
                    agent_index=agent_index,
                    status="failed",
                    assigned_urls=assigned_urls,
                    processed_urls=[],
                    domain_results=[],
                    errors=errors + [error_text],
                    metadata={**dict(bootstrap_result.get("metadata", {})), "rerun": True},
                ).model_dump(mode="json")

        browser_session = bootstrap_result.get("browser_session")
        agent_tab = bootstrap_result.get("agent_tab", {})
        try:
            domain_results: list[dict[str, Any]] = []
            errors: list[str] = []
            processed_urls: list[str] = []

            for url in assigned_urls:
                if self._is_stop_requested(process_id):
                    remaining_urls = [pending_url for pending_url in assigned_urls if pending_url not in processed_urls]
                    await self._stop_remaining_agent_urls(process_id, agent_index, remaining_urls)
                    break

                recovery_attempt_count = 0
                mark_running = True
                while True:
                    try:
                        original_item = original_item_map.get(self._normalize_domain_key(url))
                        provided_career_url = str(
                            (original_item or {}).get("provided_career_page_url")
                            or (original_item or {}).get("resolved_career_page_url")
                            or ((graph_input.get("metadata") or {}).get("career_page_urls") or {}).get(
                                self._normalize_domain_key(url)
                            )
                            or ""
                        ).strip() or None
                        record = await self._rerun_domain(
                            process_id=process_id,
                            client_key=client_key,
                            client_name=client_name,
                            url=url,
                            browser_session=browser_session,
                            agent_index=agent_index,
                            agent_tab=agent_tab,
                            ats_check=bool((graph_input.get("metadata") or {}).get("ats_check", True)),
                            job_extract=bool((graph_input.get("metadata") or {}).get("job_extract", False)),
                            job_monitoring=bool((graph_input.get("metadata") or {}).get("job_monitoring", False)),
                            requested_capability=str((graph_input.get("metadata") or {}).get("requested_capability", "career_page")),
                            original_item=original_item,
                            provided_career_url=provided_career_url,
                            mark_running=mark_running,
                        )
                        break
                    except AgentSessionRecoveryNeeded as exc:
                        recovery_attempt_count += 1
                        if recovery_attempt_count > 2:
                            error_text = str(exc)
                            record = DomainProcessRecord(
                                domain=url,
                                main_domain=extract_domain(url),
                                status="failed",
                                error=error_text,
                            ).model_dump(mode="json")
                            await self._mongodb_service.mark_url_failed(
                                process_id,
                                url,
                                error_text,
                                result_payload=record,
                                was_running=True,
                            )
                            break

                        browser_session, agent_tab = await self._recover_agent_tab(
                            shared_runtime=shared_runtime,
                            browser_session=browser_session,
                            agent_index=agent_index,
                            url=url,
                        )
                        mark_running = False

                domain_results.append(record)
                processed_urls.append(url)
                if record["status"] != "completed" and record.get("error"):
                    errors.append(str(record["error"]))

            return WorkerProcessResult(
                agent_index=agent_index,
                status="stopped" if self._is_stop_requested(process_id) else ("completed" if not errors else "failed"),
                assigned_urls=assigned_urls,
                processed_urls=processed_urls,
                domain_results=[DomainProcessRecord(**record) for record in domain_results],
                errors=errors,
                metadata={**dict(bootstrap_result.get("metadata", {})), "rerun": True},
            ).model_dump(mode="json")
        finally:
            if assigned_urls:
                assignment_status = "stopped" if self._is_stop_requested(process_id) else "completed"
                await self._mongodb_service.update_assignment_status(process_id, agent_index, assignment_status)
            await close_agent_tab(browser_session)

    async def _process_domain(
        self,
        *,
        process_id: str,
        client_key: str,
        client_name: str,
        url: str,
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
        ats_check: bool,
        job_extract: bool,
        job_monitoring: bool,
        requested_capability: str,
        provided_career_url: str | None = None,
        mark_running: bool = True,
    ) -> dict[str, Any]:
        domain_key = self._normalize_domain_key(url)
        main_domain = extract_domain(url)
        if mark_running:
            await self._mongodb_service.mark_url_running(process_id, url, agent_index)

        try:
            if provided_career_url:
                career_url_result = {
                    "status": "career_urls_found",
                    "error_message": None,
                    "career_urls": [provided_career_url],
                    "non_domain_career_urls": [],
                    "provided_career_page_url": provided_career_url,
                    "discovery_method": "provided_by_upload",
                }
            else:
                career_url_result = await career_url_extraction_node(main_domain or domain_key, browser_session)

            career_urls = list(career_url_result.get("career_urls") or [])
            if career_urls:
                career_page_result = await career_page_category_node(
                    career_urls,
                    browser_session,
                    agent_index,
                    agent_tab,
                )
            else:
                career_page_result = self._build_empty_career_page_result(career_url_result)

            resolved_career_page_url = self._resolve_career_page_start_url(
                career_url_result=career_url_result,
                career_page_result=career_page_result,
                provided_career_url=provided_career_url,
            )
            stored_career_page_result = self._clean_career_page_result_for_storage(career_page_result)

            content_changed = None

            if ats_check:
                # ats_detection = await detect_ats(
                #     career_page_result,
                #     main_domain or domain_key,
                #     browser_session,
                #     agent_index,
                #     agent_tab,
                # )
                ats_detection = {
                    "ats_detected": None,
                    "detection_method": "skipped",
                    "reasoning": "ATS check disabled for this process.",
                }
                job_urls = list((career_page_result.get("overview") or {}).get("job_urls") or [])
                apply_url_detection = await get_apply_url(
                    job_urls=job_urls,
                    browser_session=browser_session,
                    agent_index=agent_index,
                    agent_tab=agent_tab,
                    main_domain=main_domain or domain_key,
                )
            else:
                ats_detection = {
                    "ats_detected": None,
                    "detection_method": "skipped",
                    "reasoning": "ATS check disabled for this process.",
                }
                apply_url_detection = {
                    "status": "skipped",
                    "means_of_application": None,
                    "reasoning": "ATS check disabled — apply URL detection skipped.",
                }

            if job_extract:
                jobs_extraction = await self._job_extraction_service.extract_jobs_for_domain(
                    process_id=process_id,
                    client_key=client_key,
                    client_name=client_name,
                    raw_url=url,
                    domain_key=domain_key,
                    career_page_result=career_page_result,
                    browser_session=browser_session,
                    agent_index=agent_index,
                    agent_tab=agent_tab,
                )
            else:
                jobs_extraction = {
                    "status": "skipped",
                    "requested": False,
                    "job_count": 0,
                    "jobs": [],
                    "sources": [],
                }

            record = DomainProcessRecord(
                domain=url,
                main_domain=main_domain,
                career_url_extraction=career_url_result,
                career_page_result=stored_career_page_result,
                ats_detection=ats_detection,
                apply_url_detection=apply_url_detection,
                jobs_extraction=jobs_extraction,
                status="completed",
            ).model_dump(mode="json")

            result_summary = self._build_result_summary(
                record,
                reused_career_discovery=False,
                reused_ats_detection=False,
                content_changed=content_changed,
                job_monitoring=job_monitoring,
            )

            await self._mongodb_service.mark_url_completed(
                process_id,
                url,
                result_summary=result_summary,
                result_payload=record,
                resolved_career_page_url=resolved_career_page_url,
            )
            return record
        except Exception as exc:
            error_text = str(exc)
            if self._is_recoverable_agent_session_error(error_text):
                log_event(
                    logger,
                    "warning",
                    "agent_session_recovery_needed process_id=%s agent_index=%s url=%s error=%s",
                    process_id,
                    agent_index,
                    url,
                    error_text,
                    domain=url,
                    process_id=process_id,
                    agent_index=agent_index,
                    url=url,
                    error=error_text,
                )
                raise AgentSessionRecoveryNeeded(error_text) from exc
            failed_record = DomainProcessRecord(
                domain=url,
                main_domain=main_domain,
                status="failed",
                error=error_text,
            ).model_dump(mode="json")
            await self._mongodb_service.mark_url_failed(
                process_id,
                url,
                error_text,
                result_payload=failed_record,
                was_running=True,
            )
            return failed_record

    async def _rerun_domain(
        self,
        *,
        process_id: str,
        client_key: str,
        client_name: str,
        url: str,
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
        ats_check: bool,
        job_extract: bool,
        job_monitoring: bool,
        requested_capability: str,
        original_item: dict[str, Any] | None,
        provided_career_url: str | None = None,
        mark_running: bool = True,
    ) -> dict[str, Any]:
        domain_key = self._normalize_domain_key(url)
        main_domain = extract_domain(url)
        if mark_running:
            await self._mongodb_service.mark_url_running(process_id, url, agent_index)

        previous_record = dict((original_item or {}).get("result_payload") or {})
        previous_career_url_extraction = dict(previous_record.get("career_url_extraction") or {})
        previous_career_page_result = dict(previous_record.get("career_page_result") or {})
        previous_ats_detection = dict(previous_record.get("ats_detection") or {})
        try:
            if provided_career_url:
                fresh_career_url_result = {
                    "status": "career_urls_found",
                    "error_message": None,
                    "career_urls": [provided_career_url],
                    "non_domain_career_urls": [],
                    "provided_career_page_url": provided_career_url,
                    "discovery_method": "provided_by_upload",
                }
            else:
                fresh_career_url_result = await career_url_extraction_node(main_domain or domain_key, browser_session)
            career_url_result = self._select_rerun_career_url_result(
                fresh_result=fresh_career_url_result,
                previous_result=previous_career_url_extraction,
            )

            previous_not_job_related_urls = list(
                ((previous_career_page_result.get("overview") or {}).get("not_job_related_urls") or [])
            )
            previous_general_job_info_urls = list(
                ((previous_career_page_result.get("overview") or {}).get("general_job_info_urls") or [])
            )
            candidate_career_urls = self._filter_rerun_career_urls(
                career_url_result.get("career_urls") or [],
                previous_not_job_related_urls,
                previous_general_job_info_urls,
            )
            career_urls_changed = self._rerun_career_urls_changed(
                previous_urls=list(previous_career_url_extraction.get("career_urls") or []),
                current_urls=list(career_url_result.get("career_urls") or []),
            )

            reused_analysis_count = 0
            changed_page_count = 0
            content_changed = career_urls_changed
            career_page_result = self._build_empty_career_page_result(career_url_result)

            if candidate_career_urls:
                rerun_page_result = await self._build_rerun_career_page_result(
                    career_urls=candidate_career_urls,
                    previous_career_page_result=previous_career_page_result,
                    browser_session=browser_session,
                    agent_index=agent_index,
                    agent_tab=agent_tab,
                )
                career_page_result = rerun_page_result["career_page_result"]
                reused_analysis_count = int(rerun_page_result["reused_analysis_count"])
                changed_page_count = int(rerun_page_result["changed_page_count"])
                content_changed = bool(content_changed or rerun_page_result["content_changed"])

            resolved_career_page_url = self._resolve_career_page_start_url(
                career_url_result=career_url_result,
                career_page_result=career_page_result,
                provided_career_url=provided_career_url,
            )
            stored_career_page_result = self._clean_career_page_result_for_storage(career_page_result)

            ats_detection: dict[str, Any]
            apply_url_detection: dict[str, Any]
            previous_apply_url_detection = dict(previous_record.get("apply_url_detection") or {})
            if ats_check:
                if not content_changed:
                    ats_detection = {
                        **previous_ats_detection,
                        "detection_method": "rerun_skipped_no_career_change",
                        "reasoning": "Career content did not change during rerun, so ATS check was skipped.",
                        "reused": True,
                    }
                    apply_url_detection = {
                        **previous_apply_url_detection,
                        "detection_method": "rerun_skipped_no_career_change",
                        "reasoning": "Career content did not change during rerun, so apply URL detection was skipped.",
                        "reused": True,
                    }
                else:
                    ats_detection = await detect_ats(
                        career_page_result,
                        main_domain or domain_key,
                        browser_session,
                        agent_index,
                        agent_tab,
                    )
                    job_urls = list((career_page_result.get("overview") or {}).get("job_urls") or [])
                    apply_url_detection = await get_apply_url(
                        job_urls=job_urls,
                        browser_session=browser_session,
                        agent_index=agent_index,
                        agent_tab=agent_tab,
                        main_domain=main_domain or domain_key,
                    )
            else:
                ats_detection = {
                    "ats_detected": None,
                    "detection_method": "skipped",
                    "reasoning": "ATS check disabled for this process.",
                }
                apply_url_detection = {
                    "status": "skipped",
                    "means_of_application": None,
                    "reasoning": "ATS check disabled — apply URL detection skipped.",
                }

            if job_extract and content_changed:
                jobs_extraction = await self._job_extraction_service.extract_jobs_for_domain(
                    process_id=process_id,
                    client_key=client_key,
                    client_name=client_name,
                    raw_url=url,
                    domain_key=domain_key,
                    career_page_result=career_page_result,
                    browser_session=browser_session,
                    agent_index=agent_index,
                    agent_tab=agent_tab,
                )
            elif job_extract:
                jobs_extraction = {
                    "status": "skipped_no_career_change",
                    "requested": True,
                    "job_count": 0,
                    "jobs": [],
                    "sources": [],
                    "reason": "Career content did not change during rerun.",
                }
            else:
                jobs_extraction = {
                    "status": "skipped",
                    "requested": False,
                    "job_count": 0,
                    "jobs": [],
                    "sources": [],
                }

            record = DomainProcessRecord(
                domain=url,
                main_domain=main_domain,
                career_url_extraction=career_url_result,
                career_page_result=stored_career_page_result,
                ats_detection=ats_detection,
                apply_url_detection=apply_url_detection,
                jobs_extraction=jobs_extraction,
                status="completed",
            ).model_dump(mode="json")
            record["rerun_metadata"] = {
                "reused_analysis_count": reused_analysis_count,
                "changed_page_count": changed_page_count,
                "content_changed": content_changed,
            }

            result_summary = self._build_result_summary(
                record,
                reused_career_discovery=bool(career_url_result.get("used_previous_career_urls")),
                reused_ats_detection=bool(ats_detection.get("reused")),
                content_changed=content_changed,
                job_monitoring=job_monitoring,
            )

            await self._mongodb_service.mark_url_completed(
                process_id,
                url,
                result_summary=result_summary,
                result_payload=record,
                resolved_career_page_url=resolved_career_page_url,
            )
            return record
        except Exception as exc:
            error_text = str(exc)
            if self._is_recoverable_agent_session_error(error_text):
                raise AgentSessionRecoveryNeeded(error_text) from exc
            failed_record = DomainProcessRecord(
                domain=url,
                main_domain=main_domain,
                status="failed",
                error=error_text,
            ).model_dump(mode="json")
            await self._mongodb_service.mark_url_failed(
                process_id,
                url,
                error_text,
                result_payload=failed_record,
                was_running=True,
            )
            return failed_record

    async def _recover_agent_tab(
        self,
        *,
        shared_runtime: SharedSessionRuntime,
        browser_session: Any,
        agent_index: int,
        url: str,
    ) -> tuple[Any, dict[str, Any]]:
        async with shared_runtime.recovery_lock:
            original_session_id = shared_runtime.session_id
            session_active = await is_grid_session_active_async(shared_runtime.grid_url, original_session_id)

            if session_active:
                target_cdp_url = shared_runtime.cdp_url
                log_event(
                    logger,
                    "info",
                    "agent_tab_recovery_reusing_shared_session agent_index=%s session_id=%s url=%s",
                    agent_index,
                    original_session_id,
                    url,
                    domain=url,
                    agent_index=agent_index,
                    session_id=original_session_id,
                    url=url,
                )
            else:
                replacement = await create_session_async(
                    grid_url=shared_runtime.grid_url,
                    reuse_existing=False,
                )
                if replacement is None or not replacement.cdp_url:
                    raise RuntimeError("Shared browser session is unavailable and could not be recreated")

                await close_shared_session_async(original_session_id)
                shared_runtime.session_id = replacement.session_id
                shared_runtime.cdp_url = replacement.cdp_url
                target_cdp_url = replacement.cdp_url
                log_event(
                    logger,
                    "warning",
                    "shared_session_recreated_for_agent_recovery agent_index=%s old_session_id=%s new_session_id=%s url=%s",
                    agent_index,
                    original_session_id,
                    replacement.session_id,
                    url,
                    domain=url,
                    agent_index=agent_index,
                    old_session_id=original_session_id,
                    new_session_id=replacement.session_id,
                    url=url,
                )

        await close_browser_attachment(browser_session)
        rebuilt_session = await attach_playwright_to_cdp(shared_runtime.cdp_url)
        if rebuilt_session is None:
            raise RuntimeError("Failed to reattach Playwright during agent recovery")

        rebuilt_tab = await ensure_agent_tab(rebuilt_session, agent_index=agent_index)
        log_event(
            logger,
            "info",
            "agent_tab_recovery_completed agent_index=%s session_id=%s url=%s",
            agent_index,
            shared_runtime.session_id,
            url,
            domain=url,
            agent_index=agent_index,
            session_id=shared_runtime.session_id,
            url=url,
        )
        return rebuilt_session, rebuilt_tab

    def _select_rerun_career_url_result(
        self,
        *,
        fresh_result: dict[str, Any],
        previous_result: dict[str, Any],
    ) -> dict[str, Any]:
        fresh_urls = list(fresh_result.get("career_urls") or [])
        previous_urls = list(previous_result.get("career_urls") or [])
        if fresh_urls:
            selected = dict(fresh_result)
            selected["used_previous_career_urls"] = False
            return selected
        if previous_urls:
            selected = dict(previous_result)
            selected["used_previous_career_urls"] = True
            selected["fallback_reason"] = str(fresh_result.get("error_message") or fresh_result.get("status") or "").strip() or None
            return selected
        selected = dict(fresh_result)
        selected["used_previous_career_urls"] = False
        return selected

    def _filter_rerun_career_urls(
        self,
        urls: list[str],
        not_job_related_urls: list[str],
        general_job_info_urls: list[str] | None = None,
    ) -> list[str]:
        excluded = {
            self._normalize_url_for_compare(url)
            for url in [*not_job_related_urls, *(general_job_info_urls or [])]
        }
        filtered: list[str] = []
        seen: set[str] = set()
        for url in urls:
            normalized = self._normalize_url_for_compare(url)
            if not normalized or normalized in seen or normalized in excluded:
                continue
            seen.add(normalized)
            filtered.append(str(url).strip())
        return filtered

    def _rerun_career_urls_changed(self, *, previous_urls: list[str], current_urls: list[str]) -> bool:
        previous_set = {self._normalize_url_for_compare(url) for url in previous_urls if self._normalize_url_for_compare(url)}
        current_set = {self._normalize_url_for_compare(url) for url in current_urls if self._normalize_url_for_compare(url)}
        return previous_set != current_set

    def _normalize_url_for_compare(self, url: str | None) -> str:
        value = str(url or "").strip().lower()
        if not value:
            return ""
        value = value.rstrip("/")
        return value

    def _fingerprint_text(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _build_previous_page_analysis_map(self, career_page_result: dict[str, Any]) -> dict[str, dict[str, Any]]:
        previous_map: dict[str, dict[str, Any]] = {}
        for page in list(career_page_result.get("career_pages_analysis") or []):
            page_url = (
                page.get("extracted_url")
                or page.get("current_url")
                or page.get("url")
                or page.get("navigation_url")
                or ""
            )
            normalized = self._normalize_url_for_compare(str(page_url))
            if normalized:
                previous_map[normalized] = dict(page)
        return previous_map

    def _should_retry_failed_previous_page(self, previous_page: dict[str, Any] | None) -> bool:
        if not previous_page:
            return False

        status = str(previous_page.get("status") or "").strip()
        page_access_status = str(previous_page.get("page_access_status") or "").strip()
        error_text = str(previous_page.get("error") or "").strip()

        retryable_statuses = {
            "navigation_skipped",
            "navigation_timeout",
            "navigation_non_web_url",
            "action_failed",
            "download_started",
            "extraction_failed",
            "ai_analysis_failed",
            "access_issue",
        }
        retryable_page_access_statuses = {
            "blocked",
            "forbidden",
            "login_required",
            "captcha",
            "bot_check",
            "rate_limited",
            "timeout",
            "unknown",
        }
        retryable_error_signatures = (
            "timeout",
            "unable to extract page content",
            "access denied",
            "forbidden",
            "captcha",
            "blocked",
            "navigation",
            "connection closed",
            "session closed",
            "browser has been closed",
            "page has been closed",
        )

        if status in retryable_statuses:
            return True
        if page_access_status and page_access_status != "accessible" and page_access_status in retryable_page_access_statuses:
            return True

        normalized_error = error_text.lower()
        return any(signature in normalized_error for signature in retryable_error_signatures)

    async def _build_rerun_career_page_result(
        self,
        *,
        career_urls: list[str],
        previous_career_page_result: dict[str, Any],
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
    ) -> dict[str, Any]:
        previous_analysis_map = self._build_previous_page_analysis_map(previous_career_page_result)
        reused_results: list[dict[str, Any]] = []
        prechecked_failures: list[dict[str, Any]] = []
        changed_urls: list[str] = []

        for career_url in career_urls:
            normalized_url = self._normalize_url_for_compare(career_url)
            previous_page = previous_analysis_map.get(normalized_url)
            if self._should_retry_failed_previous_page(previous_page):
                changed_urls.append(career_url)
                continue

            previous_markdown = str((previous_page or {}).get("extracted_content") or "").strip()
            if not previous_markdown:
                changed_urls.append(career_url)
                continue

            nav_response = await navigate_to_url(
                browser_session.page if browser_session is not None else None,
                agent_index=agent_index,
                tab_handle=agent_tab["handle"],
                url=career_url,
                post_navigation_delay_ms=0,
            )
            if nav_response.get("status") != "navigated":
                prechecked_failures.append({**nav_response, "navigation_url": career_url})
                continue

            extracted_content_response = await extract_page_content(
                browser_session.page if browser_session is not None else None,
                sections=["body"],
            )
            current_markdown = str((extracted_content_response or {}).get("markdown") or "").strip()
            if not current_markdown:
                failure_record = {
                    **nav_response,
                    "navigation_url": career_url,
                    "status": "extraction_failed",
                    "error": "Unable to extract page content",
                }
                prechecked_failures.append(failure_record)
                continue

            if self._fingerprint_text(current_markdown) == self._fingerprint_text(previous_markdown):
                reused_result = dict(previous_page or {})
                reused_result["rerun_content_reused"] = True
                reused_result["current_url"] = nav_response.get("current_url") or reused_result.get("current_url")
                reused_results.append(reused_result)
            else:
                changed_urls.append(career_url)

        changed_result = {"overview": {}, "career_pages_analysis": []}
        if changed_urls:
            changed_result = await career_page_category_node(
                changed_urls,
                browser_session,
                agent_index,
                agent_tab,
            )

        merged_analysis = reused_results + list(changed_result.get("career_pages_analysis") or []) + prechecked_failures
        overview = _build_career_page_overview(merged_analysis)
        content_changed = bool(changed_urls or prechecked_failures)
        return {
            "career_page_result": {
                "overview": overview,
                "career_pages_analysis": merged_analysis,
            },
            "content_changed": content_changed,
            "reused_analysis_count": len(reused_results),
            "changed_page_count": len(changed_urls) + len(prechecked_failures),
        }

    def _is_stop_requested(self, process_id: str) -> bool:
        return process_id in self._stop_requests

    async def _stop_remaining_agent_urls(
        self,
        process_id: str,
        agent_index: int,
        urls: list[str],
    ) -> None:
        await self._mongodb_service.mark_urls_stopped(
            process_id,
            urls,
            agent_index=agent_index,
            reason="Process stop requested.",
        )

    def _is_recoverable_agent_session_error(self, error_text: str) -> bool:
        normalized = str(error_text or "").lower()
        return any(
            signature in normalized
            for signature in (
                "connection closed while reading from the driver",
                "target page, context or browser has been closed",
                "browser has been closed",
                "page has been closed",
                "context closed",
                "session closed",
                "cdp session closed",
                "websocket closed",
                "closed while reading from the driver",
            )
        )

    def _normalize_domain_key(self, raw_value: str) -> str:
        extracted = extract_domain(raw_value)
        return extracted or raw_value.strip().lower()

    def _build_empty_career_page_result(self, career_url_result: dict[str, Any]) -> dict[str, Any]:
        status = str(career_url_result.get("status") or "").strip()
        error_message = str(career_url_result.get("error_message") or "").strip() or None

        outcome = "career_page_not_analyzed"
        outcome_reason = "Career page analysis was skipped because no career URLs were available."

        if status == "no_career_page_found":
            outcome = "no_career_page_found"
            outcome_reason = "No career or job page candidates were found for this domain."
        elif status == "career_page_discovery_failed":
            outcome = "career_page_discovery_failed"
            outcome_reason = (
                "Career page discovery could not be completed."
                + (f" {error_message}" if error_message else "")
            )
        elif status == "domain_access_failed":
            outcome = "career_page_discovery_failed"
            outcome_reason = (
                f"Career page discovery failed because the domain could not be accessed."
                + (f" {error_message}" if error_message else "")
            )
        elif status == "domain_redirected":
            outcome = "career_page_domain_redirected"
            outcome_reason = (
                f"Career page discovery stopped because the domain redirected externally."
                + (f" {error_message}" if error_message else "")
            )
        elif error_message:
            outcome_reason = error_message

        return {
            "overview": {
                "outcome": outcome,
                "outcome_reason": outcome_reason,
                "jobs_found": False,
                "total_jobs_found": 0,
                "job_urls": [],
                "job_found_on_urls": [],
                "listing_ui": None,
                "job_alert": False,
                "job_alert_note": None,
                "job_alert_urls": [],
                "career_page_confirmed": False,
                "no_vacancy_urls": [],
                "general_job_info_urls": [],
                "blocked_platform_urls": {},
                "external_redirect_urls": [],
                "embedded_urls": [],
                "navigation_blocked_urls": [],
                "navigation_issues": [],
                "not_job_related_urls": [],
                "access_issue_urls": [],
                "unknown_urls": [],
                "total_urls_processed": 0,
            },
            "career_pages_analysis": [],
        }

    def _requested_capability_for_request(self, request: JobProcessRequest) -> RequestedCapability:
        if request.job_monitoring:
            return "job_monitoring"
        if request.ats_check and request.job_extract:
            return "ats_and_job_extract"
        if request.job_extract:
            return "job_extract"
        if request.ats_check:
            return "ats_check"
        return "career_page"

    def _fingerprint_payload(self, payload: Any) -> str:
        serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _extract_latest_page_text(self, career_page_result: dict[str, Any]) -> str | None:
        analyses = career_page_result.get("career_pages_analysis") or []
        for page in analyses:
            content = str(page.get("extracted_content") or "").strip()
            if content:
                return content
        return None

    def _resolve_career_page_start_url(
        self,
        *,
        career_url_result: dict[str, Any],
        career_page_result: dict[str, Any],
        provided_career_url: str | None = None,
    ) -> str | None:
        overview = dict(career_page_result.get("overview") or {})
        analyses = list(career_page_result.get("career_pages_analysis") or [])
        navigation_target_urls: list[str] = []
        for page in analyses:
            page_status = str(page.get("status") or "").strip()
            if page_status not in {"jobs_listed_on_page", "external_domain_redirect"}:
                continue

            for step in list(page.get("navigation_history") or []):
                landed_url = str(step.get("landed_url") or "").strip()
                target_url = str(step.get("target_url") or "").strip()
                step_status = str(step.get("status") or "").strip().lower()
                if landed_url and ("navigated" in step_status or "redirect" in step_status):
                    navigation_target_urls.append(landed_url)
                elif target_url and ("navigated" in step_status or "redirect" in step_status):
                    navigation_target_urls.append(target_url)

            current_url = str(page.get("current_url") or page.get("extracted_url") or "").strip()
            if current_url and page_status == "jobs_listed_on_page":
                navigation_target_urls.append(current_url)

        candidate_lists = (
            navigation_target_urls,
            overview.get("job_found_on_urls") or [],
            overview.get("no_vacancy_urls") or [],
            overview.get("general_job_info_urls") or [],
            overview.get("job_alert_urls") or [],
            [provided_career_url] if provided_career_url else [],
            career_url_result.get("career_urls") or [],
        )
        for values in candidate_lists:
            for value in list(values or []):
                normalized = str(value or "").strip()
                if normalized:
                    return normalized
        return None

    def _clean_career_page_result_for_storage(self, career_page_result: dict[str, Any]) -> dict[str, Any]:
        cleaned = dict(career_page_result or {})
        cleaned_analysis: list[dict[str, Any]] = []
        for page in list(cleaned.get("career_pages_analysis") or []):
            page_copy = dict(page)
            if str(page_copy.get("status") or "").strip() == "not_job_related":
                page_copy.pop("extracted_content", None)
            cleaned_analysis.append(page_copy)
        cleaned["career_pages_analysis"] = cleaned_analysis
        return cleaned

    def _build_result_summary(
        self,
        record: dict[str, Any],
        *,
        reused_career_discovery: bool,
        reused_ats_detection: bool,
        content_changed: bool | None,
        job_monitoring: bool,
    ) -> dict[str, Any]:
        ats_detection = record.get("ats_detection") or {}
        career_url_extraction = record.get("career_url_extraction") or {}
        jobs_extraction = record.get("jobs_extraction") or {}
        return {
            "status": record.get("status"),
            "main_domain": record.get("main_domain"),
            "career_url_status": career_url_extraction.get("status"),
            "career_url_count": len(career_url_extraction.get("career_urls") or []),
            "ats_detected": ats_detection.get("ats_detected"),
            "ats_provider": ats_detection.get("ats_provider"),
            "job_extract_requested": bool(jobs_extraction.get("requested")),
            "job_count": int(jobs_extraction.get("job_count") or 0),
            "reused_career_discovery": reused_career_discovery,
            "reused_ats_detection": reused_ats_detection,
            "content_changed": content_changed,
            "job_monitoring": job_monitoring,
        }
