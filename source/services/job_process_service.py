from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4

from models.process import (
    ClientDomainDocument,
    ClientDocument,
    DomainCheckDocument,
    DomainProcessRecord,
    JobProcessRequest,
    ProcessRunDocument,
    ProcessRunItemDocument,
    RequestedCapability,
    WorkerProcessResult,
)
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
    create_session_async,
    is_grid_session_active_async,
)
from services.job_extraction_service import JobExtractionService
from services.content_extraction import extract_page_content
from services.navigation import navigate_to_url
from services.openai_service import (
    mask_api_key,
    reset_openai_runtime_config,
    set_openai_runtime_config,
    validate_openai_api_key,
)
from services.tab_manager import ensure_agent_tab
from services.mongodb_service import MongoDBService
from core.config import get_settings
from utils.logging import configure_logging, get_logger, log_event

logger = get_logger("job_process_service")


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
        log_event(logger, "info", "job_process_service_initialized", domain="service")

    async def submit_process(self, request: JobProcessRequest) -> dict[str, Any]:
        client = await self._require_active_client(request.client_name)
        resolved_grid_url = client.get("grid_url") or self._settings.selenium_remote_url
        request = request.model_copy(update={"client_name": client["client_name"]})
        assignments = allocate_urls_to_agents(request.urls, request.agent_count)
        process_id = request.task_id or str(uuid4())
        requested_capability = self._requested_capability_for_request(request)
        now = datetime.utcnow()

        normalized_urls = [self._normalize_domain_key(url) for url in request.urls]
        for domain_key in normalized_urls:
            await self._mongodb_service.upsert_client_domain(
                client_key=client["client_key"],
                client_name=client["client_name"],
                domain_key=domain_key,
                requested_capability=requested_capability,
                ats_check=request.ats_check,
                job_extract=request.job_extract,
                job_monitoring=request.job_monitoring,
            )

        run_document = ProcessRunDocument(
            process_id=process_id,
            client_key=client["client_key"],
            client_name=client["client_name"],
            status="queued",
            request=request,
            assignments=assignments,
            queued_urls=list(request.urls),
            metadata={
                "client_model": client.get("model") or "gpt-5-nano",
                "client_grid_url": resolved_grid_url,
                "ats_check": request.ats_check,
                "job_extract": request.job_extract,
                "job_monitoring": request.job_monitoring,
                "requested_capability": requested_capability,
            },
            summary={
                "total_urls": len(request.urls),
                "assigned_agent_count": request.agent_count,
                "processed_url_count": 0,
                "completed_domain_count": 0,
                "failed_domain_count": 0,
                "queued_url_count": len(request.urls),
                "running_url_count": 0,
                "stopped_url_count": 0,
            },
            created_at=now,
            updated_at=now,
        )

        run_items = [
            ProcessRunItemDocument(
                process_id=process_id,
                client_key=client["client_key"],
                client_name=client["client_name"],
                raw_url=url,
                domain_key=self._normalize_domain_key(url),
                requested_capability=requested_capability,
                status="queued",
                created_at=now,
                updated_at=now,
            ).model_dump(mode="json")
            for url in request.urls
        ]

        await self._mongodb_service.insert_process_run(run_document.model_dump(mode="json"))
        await self._mongodb_service.insert_process_run_items(run_items)

        log_event(
            logger,
            "info",
            "process_submission_completed process_id=%s client_key=%s url_count=%s",
            process_id,
            client["client_key"],
            len(request.urls),
            domain=normalized_urls[0] if normalized_urls else client["client_key"],
            process_id=process_id,
            client_key=client["client_key"],
            url_count=len(request.urls),
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

        domain = (((process.get("request") or {}).get("urls") or ["unknown"])[0])
        request = JobProcessRequest(**process["request"])
        assignments = process.get("assignments", [])
        client = await self._require_active_client_by_key(process["client_key"])

        await self._mongodb_service.update_process_run(
            process_id,
            {
                "status": "running",
                "started_at": datetime.utcnow(),
                "errors": [],
            },
        )

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
            api_key=client.get("api_key"),
            model=client.get("model") or "gpt-5-nano",
        )
        try:
            result = await self._execute_process(
                process_id=process_id,
                request=request,
                assignments=assignments,
                client_key=process["client_key"],
                client_name=process["client_name"],
                grid_url=str((process.get("metadata") or {}).get("client_grid_url") or client.get("grid_url") or self._settings.selenium_remote_url),
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
            raise ValueError("Original process not found")
        original_status = str(original_process.get("status") or "").strip().lower()
        if original_status in {"running", "queued", "stop_requested"}:
            raise ValueError(
                f"Process {original_process_id} is currently {original_status} and cannot be rerun yet."
            )

        request = JobProcessRequest(**dict(original_process.get("request") or {}))
        client = await self._require_active_client(request.client_name)
        resolved_grid_url = client.get("grid_url") or self._settings.selenium_remote_url
        assignments = allocate_urls_to_agents(request.urls, request.agent_count)
        process_id = str(uuid4())
        requested_capability = self._requested_capability_for_request(request)
        now = datetime.utcnow()

        run_document = ProcessRunDocument(
            process_id=process_id,
            client_key=client["client_key"],
            client_name=client["client_name"],
            status="queued",
            request=request.model_copy(update={"task_id": process_id, "client_name": client["client_name"]}),
            assignments=assignments,
            queued_urls=list(request.urls),
            metadata={
                "client_model": client.get("model") or "gpt-5-nano",
                "client_grid_url": resolved_grid_url,
                "ats_check": request.ats_check,
                "job_extract": request.job_extract,
                "job_monitoring": request.job_monitoring,
                "requested_capability": requested_capability,
                "workflow_mode": "rerun",
                "rerun_of_process_id": original_process_id,
            },
            summary={
                "total_urls": len(request.urls),
                "assigned_agent_count": request.agent_count,
                "processed_url_count": 0,
                "completed_domain_count": 0,
                "failed_domain_count": 0,
                "queued_url_count": len(request.urls),
                "running_url_count": 0,
                "stopped_url_count": 0,
            },
            created_at=now,
            updated_at=now,
        )

        run_items = [
            ProcessRunItemDocument(
                process_id=process_id,
                client_key=client["client_key"],
                client_name=client["client_name"],
                raw_url=url,
                domain_key=self._normalize_domain_key(url),
                requested_capability=requested_capability,
                status="queued",
                created_at=now,
                updated_at=now,
            ).model_dump(mode="json")
            for url in request.urls
        ]

        await self._mongodb_service.insert_process_run(run_document.model_dump(mode="json"))
        await self._mongodb_service.insert_process_run_items(run_items)
        return run_document.model_dump(mode="json")

    async def execute_rerun_process(self, rerun_process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_run(rerun_process_id)
        if process is None:
            raise ValueError(f"Unknown process_id: {rerun_process_id}")

        original_process_id = str((process.get("metadata") or {}).get("rerun_of_process_id") or "").strip()
        if not original_process_id:
            raise ValueError("Rerun source process is not configured")

        original_process = await self._mongodb_service.get_process_run_with_items(original_process_id)
        if original_process is None:
            raise ValueError("Original process not found")

        request = JobProcessRequest(**process["request"])
        assignments = process.get("assignments", [])
        client = await self._require_active_client_by_key(process["client_key"])

        await self._mongodb_service.update_process_run(
            rerun_process_id,
            {
                "status": "running",
                "started_at": datetime.utcnow(),
                "errors": [],
            },
        )

        runtime_tokens = set_openai_runtime_config(
            api_key=client.get("api_key"),
            model=client.get("model") or "gpt-5-nano",
        )
        try:
            result = await self._execute_rerun_process(
                process_id=rerun_process_id,
                request=request,
                assignments=assignments,
                client_key=process["client_key"],
                client_name=process["client_name"],
                grid_url=str((process.get("metadata") or {}).get("client_grid_url") or client.get("grid_url") or self._settings.selenium_remote_url),
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

    async def register_client(
        self,
        client_name: str,
        api_key: str,
        model: str = "gpt-5-nano",
        grid_url: str | None = None,
    ) -> dict[str, Any]:
        client_key = self._build_client_key(client_name)
        validation = await validate_openai_api_key(api_key=api_key, model=model)
        if not validation.active:
            raise ValueError(validation.user_message or "The OpenAI API key could not be validated.")

        client = await self._mongodb_service.upsert_client_configuration(
            client_key=client_key,
            client_name=client_name,
            api_key=api_key,
            model=model,
            grid_url=grid_url or self._settings.selenium_remote_url,
            api_key_status="active",
            api_key_validation_error=None,
        )
        return self._sanitize_client_document(client)

    async def update_client(
        self,
        current_client_name: str,
        *,
        new_client_name: str | None = None,
        api_key: str | None = None,
        model: str | None = None,
        grid_url: str | None = None,
    ) -> dict[str, Any]:
        current_client = await self._require_client(current_client_name)
        final_client_name = new_client_name or current_client["client_name"]
        final_model = model or current_client.get("model") or "gpt-5-nano"
        final_api_key = api_key or current_client.get("api_key")
        final_grid_url = grid_url if grid_url is not None else current_client.get("grid_url") or self._settings.selenium_remote_url
        if not final_api_key:
            raise ValueError("Client does not have an API key configured")

        validation = await validate_openai_api_key(api_key=final_api_key, model=final_model)
        if not validation.active:
            raise ValueError(validation.user_message or "The OpenAI API key could not be validated.")

        updated = await self._mongodb_service.update_client_configuration(
            current_client_key=current_client["client_key"],
            new_client_key=self._build_client_key(final_client_name),
            client_name=final_client_name,
            api_key=final_api_key,
            model=final_model,
            grid_url=final_grid_url,
            api_key_status="active",
            api_key_validation_error=None,
        )
        if updated is None:
            raise ValueError(f"Unknown client: {current_client_name}")
        return self._sanitize_client_document(updated)

    async def get_client_configuration(self, client_name: str) -> dict[str, Any]:
        client = await self._require_client(client_name)
        return self._sanitize_client_document(client)

    async def list_clients(self) -> dict[str, Any]:
        clients = await self._mongodb_service.list_clients()
        return {
            "count": len(clients),
            "clients": [self._sanitize_client_document(client) for client in clients],
        }

    async def list_processes(
        self,
        client_name: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        client = await self._require_client(client_name)
        processes, total = await self._mongodb_service.list_process_runs_for_client(
            client["client_key"],
            page=page,
            page_size=page_size,
        )
        return {
            "client_key": client["client_key"],
            "client_name": client["client_name"],
            "count": len(processes),
            "total": total,
            "page": page,
            "page_size": page_size,
            "has_next": page * page_size < total,
            "has_previous": page > 1,
            "processes": processes,
        }

    async def get_client_overview(self, client_name: str) -> dict[str, Any]:
        client = await self._require_client(client_name)
        client_key = client["client_key"]
        subscriptions = await self._mongodb_service.get_client_domains(client_key)
        runs, _ = await self._mongodb_service.list_process_runs_for_client(client_key)
        return {
            "client_key": client_key,
            "client_name": client["client_name"],
            "subscriptions": subscriptions,
            "process_runs": runs,
        }

    async def get_client_jobs(self, client_name: str, limit: int = 500) -> dict[str, Any]:
        client = await self._require_client(client_name)
        client_key = client["client_key"]
        jobs = await self._mongodb_service.list_client_jobs(client_key, limit=limit)
        return {
            "client_key": client_key,
            "client_name": client["client_name"],
            "count": len(jobs),
            "jobs": jobs,
        }

    async def get_process_jobs(self, process_id: str, limit: int = 500) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_run(process_id)
        if process is None:
            raise ValueError("Process not found")
        jobs = await self._mongodb_service.list_client_jobs_for_process(process_id, limit=limit)
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
                },
            }
            for assignment in assignments
        ]

        try:
            worker_results = await asyncio.gather(*[self._run_agent(worker_input) for worker_input in worker_inputs])
        finally:
            # await close_shared_session_async(shared_runtime.session_id)
            pass

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
                            original_item=original_item_map.get(self._normalize_domain_key(url)),
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
        mark_running: bool = True,
    ) -> dict[str, Any]:
        domain_key = self._normalize_domain_key(url)
        main_domain = extract_domain(url)
        if mark_running:
            await self._mongodb_service.mark_url_running(process_id, url, agent_index)

        existing_domain = await self._mongodb_service.get_domain(domain_key)
        reused_career_discovery = False
        reused_ats_detection = False

        try:
            career_url_result = {}
            cached_career_result = ((existing_domain or {}).get("career_url_extraction") or {})
            cached_career_urls = list(cached_career_result.get("career_urls") or [])
            if cached_career_result.get("status") == "career_urls_found" and cached_career_urls:
                career_url_result = cached_career_result
                reused_career_discovery = True
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

            fingerprint_source = career_page_result.get("career_pages_analysis") or career_page_result
            page_fingerprint = self._fingerprint_payload(fingerprint_source)
            previous_fingerprint = (existing_domain or {}).get("latest_page_fingerprint")
            content_changed = None if previous_fingerprint is None else previous_fingerprint != page_fingerprint

            if ats_check:
                cached_ats_detection = ((existing_domain or {}).get("ats_detection") or {})
                if cached_ats_detection and cached_ats_detection.get("confidence") == "high":
                    ats_detection = cached_ats_detection
                    reused_ats_detection = True
                else:
                    ats_detection = await detect_ats(
                        career_page_result,
                        main_domain or domain_key,
                        browser_session,
                        agent_index,
                        agent_tab,
                    )
            else:
                ats_detection = {
                    "ats_detected": None,
                    "detection_method": "skipped",
                    "reasoning": "ATS check disabled for this process.",
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
                career_page_result=career_page_result,
                ats_detection=ats_detection,
                jobs_extraction=jobs_extraction,
                status="completed",
            ).model_dump(mode="json")

            domain_check_id = str(uuid4())
            result_summary = self._build_result_summary(
                record,
                reused_career_discovery=reused_career_discovery,
                reused_ats_detection=reused_ats_detection,
                content_changed=content_changed,
                job_monitoring=job_monitoring,
            )
            await self._mongodb_service.insert_domain_check(
                DomainCheckDocument(
                    domain_check_id=domain_check_id,
                    process_id=process_id,
                    client_key=client_key,
                    client_name=client_name,
                    raw_url=url,
                    domain_key=domain_key,
                    requested_capability=requested_capability,  # type: ignore[arg-type]
                    content_changed=content_changed,
                    page_fingerprint=page_fingerprint,
                    llm_skipped=not ats_check or reused_ats_detection,
                    result_payload=record,
                ).model_dump(mode="json")
            )

            await self._mongodb_service.upsert_domain(
                domain_key,
                {
                    "career_url_extraction": career_url_result,
                    "career_page_result": career_page_result,
                    "ats_detection": ats_detection,
                    "jobs_extraction_summary": {
                        "status": jobs_extraction.get("status"),
                        "requested": jobs_extraction.get("requested"),
                        "job_count": jobs_extraction.get("job_count"),
                        "source_count": jobs_extraction.get("source_count"),
                        "reused_source_count": jobs_extraction.get("reused_source_count"),
                    },
                    "latest_page_fingerprint": page_fingerprint,
                    "latest_extracted_text": self._extract_latest_page_text(career_page_result),
                    "last_career_discovery_at": datetime.utcnow(),
                    "last_career_check_at": datetime.utcnow(),
                    "last_ats_check_at": datetime.utcnow() if ats_check else (existing_domain or {}).get("last_ats_check_at"),
                    "last_job_extract_at": datetime.utcnow() if job_extract else (existing_domain or {}).get("last_job_extract_at"),
                },
            )

            await self._mongodb_service.mark_url_completed(
                process_id,
                url,
                result_summary=result_summary,
                result_payload=record,
                domain_check_id=domain_check_id,
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
        existing_domain = await self._mongodb_service.get_domain(domain_key)

        try:
            fresh_career_url_result = await career_url_extraction_node(main_domain or domain_key, browser_session)
            career_url_result = self._select_rerun_career_url_result(
                fresh_result=fresh_career_url_result,
                previous_result=previous_career_url_extraction,
            )

            previous_not_job_related_urls = list(
                ((previous_career_page_result.get("overview") or {}).get("not_job_related_urls") or [])
            )
            candidate_career_urls = self._filter_rerun_career_urls(
                career_url_result.get("career_urls") or [],
                previous_not_job_related_urls,
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

            ats_detection: dict[str, Any]
            if ats_check:
                if not content_changed:
                    ats_detection = {
                        **previous_ats_detection,
                        "detection_method": "rerun_skipped_no_career_change",
                        "reasoning": "Career content did not change during rerun, so ATS check was skipped.",
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
            else:
                ats_detection = {
                    "ats_detected": None,
                    "detection_method": "skipped",
                    "reasoning": "ATS check disabled for this process.",
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
                career_page_result=career_page_result,
                ats_detection=ats_detection,
                jobs_extraction=jobs_extraction,
                status="completed",
            ).model_dump(mode="json")
            record["rerun_metadata"] = {
                "reused_analysis_count": reused_analysis_count,
                "changed_page_count": changed_page_count,
                "content_changed": content_changed,
            }

            domain_check_id = str(uuid4())
            result_summary = self._build_result_summary(
                record,
                reused_career_discovery=bool(career_url_result.get("used_previous_career_urls")),
                reused_ats_detection=bool(ats_detection.get("reused")),
                content_changed=content_changed,
                job_monitoring=job_monitoring,
            )

            await self._mongodb_service.insert_domain_check(
                DomainCheckDocument(
                    domain_check_id=domain_check_id,
                    process_id=process_id,
                    client_key=client_key,
                    client_name=client_name,
                    raw_url=url,
                    domain_key=domain_key,
                    requested_capability=requested_capability,  # type: ignore[arg-type]
                    content_changed=content_changed,
                    page_fingerprint=self._fingerprint_payload(career_page_result),
                    llm_skipped=not content_changed,
                    result_payload=record,
                ).model_dump(mode="json")
            )

            await self._mongodb_service.upsert_domain(
                domain_key,
                {
                    "career_url_extraction": career_url_result,
                    "career_page_result": career_page_result,
                    "ats_detection": ats_detection,
                    "jobs_extraction_summary": {
                        "status": jobs_extraction.get("status"),
                        "requested": jobs_extraction.get("requested"),
                        "job_count": jobs_extraction.get("job_count"),
                        "source_count": jobs_extraction.get("source_count"),
                        "reused_source_count": jobs_extraction.get("reused_source_count"),
                    },
                    "latest_page_fingerprint": self._fingerprint_payload(career_page_result),
                    "latest_extracted_text": self._extract_latest_page_text(career_page_result),
                    "last_career_discovery_at": datetime.utcnow(),
                    "last_career_check_at": datetime.utcnow(),
                    "last_ats_check_at": datetime.utcnow() if ats_check and content_changed else (existing_domain or {}).get("last_ats_check_at"),
                    "last_job_extract_at": datetime.utcnow() if job_extract and content_changed else (existing_domain or {}).get("last_job_extract_at"),
                },
            )

            await self._mongodb_service.mark_url_completed(
                process_id,
                url,
                result_summary=result_summary,
                result_payload=record,
                domain_check_id=domain_check_id,
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

    def _build_client_key(self, client_name: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", "_", client_name.strip().lower()).strip("_")
        return normalized or "default_client"

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

    def _filter_rerun_career_urls(self, urls: list[str], not_job_related_urls: list[str]) -> list[str]:
        excluded = {self._normalize_url_for_compare(url) for url in not_job_related_urls}
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

    async def _require_client(self, client_name: str) -> dict[str, Any]:
        client_key = self._build_client_key(client_name)
        client = await self._mongodb_service.get_client(client_key)
        if client is None:
            raise ValueError(f"Client '{client_name}' is not registered")
        return client

    async def _require_active_client(self, client_name: str) -> dict[str, Any]:
        client = await self._require_client(client_name)
        if str(client.get("api_key_status") or "").lower() != "active":
            raise ValueError(f"Client '{client_name}' does not have an active API key")
        if not client.get("api_key"):
            raise ValueError(f"Client '{client_name}' does not have an API key configured")
        return client

    async def _require_active_client_by_key(self, client_key: str) -> dict[str, Any]:
        client = await self._mongodb_service.get_client(client_key)
        if client is None:
            raise ValueError(f"Client '{client_key}' is not registered")
        if str(client.get("api_key_status") or "").lower() != "active":
            raise ValueError(f"Client '{client.get('client_name') or client_key}' does not have an active API key")
        if not client.get("api_key"):
            raise ValueError(f"Client '{client.get('client_name') or client_key}' does not have an API key configured")
        return client

    def _sanitize_client_document(self, client: dict[str, Any]) -> dict[str, Any]:
        sanitized = dict(client)
        sanitized["api_key"] = mask_api_key(client.get("api_key"))
        return sanitized

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
