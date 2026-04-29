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
from nodes.career_page_category import career_page_category_node
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
            raise ValueError(f"Client API key validation failed: {validation.error}")

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
            raise ValueError(f"Client API key validation failed: {validation.error}")

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

    async def list_processes(self, limit: int = 100) -> list[dict[str, Any]]:
        return await self._mongodb_service.list_all_process_runs(limit=limit)

    async def get_client_overview(self, client_name: str) -> dict[str, Any]:
        client = await self._require_client(client_name)
        client_key = client["client_key"]
        subscriptions = await self._mongodb_service.get_client_domains(client_key)
        runs = await self._mongodb_service.list_process_runs_for_client(client_key)
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
        status = "completed" if not errors else "failed"

        return {
            "process_id": process_id,
            "status": status,
            "errors": errors,
            "worker_results": worker_results,
            "summary": {
                "total_urls": len(request.urls),
                "assigned_agent_count": request.agent_count,
                "processed_url_count": sum(len(worker["processed_urls"]) for worker in worker_results),
                "completed_domain_count": completed_domain_count,
                "failed_domain_count": failed_domain_count,
                "queued_url_count": 0,
                "running_url_count": 0,
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
                status="completed" if not errors else "failed",
                assigned_urls=assigned_urls,
                processed_urls=processed_urls,
                domain_results=[DomainProcessRecord(**record) for record in domain_results],
                errors=errors,
                metadata=dict(bootstrap_result.get("metadata", {})),
            ).model_dump(mode="json")
        finally:
            if assigned_urls:
                await self._mongodb_service.update_assignment_status(process_id, agent_index, "completed")
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
