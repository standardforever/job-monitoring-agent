from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any
from uuid import uuid4

from models.process import DomainProcessRecord, JobProcessDocument, JobProcessRequest, WorkerProcessResult
from nodes.ats_check_node import detect_ats
from nodes.career_page_category import career_page_category_node
from nodes.session_bootstrap import bootstrap_browser_node
from nodes.url_extraction import career_url_extraction_node
from services.agent_allocator import allocate_urls_to_agents
from services.flow_safety import extract_domain
from services.grid_session import close_agent_tab, create_session_async
from services.mongodb_service import MongoDBService
from utils.logging import configure_logging, get_logger, log_event

logger = get_logger("job_process_service")


class JobProcessService:
    def __init__(self, mongodb_service: MongoDBService | None = None) -> None:
        self._mongodb_service = mongodb_service or MongoDBService()
        log_event(logger, "info", "job_process_service_initialized", domain="service")

    async def submit_process(self, request: JobProcessRequest) -> dict[str, Any]:
        assignments = allocate_urls_to_agents(request.urls, request.agent_count)
        process_id = request.task_id or str(uuid4())
        log_event(
            logger,
            "info",
            "process_submission_started process_id=%s url_count=%s agent_count=%s",
            process_id,
            len(request.urls),
            request.agent_count,
            domain=request.urls[0] if request.urls else "unknown",
            process_id=process_id,
            url_count=len(request.urls),
            agent_count=request.agent_count,
        )
        document = JobProcessDocument(
            process_id=process_id,
            status="queued",
            request=request,
            assignments=assignments,
            queued_urls=list(request.urls),
            metadata={
                "headless": False,
                "ats_check": request.ats_check,
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
        )
        await self._mongodb_service.insert_process(document.model_dump(mode="json"))
        log_event(
            logger,
            "info",
            "process_submission_completed process_id=%s",
            process_id,
            domain=request.urls[0] if request.urls else "unknown",
            process_id=process_id,
        )
        return document.model_dump(mode="json")

    async def run_process(self, request: JobProcessRequest, process_id: str | None = None) -> dict[str, Any]:
        configure_logging()
        log_event(
            logger,
            "info",
            "run_process_started requested_process_id=%s",
            process_id or request.task_id,
            domain=request.urls[0] if request.urls else "unknown",
            requested_process_id=process_id or request.task_id,
        )

        submitted_process = await self.submit_process(
            request.model_copy(update={"task_id": process_id or request.task_id})
        )
        active_process_id = submitted_process["process_id"]

        await self._mongodb_service.update_process(
            active_process_id,
            {
                "status": "running",
                "started_at": datetime.utcnow(),
            },
        )

        try:
            result = await self._execute_process(active_process_id, request, submitted_process["assignments"])
        except Exception as exc:
            log_event(
                logger,
                "error",
                "run_process_failed process_id=%s error=%s",
                active_process_id,
                str(exc),
                domain=request.urls[0] if request.urls else "unknown",
                process_id=active_process_id,
                error=str(exc),
            )
            await self._mongodb_service.update_process(
                active_process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [str(exc)],
                },
            )
            raise

        await self._mongodb_service.update_process(
            active_process_id,
            {
                "status": result["status"],
                "completed_at": datetime.utcnow(),
                "errors": result["errors"],
                "summary": result["summary"],
            },
        )
        log_event(
            logger,
            "info",
            "run_process_completed process_id=%s status=%s",
            active_process_id,
            result["status"],
            domain=request.urls[0] if request.urls else "unknown",
            process_id=active_process_id,
            status=result["status"],
        )
        return result

    async def execute_existing_process(self, process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.get_process(process_id)
        if process is None:
            raise ValueError(f"Unknown process_id: {process_id}")
        domain = (((process.get("request") or {}).get("urls") or ["unknown"])[0])
        log_event(
            logger,
            "info",
            "execute_existing_process_started process_id=%s",
            process_id,
            domain=domain,
            process_id=process_id,
        )

        request = JobProcessRequest(**process["request"])
        assignments = process.get("assignments", [])

        await self._mongodb_service.update_process(
            process_id,
            {
                "status": "running",
                "started_at": datetime.utcnow(),
                "errors": [],
            },
        )

        try:
            result = await self._execute_process(process_id, request, assignments)
        except Exception as exc:
            log_event(
                logger,
                "error",
                "execute_existing_process_failed process_id=%s error=%s",
                process_id,
                str(exc),
                domain=domain,
                process_id=process_id,
                error=str(exc),
            )
            await self._mongodb_service.update_process(
                process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [str(exc)],
                },
            )
            raise

        await self._mongodb_service.update_process(
            process_id,
            {
                "status": result["status"],
                "completed_at": datetime.utcnow(),
                "errors": result["errors"],
                "summary": result["summary"],
            },
        )
        log_event(
            logger,
            "info",
            "execute_existing_process_completed process_id=%s status=%s",
            process_id,
            result["status"],
            domain=domain,
            process_id=process_id,
            status=result["status"],
        )
        return result

    async def get_process(self, process_id: str) -> dict[str, Any] | None:
        return await self._mongodb_service.get_process(process_id)

    async def _execute_process(
        self,
        process_id: str,
        request: JobProcessRequest,
        assignments: list[dict[str, Any]],
    ) -> dict[str, Any]:
        log_event(
            logger,
            "info",
            "execute_process_started process_id=%s assignment_count=%s",
            process_id,
            len(assignments),
            domain=request.urls[0] if request.urls else "unknown",
            process_id=process_id,
            assignment_count=len(assignments),
        )
        session_info = await create_session_async(grid_url=request.grid_url)
        if session_info is None or not session_info.cdp_url:
            error = "Unable to establish shared Selenium/CDP session"
            log_event(
                logger,
                "error",
                "execute_process_session_failed process_id=%s error=%s",
                process_id,
                error,
                domain=request.urls[0] if request.urls else "unknown",
                process_id=process_id,
                error=error,
            )
            result = {
                "process_id": process_id,
                "status": "failed",
                "errors": [error],
                "worker_results": [],
                "summary": {
                    "total_urls": len(request.urls),
                    "assigned_agent_count": request.agent_count,
                    "processed_url_count": 0,
                    "completed_domain_count": 0,
                    "failed_domain_count": len(request.urls),
                    "queued_url_count": 0,
                    "running_url_count": 0,
                },
            }
            await self._mongodb_service.update_process(
                process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [error],
                    "summary": result["summary"],
                    "queued_urls": [],
                    "failed_urls": list(request.urls),
                },
            )
            return result

        worker_inputs = [
            {
                "process_id": process_id,
                "grid_url": request.grid_url,
                "agent_count": request.agent_count,
                "agent_index": assignment["agent_index"],
                "assigned_urls": assignment["urls"],
                "completed_urls": [],
                "errors": [],
                "session_id": session_info.session_id,
                "cdp_url": session_info.cdp_url,
                "metadata": {
                    "allocation_status": assignment["status"],
                    "allocated_url_count": assignment["url_count"],
                    "headless": False,
                    "ats_check": request.ats_check,
                    "reused_existing_session": session_info.reused_existing_session,
                    "task_id": process_id,
                },
            }
            for assignment in assignments
        ]

        worker_results = await asyncio.gather(
            *[self._run_agent(worker_input) for worker_input in worker_inputs]
        )

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
        log_event(
            logger,
            "info",
            "execute_process_completed process_id=%s status=%s completed_domain_count=%s failed_domain_count=%s",
            process_id,
            status,
            completed_domain_count,
            failed_domain_count,
            domain=request.urls[0] if request.urls else "unknown",
            process_id=process_id,
            status=status,
            completed_domain_count=completed_domain_count,
            failed_domain_count=failed_domain_count,
        )
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
        agent_index = int(graph_input["agent_index"])
        domain = assigned_urls[0] if assigned_urls else "unknown"
        browser_session = None

        if assigned_urls:
            await self._mongodb_service.update_assignment_status(process_id, agent_index, "running")

        bootstrap_result = await bootstrap_browser_node(state=graph_input)

        if not bootstrap_result.get("session_established"):
            errors = list(bootstrap_result.get("errors", []))
            for url in assigned_urls:
                await self._mongodb_service.mark_url_failed(process_id, url, was_running=False)
            if assigned_urls:
                await self._mongodb_service.update_assignment_status(process_id, agent_index, "completed")
            log_event(
                logger,
                "error",
                "worker_bootstrap_failed process_id=%s agent_index=%s",
                process_id,
                agent_index,
                domain=domain,
                process_id=process_id,
                agent_index=agent_index,
                errors=errors,
            )
            return WorkerProcessResult(
                agent_index=agent_index,
                status="failed",
                assigned_urls=assigned_urls,
                processed_urls=[],
                domain_results=[],
                errors=errors,
                metadata=dict(bootstrap_result.get("metadata", {})),
            ).model_dump(mode="json")

        browser_session = bootstrap_result.get("browser_session")
        agent_tab = bootstrap_result.get("agent_tab", {})

        try:
            log_event(
                logger,
                "info",
                "worker_start agent_index=%s assigned_url_count=%s",
                agent_index,
                len(assigned_urls),
                domain="run",
                agent_index=agent_index,
                assigned_url_count=len(assigned_urls),
                process_id=process_id,
            )

            domain_results: list[dict[str, Any]] = []
            errors: list[str] = []
            processed_urls: list[str] = []

            for url in assigned_urls:
                record = await self._process_domain(
                    process_id=process_id,
                    url=url,
                    browser_session=browser_session,
                    agent_index=agent_index,
                    agent_tab=agent_tab,
                    ats_check=bool((graph_input.get("metadata") or {}).get("ats_check", True)),
                )
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
            log_event(
                logger,
                "info",
                "worker_tab_closed process_id=%s agent_index=%s",
                process_id,
                agent_index,
                domain=domain,
                process_id=process_id,
                agent_index=agent_index,
            )

    async def _process_domain(
        self,
        *,
        process_id: str,
        url: str,
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
        ats_check: bool,
    ) -> dict[str, Any]:
        main_domain = extract_domain(url)
        await self._mongodb_service.mark_url_running(process_id, url)
        log_event(
            logger,
            "info",
            "domain_processing_started process_id=%s agent_index=%s url=%s",
            process_id,
            agent_index,
            url,
            domain=main_domain or url,
            process_id=process_id,
            agent_index=agent_index,
            url=url,
        )
        if not main_domain:
            record = DomainProcessRecord(
                domain=url,
                main_domain=None,
                status="failed",
                error="Unable to extract domain from URL",
            ).model_dump(mode="json")
            await self._mongodb_service.mark_url_failed(process_id, url)
            await self._mongodb_service.append_domain_result(process_id, agent_index, record)
            return record

        try:
            career_url_result = await career_url_extraction_node(main_domain, browser_session)
            career_page_result = await career_page_category_node(
                career_url_result.get("career_urls", []),
                browser_session,
                agent_index,
                agent_tab,
            )
            if ats_check:
                ats_detection = await detect_ats(
                    career_page_result,
                    main_domain,
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
                log_event(
                    logger,
                    "info",
                    "ats_check_skipped process_id=%s agent_index=%s",
                    process_id,
                    agent_index,
                    domain=main_domain,
                    process_id=process_id,
                    agent_index=agent_index,
                )

            record = DomainProcessRecord(
                domain=url,
                main_domain=main_domain,
                career_url_extraction=career_url_result,
                career_page_result=career_page_result,
                ats_detection=ats_detection,
                status="completed",
            ).model_dump(mode="json")
            await self._mongodb_service.mark_url_completed(process_id, url)
            log_event(
                logger,
                "info",
                "domain_processing_completed process_id=%s agent_index=%s status=%s",
                process_id,
                agent_index,
                record["status"],
                domain=main_domain,
                process_id=process_id,
                agent_index=agent_index,
                status=record["status"],
            )
        except Exception as exc:
            record = DomainProcessRecord(
                domain=url,
                main_domain=main_domain,
                status="failed",
                error=str(exc),
            ).model_dump(mode="json")
            await self._mongodb_service.mark_url_failed(process_id, url)
            log_event(
                logger,
                "error",
                "domain_processing_failed process_id=%s agent_index=%s error=%s",
                process_id,
                agent_index,
                str(exc),
                domain=main_domain,
                process_id=process_id,
                agent_index=agent_index,
                error=str(exc),
            )

        await self._mongodb_service.append_domain_result(process_id, agent_index, record)
        return record
