from __future__ import annotations

import asyncio

from models.process import JobProcessRequest
from services.job_process_service import JobProcessService
from utils.logging import get_logger, log_event

logger = get_logger("main")


async def run_job_pipeline(
    *,
    client_name: str = "default_client",
    urls: list[str],
    agent_count: int = 1,
    ats_check: bool = True,
    job_extract: bool = False,
    job_monitoring: bool = False,
    task_id: str | None = None,
) -> dict:
    log_event(
        logger,
        "info",
        "run_job_pipeline_started url_count=%s agent_count=%s",
        len(urls),
        agent_count,
        domain=urls[0] if urls else "unknown",
        client_name=client_name,
        url_count=len(urls),
        agent_count=agent_count,
        ats_check=ats_check,
        job_extract=job_extract,
        job_monitoring=job_monitoring,
        task_id=task_id,
    )
    service = JobProcessService()
    request = JobProcessRequest(
        client_name=client_name,
        urls=urls,
        agent_count=agent_count,
        ats_check=ats_check,
        job_extract=job_extract,
        job_monitoring=job_monitoring,
        task_id=task_id,
    )
    result = await service.run_process(request=request, process_id=task_id)
    log_event(
        logger,
        "info",
        "run_job_pipeline_completed status=%s",
        result.get("status"),
        domain=urls[0] if urls else "unknown",
        status=result.get("status"),
        task_id=result.get("process_id"),
    )
    return result


async def main() -> None:
    urls = [
        "coventry2021.co.uk",
        "https://www.claptongirlsacademy.com/recruitment",
    ]
    log_event(
        logger,
        "info",
        "main_entrypoint_started sample_url_count=%s",
        len(urls),
        domain=urls[0],
        sample_url_count=len(urls),
    )
    await run_job_pipeline(
        urls=urls,
        agent_count=2,
        ats_check=True,
        job_extract=False,
        job_monitoring=False,
    )


if __name__ == "__main__":
    asyncio.run(main())
