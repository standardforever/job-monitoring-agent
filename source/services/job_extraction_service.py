from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Literal

from prompts.job_detail_to_json_prompt import build_job_detail_to_json_prompt
from services.content_extraction import extract_page_content
from services.flow_safety import has_skip_extension, is_web_navigation_url
from services.mongodb_service import MongoDBService
from services.navigation import navigate_to_url
from services.openai_service import OpenAIAnalysisService
from utils.logging import get_logger, log_event

logger = get_logger("job_extraction_service")

SourceType = Literal["job_url", "embedded_page"]


@dataclass(slots=True)
class JobExtractionSource:
    source_type: SourceType
    source_url: str
    page_fingerprint: str | None = None
    extracted_markdown: str | None = None
    page_title: str | None = None


class BaseJobExtractionStrategy:
    strategy_name = "base"

    async def supports(self, source: JobExtractionSource, domain_key: str) -> bool:
        return False

    async def extract_jobs(
        self,
        *,
        source: JobExtractionSource,
        domain_key: str,
        extracted_markdown: str,
        page_url: str,
    ) -> dict[str, Any]:
        raise NotImplementedError


class LLMJobExtractionStrategy(BaseJobExtractionStrategy):
    strategy_name = "llm_job_detail_to_json"

    async def supports(self, source: JobExtractionSource, domain_key: str) -> bool:
        return True

    async def extract_jobs(
        self,
        *,
        source: JobExtractionSource,
        domain_key: str,
        extracted_markdown: str,
        page_url: str,
    ) -> dict[str, Any]:
        prompt = build_job_detail_to_json_prompt(extracted_markdown, page_url=page_url)
        service = OpenAIAnalysisService()
        analysis = await service.analyze_data(prompt=prompt, json_response=True)
        if not analysis.success:
            raise RuntimeError(f"Job extraction failed: {analysis.error}")

        jobs = analysis.response.get("jobs")
        if not isinstance(jobs, list):
            raise RuntimeError("Job extraction response did not contain a valid jobs array")

        return {
            "jobs": jobs,
            "token_usage": analysis.token_usage,
            "strategy_name": self.strategy_name,
        }


class JobExtractionService:
    def __init__(
        self,
        mongodb_service: MongoDBService,
        custom_strategies: list[BaseJobExtractionStrategy] | None = None,
    ) -> None:
        self._mongodb_service = mongodb_service
        self._custom_strategies = list(custom_strategies or [])
        self._fallback_strategy = LLMJobExtractionStrategy()

    async def extract_jobs_for_domain(
        self,
        *,
        process_id: str,
        client_key: str,
        client_name: str,
        raw_url: str,
        domain_key: str,
        career_page_result: dict[str, Any],
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
    ) -> dict[str, Any]:
        sources = self._collect_sources(career_page_result)
        extracted_jobs: list[dict[str, Any]] = []
        source_summaries: list[dict[str, Any]] = []
        reused_source_count = 0
        llm_source_count = 0
        skipped_source_count = 0

        for source in sources:
            source_summary = {
                "source_type": source.source_type,
                "source_url": source.source_url,
                "page_fingerprint": source.page_fingerprint,
                "status": "pending",
                "job_count": 0,
                "reused": False,
                "strategy": None,
                "error": None,
            }

            try:
                cache_key = self._build_cache_key(source)
                cached_extraction = await self._mongodb_service.get_job_extraction_cache(cache_key)
                if cached_extraction:
                    jobs = list(cached_extraction.get("jobs") or [])
                    source_summary["status"] = "reused"
                    source_summary["job_count"] = len(jobs)
                    source_summary["reused"] = True
                    source_summary["strategy"] = cached_extraction.get("extraction_strategy")
                    reused_source_count += 1
                else:
                    extracted_markdown, page_url, page_fingerprint = await self._prepare_source_content(
                        source=source,
                        browser_session=browser_session,
                        agent_index=agent_index,
                        agent_tab=agent_tab,
                    )
                    source.page_fingerprint = page_fingerprint or source.page_fingerprint
                    strategy = await self._select_strategy(source, domain_key)
                    extraction_result = await strategy.extract_jobs(
                        source=source,
                        domain_key=domain_key,
                        extracted_markdown=extracted_markdown,
                        page_url=page_url,
                    )
                    jobs = self._normalize_jobs(extraction_result.get("jobs"))
                    source_summary["status"] = "extracted"
                    source_summary["job_count"] = len(jobs)
                    source_summary["strategy"] = extraction_result.get("strategy_name")
                    llm_source_count += 1
                    await self._mongodb_service.upsert_job_extraction_cache(
                        cache_key,
                        {
                            "domain_key": domain_key,
                            "source_type": source.source_type,
                            "source_url": source.source_url,
                            "page_fingerprint": source.page_fingerprint,
                            "extraction_strategy": extraction_result.get("strategy_name"),
                            "jobs": jobs,
                        },
                    )

                stored_count = 0
                skipped_not_job_count = 0
                for job in jobs:
                    normalized_job = self._normalize_job_record(job, source)
                    if not normalized_job.get("is_job_page", True):
                        skipped_not_job_count += 1
                        continue

                    job_key = self._build_job_key(domain_key, source, normalized_job)
                    await self._mongodb_service.upsert_job(
                        job_key,
                        {
                            "domain_key": domain_key,
                            "source_type": source.source_type,
                            "source_url": source.source_url,
                            "page_fingerprint": source.page_fingerprint,
                            "extraction_strategy": source_summary.get("strategy") or self._fallback_strategy.strategy_name,
                            "title": normalized_job.get("title"),
                            "company_name": normalized_job.get("company_name"),
                            "structured_job": normalized_job,
                        },
                    )
                    await self._mongodb_service.upsert_client_job(
                        client_key=client_key,
                        client_name=client_name,
                        domain_key=domain_key,
                        raw_url=raw_url,
                        process_id=process_id,
                        job_key=job_key,
                        document={
                            "source_type": source.source_type,
                            "source_url": source.source_url,
                            "page_fingerprint": source.page_fingerprint,
                            "title": normalized_job.get("title"),
                            "company_name": normalized_job.get("company_name"),
                            "job_data": normalized_job,
                        },
                    )
                    extracted_jobs.append(
                        {
                            "job_key": job_key,
                            "title": normalized_job.get("title"),
                            "company_name": normalized_job.get("company_name"),
                            "source_type": source.source_type,
                            "source_url": source.source_url,
                            "reused_source": bool(source_summary["reused"]),
                        }
                    )
                    stored_count += 1

                source_summary["stored_job_count"] = stored_count
                source_summary["skipped_not_job_page_count"] = skipped_not_job_count
            except Exception as exc:
                skipped_source_count += 1
                source_summary["status"] = "failed"
                source_summary["error"] = str(exc)
                log_event(
                    logger,
                    "warning",
                    "job_source_extraction_failed domain_key=%s source_url=%s error=%s",
                    domain_key,
                    source.source_url,
                    str(exc),
                    domain=domain_key,
                    source_url=source.source_url,
                    error=str(exc),
                )

            source_summaries.append(source_summary)

        return {
            "status": "completed" if source_summaries else "skipped",
            "requested": True,
            "source_count": len(source_summaries),
            "reused_source_count": reused_source_count,
            "llm_source_count": llm_source_count,
            "skipped_source_count": skipped_source_count,
            "job_count": len(extracted_jobs),
            "jobs": extracted_jobs,
            "sources": source_summaries,
        }

    async def _select_strategy(self, source: JobExtractionSource, domain_key: str) -> BaseJobExtractionStrategy:
        for strategy in self._custom_strategies:
            if await strategy.supports(source, domain_key):
                return strategy
        return self._fallback_strategy

    def _collect_sources(self, career_page_result: dict[str, Any]) -> list[JobExtractionSource]:
        overview = dict(career_page_result.get("overview") or {})
        analysis_pages = list(career_page_result.get("career_pages_analysis") or [])
        seen: set[tuple[str, str, str | None]] = set()
        sources: list[JobExtractionSource] = []

        for job_url in overview.get("job_urls") or []:
            normalized_job_url = str(job_url or "").strip()
            if not normalized_job_url or not is_web_navigation_url(normalized_job_url) or has_skip_extension(normalized_job_url):
                continue
            marker = ("job_url", normalized_job_url, None)
            if marker in seen:
                continue
            seen.add(marker)
            sources.append(JobExtractionSource(source_type="job_url", source_url=normalized_job_url))

        for page in analysis_pages:
            if not page.get("embedded_jobs_present"):
                continue
            source_url = str(page.get("extracted_url") or page.get("current_url") or page.get("url") or "").strip()
            extracted_markdown = str(page.get("extracted_content") or "").strip()
            if not source_url or not extracted_markdown:
                continue
            page_fingerprint = self._fingerprint_text(extracted_markdown)
            marker = ("embedded_page", source_url, page_fingerprint)
            if marker in seen:
                continue
            seen.add(marker)
            sources.append(
                JobExtractionSource(
                    source_type="embedded_page",
                    source_url=source_url,
                    page_fingerprint=page_fingerprint,
                    extracted_markdown=extracted_markdown,
                )
            )

        return sources

    async def _prepare_source_content(
        self,
        *,
        source: JobExtractionSource,
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
    ) -> tuple[str, str, str | None]:
        if source.source_type == "embedded_page" and source.extracted_markdown:
            return source.extracted_markdown, source.source_url, source.page_fingerprint

        navigation_result = await navigate_to_url(
            browser_session.page if browser_session is not None else None,
            agent_index=agent_index,
            tab_handle=agent_tab["handle"],
            url=source.source_url,
            post_navigation_delay_ms=0,
        )
        if navigation_result["status"] != "navigated":
            raise RuntimeError(
                f"Unable to navigate to job source {source.source_url}: {navigation_result.get('error') or navigation_result['status']}"
            )

        extracted = await extract_page_content(
            browser_session.page if browser_session is not None else None,
            sections=["body"],
        )
        if extracted is None or not extracted.get("markdown"):
            raise RuntimeError(f"Unable to extract job content from {source.source_url}")

        markdown = str(extracted.get("markdown") or "").strip()
        page_url = str(extracted.get("url") or source.source_url)
        return markdown, page_url, self._fingerprint_text(markdown)

    def _normalize_jobs(self, jobs: Any) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for job in list(jobs or []):
            if isinstance(job, dict):
                normalized.append(job)
        return normalized

    def _normalize_job_record(self, job: dict[str, Any], source: JobExtractionSource) -> dict[str, Any]:
        normalized_job = dict(job)
        application_method = dict(normalized_job.get("application_method") or {})
        if not application_method.get("url") and source.source_type == "job_url":
            application_method["url"] = source.source_url
        normalized_job["application_method"] = application_method
        normalized_job["source_url"] = source.source_url
        normalized_job["source_type"] = source.source_type
        return normalized_job

    def _build_cache_key(self, source: JobExtractionSource) -> str:
        if source.source_type == "embedded_page":
            return self._fingerprint_text(
                json.dumps(
                    {
                        "source_type": source.source_type,
                        "source_url": source.source_url,
                        "page_fingerprint": source.page_fingerprint,
                    },
                    sort_keys=True,
                )
            )
        return self._fingerprint_text(
            json.dumps(
                {
                    "source_type": source.source_type,
                    "source_url": source.source_url,
                },
                sort_keys=True,
            )
        )

    def _build_job_key(self, domain_key: str, source: JobExtractionSource, job: dict[str, Any]) -> str:
        location = dict(job.get("location") or {})
        closing_date = dict(job.get("closing_date") or {})
        signature = {
            "domain_key": domain_key,
            "source_type": source.source_type,
            "source_url": source.source_url,
            "title": str(job.get("title") or "").strip().lower(),
            "company_name": str(job.get("company_name") or "").strip().lower(),
            "city": str(location.get("city") or "").strip().lower(),
            "region": str(location.get("region") or "").strip().lower(),
            "closing_date": str(closing_date.get("iso_format") or closing_date.get("raw_text") or "").strip().lower(),
        }
        return self._fingerprint_text(json.dumps(signature, sort_keys=True, ensure_ascii=False))

    def _fingerprint_text(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()
