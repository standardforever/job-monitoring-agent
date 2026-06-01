from __future__ import annotations

from typing import Any

from prompts.apply_url_prompt import build_apply_url_prompt
from services.content_extraction import extract_page_content
from services.flow_safety import _is_document_url, has_skip_extension, is_web_navigation_url
from services.navigation import navigate_to_url
from services.openai_service import OpenAIAnalysisService
from utils.logging import get_logger, log_event

logger = get_logger("apply_url_node")


async def get_apply_url(
    job_urls: list[str],
    browser_session: Any,
    agent_index: int,
    agent_tab: dict,
    main_domain: str,
) -> dict:
    """
    Loop through job_urls, scrape each page, and use the LLM to extract the
    apply URL / email / document link and means of application.

    Returns on the first accessible page that yields a usable result.
    Falls back to an aggregated result when all URLs are inaccessible.
    """
    if not job_urls:
        return _build_response("no_urls", {}, [], [])

    log_event(
        logger,
        "info",
        "apply_url_detection_started main_domain=%s url_count=%s",
        main_domain,
        len(job_urls),
        domain=main_domain,
        agent_index=agent_index,
        url_count=len(job_urls),
    )

    checked_urls: list[str] = []
    raw_checks: list[dict] = []

    for job_url in job_urls:
        if job_url in checked_urls:
            continue

        # Document URL — treat as the apply document directly (no navigation needed)
        if _is_document_url(job_url):
            result = {
                "page_accessible": True,
                "page_access_status": "accessible",
                "page_access_detail": None,
                "means_of_application": "document",
                "apply_url": None,
                "apply_email": None,
                "apply_document_url": job_url,
                "additional_methods": None,
                "confidence": "high",
                "reasoning": "URL is a document file — direct download link for job application.",
            }
            checked_urls.append(job_url)
            raw_checks.append({"url": job_url, "method": "document_url_detected", "result": result})
            log_event(logger, "info", "apply_url_document_link_found main_domain=%s url=%s", main_domain, job_url, domain=main_domain, agent_index=agent_index, url=job_url)
            return _build_response("found", result, checked_urls, raw_checks, source_url=job_url)

        # Skip non-navigable URLs (mailto, tel, etc.) — but capture email ones
        if not is_web_navigation_url(job_url):
            if job_url.lower().startswith("mailto:"):
                email = job_url[7:].split("?")[0].strip()
                result = {
                    "page_accessible": True,
                    "page_access_status": "accessible",
                    "page_access_detail": None,
                    "means_of_application": "email",
                    "apply_url": None,
                    "apply_email": email,
                    "apply_document_url": None,
                    "additional_methods": None,
                    "confidence": "high",
                    "reasoning": f"Job URL is a mailto link — candidate applies by email to {email}.",
                }
                checked_urls.append(job_url)
                raw_checks.append({"url": job_url, "method": "mailto_url_detected", "result": result})
                log_event(logger, "info", "apply_url_email_found main_domain=%s email=%s", main_domain, email, domain=main_domain, agent_index=agent_index, email=email)
                return _build_response("found", result, checked_urls, raw_checks, source_url=job_url)
            continue

        if has_skip_extension(job_url):
            continue

        # Navigate to the page
        nav_response = await navigate_to_url(
            browser_session.page if browser_session is not None else None,
            agent_index=agent_index,
            tab_handle=agent_tab["handle"],
            url=job_url,
            post_navigation_delay_ms=2000,
        )
        checked_urls.append(job_url)

        if nav_response.get("status") != "navigated":
            raw_checks.append({
                "url": job_url,
                "method": "navigation",
                "result": {
                    "page_accessible": False,
                    "page_access_status": "error",
                    "page_access_detail": f"Navigation failed: {nav_response.get('status')}",
                    "means_of_application": "unknown",
                    "apply_url": None,
                    "apply_email": None,
                    "apply_document_url": None,
                    "confidence": "high",
                    "reasoning": f"Could not navigate to job URL: {nav_response.get('status')}",
                },
            })
            log_event(logger, "warning", "apply_url_navigation_failed main_domain=%s url=%s status=%s", main_domain, job_url, nav_response.get("status"), domain=main_domain, agent_index=agent_index, url=job_url)
            continue

        extracted = await extract_page_content(
            browser_session.page if browser_session is not None else None,
            sections=["body"],
        )

        if not extracted or not extracted.get("markdown"):
            raw_checks.append({
                "url": job_url,
                "method": "extraction",
                "result": {
                    "page_accessible": False,
                    "page_access_status": "empty",
                    "page_access_detail": "No content extracted after navigation.",
                    "means_of_application": "unknown",
                    "apply_url": None,
                    "apply_email": None,
                    "apply_document_url": None,
                    "confidence": "uncertain",
                    "reasoning": "Could not extract page content.",
                },
            })
            continue

        llm_result = await _llm_apply_url_check(
            page_text=extracted["markdown"],
            page_url=nav_response.get("current_url") or job_url,
            main_domain=main_domain,
        )
        raw_checks.append({"url": job_url, "method": "navigate_and_llm", "result": llm_result})

        if not llm_result.get("page_accessible"):
            log_event(
                logger,
                "info",
                "apply_url_page_inaccessible main_domain=%s url=%s status=%s",
                main_domain,
                job_url,
                llm_result.get("page_access_status"),
                domain=main_domain,
                agent_index=agent_index,
                url=job_url,
                page_access_status=llm_result.get("page_access_status"),
            )
            continue

        # Page is accessible — return whatever the LLM found
        log_event(
            logger,
            "info",
            "apply_url_found main_domain=%s url=%s means=%s",
            main_domain,
            job_url,
            llm_result.get("means_of_application"),
            domain=main_domain,
            agent_index=agent_index,
            url=job_url,
            means_of_application=llm_result.get("means_of_application"),
        )
        return _build_response("found", llm_result, checked_urls, raw_checks, source_url=job_url)

    # All URLs exhausted
    return _aggregate_result(raw_checks, checked_urls)


async def _llm_apply_url_check(page_text: str, page_url: str, main_domain: str) -> dict:
    log_event(logger, "info", "apply_url_llm_check_started page_url=%s", page_url, domain=main_domain, page_url=page_url)
    prompt = build_apply_url_prompt(page_text=page_text, page_url=page_url, main_domain=main_domain)
    service = OpenAIAnalysisService()
    analysis = await service.analyze_data(prompt=prompt, json_response=True)

    if not analysis.success:
        log_event(logger, "warning", "apply_url_llm_check_failed page_url=%s error=%s", page_url, analysis.error, domain=main_domain, page_url=page_url, error=analysis.error)
        return {
            "page_accessible": False,
            "page_access_status": "error",
            "page_access_detail": f"LLM analysis failed: {analysis.error}",
            "means_of_application": "unknown",
            "apply_url": None,
            "apply_email": None,
            "apply_document_url": None,
            "confidence": "uncertain",
            "reasoning": f"LLM analysis failed: {analysis.error}",
        }

    log_event(logger, "info", "apply_url_llm_check_completed page_url=%s", page_url, domain=main_domain, page_url=page_url)
    return analysis.response


def _build_response(
    status: str,
    result: dict,
    checked_urls: list[str],
    raw_checks: list[dict],
    source_url: str | None = None,
) -> dict:
    return {
        "status": status,
        "means_of_application": result.get("means_of_application") or "unknown",
        "apply_url": result.get("apply_url"),
        "apply_email": result.get("apply_email"),
        "apply_document_url": result.get("apply_document_url"),
        "additional_methods": result.get("additional_methods") or [],
        "source_url": source_url,
        "confidence": result.get("confidence", "uncertain"),
        "reasoning": result.get("reasoning", ""),
        "page_access_status": result.get("page_access_status"),
        "checked_urls": checked_urls,
        "raw_checks": raw_checks,
    }


def _aggregate_result(raw_checks: list[dict], checked_urls: list[str]) -> dict:
    if not raw_checks:
        return _build_response("no_urls", {}, checked_urls, raw_checks)

    all_inaccessible = all(not c["result"].get("page_accessible") for c in raw_checks)
    if all_inaccessible:
        return _build_response(
            "inaccessible",
            {
                "means_of_application": "unknown",
                "reasoning": f"All {len(checked_urls)} checked URL(s) were inaccessible.",
                "confidence": "high",
            },
            checked_urls,
            raw_checks,
        )

    return _build_response(
        "not_found",
        {
            "means_of_application": "unknown",
            "reasoning": "Checked all URLs but could not determine application method.",
            "confidence": "uncertain",
        },
        checked_urls,
        raw_checks,
    )
