from typing import Any
from urllib.parse import urlparse
from prompts.ats_check_prompt import build_ats_check_prompt
from services.flow_safety import  _is_external_domain, extract_domain, has_skip_extension, detect_blocked_platform, _is_document_url, is_web_navigation_url
from services.openai_service import OpenAIAnalysisService
from services.content_extraction import extract_page_content
from services.navigation import navigate_to_url
from utils.logging import get_logger, log_event

logger = get_logger("ats_check_node")


# ── Statuses that confirm the page was job related ────────────────────────────
_JOB_RELATED_STATUSES = {
    "jobs_listed_on_page",
    "jobs_page",
    "single_job_posting",
    "jobs_related_no_vacancies",
    "jobs_related_no_vacancies_job_alert",
    "external_domain_redirect",
    "following_navigation_url",
    "follow_navigation_button",
}

# ── Known ATS domains ─────────────────────────────────────────────────────────
_KNOWN_ATS_DOMAINS: dict[str, str] = {
    "greenhouse.io": "Greenhouse",
    "lever.co": "Lever",
    "workday.com": "Workday",
    "myworkdayjobs.com": "Workday",
    "icims.com": "iCIMS",
    "smartrecruiters.com": "SmartRecruiters",
    "taleo.net": "Taleo",
    "bamboohr.com": "BambooHR",
    "recruitee.com": "Recruitee",
    "teamtailor.com": "Teamtailor",
    "jobvite.com": "Jobvite",
    "successfactors.com": "SuccessFactors",
    "sap.com": "SAP SuccessFactors",
    "ashbyhq.com": "Ashby",
    "personio.com": "Personio",
    "pinpoint.co": "Pinpoint",
    "hireful.co.uk": "Hireful",
    "networxrecruitment.com": "Networx",
    "speedadmin.dk": "SpeedAdmin",
    "current-vacancies.com": "Current Vacancies",
    "mynewterm.com": "MyNewTerm",
    "jobtrain.co.uk": "Jobtrain",
    "applytoeducation.com": "ApplyToEducation",
    "rezoomo.com": "Rezoomo",
    "jazz.co": "JazzHR",
    "jazzhr.com": "JazzHR",
    "breezy.hr": "Breezy HR",
    "workable.com": "Workable",
    "rippling.com": "Rippling",
    "oracle.com": "Oracle Recruiting",
    "oraclecloud.com": "Oracle HCM",
    "cornerstoneondemand.com": "Cornerstone",
    "lumesse.com": "Lumesse",
    "talentlink.com": "Talentlink",
    "hireserve.com": "Hireserve",
    "eploy.co.uk": "Eploy",
    "jobtrain.co.uk": "Jobtrain",
    "tes.com": "TES",
    "educationjobs.gov.uk": "Education Jobs",
    "jobs.nhs.uk": "NHS Jobs",
    "eteach.com": "eTeach",
}


def _detect_known_ats_domain(url: str | None) -> str | None:
    """Return ATS provider name if URL belongs to a known ATS domain, else None."""
    if not url:
        return None
    try:
        hostname = urlparse(url).hostname or ""
        hostname = hostname.removeprefix("www.").lower()
        for domain, provider in _KNOWN_ATS_DOMAINS.items():
            if hostname == domain or hostname.endswith(f".{domain}"):
                return provider
    except Exception:
        pass
    return None

# ── Core detection ────────────────────────────────────────────────────────────

async def detect_ats(
    career_page_result: dict,
    main_domain: str,
    browser_session: Any,
    agent_index: int,
    agent_tab: dict,
) -> dict:
    overview = career_page_result.get("overview", {})
    pages_analysis = career_page_result.get("career_pages_analysis", [])
    log_event(
        logger,
        "info",
        "ats_detection_started main_domain=%s page_analysis_count=%s",
        main_domain,
        len(pages_analysis),
        domain=main_domain,
        agent_index=agent_index,
        page_analysis_count=len(pages_analysis),
    )

    # Skip entirely if not job related
    if overview.get("outcome") == "not_job_related":
        result = _build_ats_response(
            {
                "is_ats": None,
                "confidence": "high",
                "detection_method": "skipped",
                "reasoning": "Career page is not job related — ATS detection not applicable.",
                "ats_provider": None,
            },
            checked_urls=[],
            raw_checks=[],
        )
        log_event(
            logger,
            "info",
            "ats_detection_skipped_not_job_related main_domain=%s",
            main_domain,
            domain=main_domain,
            agent_index=agent_index,
            detection_method=result.get("detection_method"),
        )
        return result

    if overview.get("outcome") == "access_issue":
        result = _build_ats_response(
            {
                "is_ats": None,
                "confidence": "uncertain",
                "detection_method": "skipped_access_issue",
                "reasoning": "ATS detection could not be verified because the discovered career pages were not accessible.",
                "ats_provider": None,
            },
            checked_urls=[],
            raw_checks=[],
        )
        log_event(
            logger,
            "info",
            "ats_detection_skipped_access_issue main_domain=%s",
            main_domain,
            domain=main_domain,
            agent_index=agent_index,
            detection_method=result.get("detection_method"),
        )
        return result

    checked_urls: list[str] = []
    raw_checks: list[dict] = []

    # ─────────────────────────────────────────────────────────────────────────
    # PASS 1 — fast domain + content scan on data we already have
    # No navigation, no LLM — exit immediately on any confident signal
    # ─────────────────────────────────────────────────────────────────────────
    fast_result = _fast_ats_scan(pages_analysis, main_domain, raw_checks)
    if fast_result is not None:
        result = _build_ats_response(fast_result, checked_urls, raw_checks)
        log_event(
            logger,
            "info",
            "ats_detection_fast_result main_domain=%s detected=%s",
            main_domain,
            result.get("ats_detected"),
            domain=main_domain,
            agent_index=agent_index,
            detection_method=result.get("detection_method"),
            ats_detected=result.get("ats_detected"),
        )
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # PASS 2 — check job_urls from overview
    # Same-domain URLs are recorded but never stop the scan —
    # the ATS may only be visible once we actually scrape the job page content.
    # Only exit early on external confirmed signals (known ATS, blocked platform)
    # or high-confidence LLM results.
    # ─────────────────────────────────────────────────────────────────────────
    job_urls: list[str] = overview.get("job_urls") or []

    # Classify all job URLs upfront
    document_urls = [u for u in job_urls if _is_document_url(u)]
    web_urls = [u for u in job_urls if is_web_navigation_url(u) and not has_skip_extension(u)]

    # All job URLs are documents — company posts jobs as files, not via ATS
    if job_urls and len(document_urls) == len(job_urls):
        result = {
            "is_ats": False,
            "confidence": "high",
            "ats_provider": None,
            "ats_category": "document_application",
            "reasoning": (
                f"All {len(document_urls)} job URL(s) are document files "
                f"(PDF/Word) — company posts jobs as downloadable files, not via an ATS."
            ),
            "apply_url": None,
            "requires_scraping": False,
            "detection_method": "all_document_urls",
        }
        for url in document_urls:
            raw_checks.append({"url": url, "method": "document_url_check", "result": result})
        final_result = _build_ats_response(result, checked_urls, raw_checks)
        log_event(
            logger,
            "info",
            "ats_detection_document_only_result main_domain=%s",
            main_domain,
            domain=main_domain,
            agent_index=agent_index,
            detection_method=final_result.get("detection_method"),
        )
        return final_result

    for job_url in web_urls:
        if job_url in checked_urls:
            continue

        # Same domain — record it but DO NOT exit.
        # The page content may still reveal an embedded ATS.
        # We will navigate and run LLM on it like any other URL.
        if not _is_external_domain(job_url, main_domain):
            raw_checks.append({
                "url": job_url,
                "method": "domain_check",
                "result": {
                    "is_ats": False,
                    "confidence": "low",
                    "ats_provider": None,
                    "ats_category": "native_form",
                    "reasoning": (
                        f"Job URL belongs to main domain '{main_domain}' — "
                        f"no external ATS signal from URL alone, but page content still needs checking."
                    ),
                    "apply_url": job_url,
                    "requires_scraping": True,
                    "detection_method": "same_domain_check",
                },
            })
            # Fall through — navigate and check page content below

        # Blocked platform — record and exit immediately
        blocked_platform = detect_blocked_platform(job_url)
        if blocked_platform:
            result = {
                "is_ats": True,
                "confidence": "high",
                "ats_provider": blocked_platform,
                "ats_category": "external_job_board",
                "platform": blocked_platform,
                "reasoning": (
                    f"Job URL points to '{blocked_platform}' — "
                    f"a social media or aggregator platform."
                ),
                "apply_url": job_url,
                "requires_scraping": False,
                "detection_method": f"job_url_social_media_or_aggregator_{blocked_platform}",
            }
            raw_checks.append({"url": job_url, "method": f"blocked_platform_check_{blocked_platform}", "result": result})
            checked_urls.append(job_url)
            return _build_ats_response(result, checked_urls, raw_checks)

        # Known ATS domain — exit immediately
        known_ats = _detect_known_ats_domain(job_url)
        if known_ats:
            result = {
                "is_ats": True,
                "confidence": "high",
                "ats_provider": known_ats,
                "ats_category": "external_ats",
                "reasoning": f"Job URL domain matches known ATS provider: {known_ats}.",
                "apply_url": job_url,
                "requires_scraping": False,
                "detection_method": "job_url_known_ats_domain",
            }
            raw_checks.append({"url": job_url, "method": "known_ats_domain", "result": result})
            checked_urls.append(job_url)
            return _build_ats_response(result, checked_urls, raw_checks)

        # Navigate to the URL — covers both same-domain and unknown external domains
        nav_response = await navigate_to_url(
            browser_session.page if browser_session is not None else None,
            agent_index=agent_index,
            tab_handle=agent_tab["handle"],
            url=job_url,
            post_navigation_delay_ms=3000,
        )
        checked_urls.append(job_url)

        if nav_response["status"] != "navigated":
            log_event(
                logger,
                "warning",
                "ats_navigation_failed job_url=%s status=%s",
                job_url,
                nav_response["status"],
                domain=main_domain,
                agent_index=agent_index,
                job_url=job_url,
                navigation_status=nav_response["status"],
            )
            raw_checks.append({
                "url": job_url,
                "method": "navigation",
                "result": {
                    "is_ats": None,
                    "confidence": "uncertain",
                    "reasoning": f"Could not navigate to job URL: {nav_response['status']}",
                    "ats_provider": None,
                    "detection_method": "navigation_failed",
                },
            })
            continue  # uncertain — try next URL

        extracted = await extract_page_content(
            browser_session.page if browser_session is not None else None,
            sections=["body"],
        )

        if not extracted or not extracted.get("markdown"):
            raw_checks.append({
                "url": job_url,
                "method": "navigation",
                "result": {
                    "is_ats": None,
                    "confidence": "uncertain",
                    "reasoning": "Navigated but could not extract page content.",
                    "ats_provider": None,
                    "detection_method": "extraction_failed",
                },
            })
            continue  # uncertain — try next URL

        llm_result = await _llm_ats_check(
            page_text=extracted["markdown"],
            page_url=job_url,
            main_domain=main_domain,
        )
        llm_result.setdefault("detection_method", "navigate_and_llm")
        raw_checks.append({"url": job_url, "method": "navigate_and_llm", "result": llm_result})

        confidence = llm_result.get("confidence", "uncertain")
        is_ats = llm_result.get("is_ats")

        # Exit on confident ATS result
        if is_ats is True and confidence in {"high", "medium"}:
            final_result = _build_ats_response(llm_result, checked_urls, raw_checks)
            log_event(
                logger,
                "info",
                "ats_detection_confident_result main_domain=%s job_url=%s detected=%s",
                main_domain,
                job_url,
                final_result.get("ats_detected"),
                domain=main_domain,
                agent_index=agent_index,
                job_url=job_url,
                detection_method=final_result.get("detection_method"),
            )
            return final_result

        # Exit on confident non-ATS result
        if is_ats is False and confidence == "high":
            final_result = _build_ats_response(llm_result, checked_urls, raw_checks)
            log_event(
                logger,
                "info",
                "ats_detection_confident_non_ats_result main_domain=%s job_url=%s",
                main_domain,
                job_url,
                domain=main_domain,
                agent_index=agent_index,
                job_url=job_url,
                detection_method=final_result.get("detection_method"),
            )
            return final_result

        # Low/uncertain confidence — record and continue to next URL

    # ─────────────────────────────────────────────────────────────────────────
    # PASS 3 — LLM on already-extracted job-related page content
    # No new navigation — use what we already paid for
    # ─────────────────────────────────────────────────────────────────────────
    for page in pages_analysis:
        if page.get("status", "") not in _JOB_RELATED_STATUSES:
            continue

        extracted_content = page.get("extracted_content")
        page_url = page.get("extracted_url") or page.get("url") or ""

        if not extracted_content or not page_url or page_url in checked_urls:
            continue

        llm_result = await _llm_ats_check(
            page_text=extracted_content,
            page_url=page_url,
            main_domain=main_domain,
        )
        llm_result.setdefault("detection_method", "existing_content_llm")
        checked_urls.append(page_url)
        raw_checks.append({"url": page_url, "method": "existing_content_llm", "result": llm_result})

        confidence = llm_result.get("confidence", "uncertain")
        is_ats = llm_result.get("is_ats")

        if is_ats is True and confidence in {"high", "medium"}:
            final_result = _build_ats_response(llm_result, checked_urls, raw_checks)
            log_event(
                logger,
                "info",
                "ats_detection_existing_content_result main_domain=%s detected=%s",
                main_domain,
                final_result.get("ats_detected"),
                domain=main_domain,
                agent_index=agent_index,
                detection_method=final_result.get("detection_method"),
            )
            return final_result

        if is_ats is False and confidence == "high":
            final_result = _build_ats_response(llm_result, checked_urls, raw_checks)
            log_event(
                logger,
                "info",
                "ats_detection_existing_content_non_ats_result main_domain=%s",
                main_domain,
                domain=main_domain,
                agent_index=agent_index,
                detection_method=final_result.get("detection_method"),
            )
            return final_result

        # Uncertain — continue to next page

    # ─────────────────────────────────────────────────────────────────────────
    # PASS 4 — aggregate everything collected, give best possible answer
    # ─────────────────────────────────────────────────────────────────────────
    final_result = _aggregate_ats_result(raw_checks, checked_urls)
    log_event(
        logger,
        "info",
        "ats_detection_aggregated_result main_domain=%s detected=%s",
        main_domain,
        final_result.get("ats_detected"),
        domain=main_domain,
        agent_index=agent_index,
        detection_method=final_result.get("detection_method"),
        ats_detected=final_result.get("ats_detected"),
    )
    return final_result

# ── Supporting functions ──────────────────────────────────────────────────────

def _fast_ats_scan(
    pages_analysis: list[dict],
    main_domain: str,
    raw_checks: list[dict],
) -> dict | None:
    """
    Scan already-collected data for ATS signals — no LLM, no navigation.
    Mutates raw_checks to record blocked platform signals.
    Returns a result dict to exit immediately, or None if inconclusive.
    """
    for page in pages_analysis:

        # ── job_urls on the page ──────────────────────────────────────────────
        for job_url in page.get("jobs_listed_on_page") or []:
            if not job_url or not is_web_navigation_url(job_url) or has_skip_extension(job_url):
                continue

            if not _is_external_domain(job_url, main_domain):
                continue

            # ── blocked_platform recorded by career_page_category_node ───────────────
            blocked_platform = page.get("blocked_platform")
            blocked_platform_url = page.get("blocked_platform_url")
            if blocked_platform and blocked_platform_url:
                result = {
                    "is_ats": False,
                    "confidence": "high",
                    "ats_provider": None,
                    "ats_category": "external_job_board",
                    "platform": blocked_platform,
                    "reasoning": (
                        f"Career page navigation stopped at '{blocked_platform_url}' "
                        f"— '{blocked_platform}' is a social media or aggregator platform, not a company ATS."
                    ),
                    "apply_url": blocked_platform_url,
                    "requires_scraping": False,
                    "detection_method": f"career_page_social_media_or_aggregator_{blocked_platform}",
                }
                raw_checks.append({
                    "url": blocked_platform_url,
                    "method": f"career_page_social_media_or_aggregator_{blocked_platform}",
                    "result": result,
                })
                return result

            known_ats = _detect_known_ats_domain(job_url)
            if known_ats:
                return {
                    "is_ats": True,
                    "confidence": "high",
                    "ats_provider": known_ats,
                    "ats_category": "external_ats",
                    "reasoning": f"Job URL '{job_url}' belongs to known ATS: {known_ats}.",
                    "apply_url": job_url,
                    "requires_scraping": False,
                    "detection_method": "job_url_known_ats_domain",
                }

            # Unknown external domain — ATS likely but needs LLM to confirm
            ats_domain = extract_domain(job_url)
            return {
                "is_ats": True,
                "confidence": "medium",
                "ats_provider": ats_domain,
                "ats_category": "external_ats",
                "reasoning": (
                    f"Job URL '{job_url}' is on external domain '{ats_domain}', "
                    f"not the company domain '{main_domain}' — likely an ATS."
                ),
                "apply_url": job_url,
                "requires_scraping": False,
                "detection_method": "job_url_external_domain",
            }

        # ── navigation history ────────────────────────────────────────────────
        for step in page.get("navigation_history") or []:
            for url_field in ("target_url", "landed_url"):
                url = step.get(url_field)
                if not url or not is_web_navigation_url(url):
                    continue
                known_ats = _detect_known_ats_domain(url)
                if known_ats:
                    return {
                        "is_ats": True,
                        "confidence": "high",
                        "ats_provider": known_ats,
                        "ats_category": "external_ats",
                        "reasoning": f"Navigation history URL matches known ATS: {known_ats}.",
                        "apply_url": url,
                        "requires_scraping": False,
                        "detection_method": "navigation_history_domain_scan",
                    }

        # ── extracted content string scan ─────────────────────────────────────
        content = (page.get("extracted_content") or "").lower()
        if content:
            for domain, provider in _KNOWN_ATS_DOMAINS.items():
                if domain in content:
                    return {
                        "is_ats": True,
                        "confidence": "medium",
                        "ats_provider": provider,
                        "ats_category": "embedded_form",
                        "reasoning": f"Known ATS domain '{domain}' found in page content.",
                        "apply_url": None,
                        "requires_scraping": True,
                        "detection_method": "content_string_scan",
                    }

        # ── external domain redirect ──────────────────────────────────────────
        redirect_url = page.get("external_domain_redirect")
        if redirect_url and is_web_navigation_url(redirect_url):
            known_ats = _detect_known_ats_domain(redirect_url)
            if known_ats:
                return {
                    "is_ats": True,
                    "confidence": "high",
                    "ats_provider": known_ats,
                    "ats_category": "external_ats",
                    "reasoning": f"External redirect matches known ATS: {known_ats}.",
                    "apply_url": redirect_url,
                    "requires_scraping": False,
                    "detection_method": "external_redirect_domain_scan",
                }

        # ── embedded jobs — content is there, let LLM handle it in pass 3 ────
        if page.get("embedded_jobs_present"):
            return None

    return None


async def _llm_ats_check(
    page_text: str,
    page_url: str,
    main_domain: str,
) -> dict:
    """Run the ATS detection LLM prompt on given page text."""
    log_event(
        logger,
        "info",
        "ats_llm_check_started page_url=%s",
        page_url,
        domain=main_domain,
        page_url=page_url,
    )
    prompt = build_ats_check_prompt(
        page_text=page_text,
        main_domain=main_domain,
        page_url=page_url,
    )
    service = OpenAIAnalysisService()
    analysis = await service.analyze_data(prompt=prompt, json_response=True)

    if not analysis.success:
        log_event(
            logger,
            "warning",
            "ats_llm_check_failed page_url=%s error=%s",
            page_url,
            analysis.error,
            domain=main_domain,
            page_url=page_url,
            error=analysis.error,
        )
        return {
            "is_ats": None,
            "confidence": "uncertain",
            "reasoning": f"LLM analysis failed: {analysis.error}",
            "ats_provider": None,
            "ats_category": "unknown",
            "requires_scraping": False,
            "detection_method": "llm_failed",
        }
    log_event(
        logger,
        "info",
        "ats_llm_check_completed page_url=%s",
        page_url,
        domain=main_domain,
        page_url=page_url,
    )
    return analysis.response


def _build_ats_response(
    result: dict,
    checked_urls: list[str],
    raw_checks: list[dict],
) -> dict:
    """Normalise a result dict into the final ATS response shape."""
    ats_provider = result.get("ats_provider")
    is_ats = result.get("is_ats")

    ats_provider_known: bool | None = None
    if is_ats and ats_provider:
        ats_provider_known = ats_provider in set(_KNOWN_ATS_DOMAINS.values())

    # Collect all blocked/aggregator platform signals across all checks
    external_job_boards = {}
    for check in raw_checks:
        r = check.get("result", {})
        if r.get("ats_category") == "external_job_board" and r.get("platform"):
            platform = r["platform"]
            if platform not in external_job_boards:
                external_job_boards[platform] = r.get("apply_url")

    return {
        "ats_detected": is_ats,
        "ats_provider": ats_provider,
        "ats_provider_known": ats_provider_known,
        "ats_category": result.get("ats_category") or result.get("application_type"),
        "confidence": result.get("confidence", "uncertain"),
        "detection_method": result.get("detection_method", "llm"),
        "reasoning": result.get("reasoning", ""),
        "non_ats_reason": result.get("reasoning") if not is_ats else None,
        "requires_scraping": result.get("requires_scraping", False),
        "apply_url": result.get("apply_url"),
        "indicators_found": result.get("indicators_found") or [],
        "page_access_status": result.get("page_access_status"),
        "external_job_boards": [
            {"platform": p, "url": u} for p, u in external_job_boards.items()
        ],
        "checked_urls": checked_urls,
        "raw_checks": raw_checks,
    }


def _aggregate_ats_result(
    raw_checks: list[dict],
    checked_urls: list[str],
) -> dict:
    """Aggregate all inconclusive checks and return the best summary."""
    if not raw_checks:
        return _build_ats_response(
            {
                "is_ats": None,
                "confidence": "uncertain",
                "reasoning": "No URLs were checked — insufficient data to determine ATS.",
                "ats_provider": None,
                "detection_method": "aggregated",
            },
            checked_urls,
            raw_checks,
        )

    confidence_rank = {"high": 3, "medium": 2, "low": 1, "uncertain": 0}

    ats_true = [c for c in raw_checks if c["result"].get("is_ats") is True]
    ats_false = [c for c in raw_checks if c["result"].get("is_ats") is False]
    uncertain = [c for c in raw_checks if c["result"].get("is_ats") is None]

    if ats_true:
        best = max(ats_true, key=lambda c: confidence_rank.get(c["result"].get("confidence", "uncertain"), 0))
        best["result"]["detection_method"] = "aggregated"
        return _build_ats_response(best["result"], checked_urls, raw_checks)

    if ats_false:
        reasons = "; ".join(
            c["result"].get("reasoning", "") for c in ats_false if c["result"].get("reasoning")
        )
        return _build_ats_response(
            {
                "is_ats": False,
                "confidence": "medium",
                "reasoning": f"No ATS signals found across {len(ats_false)} checked page(s). {reasons}",
                "ats_provider": None,
                "detection_method": "aggregated",
            },
            checked_urls,
            raw_checks,
        )

    reasons = "; ".join(
        c["result"].get("reasoning", "") for c in uncertain if c["result"].get("reasoning")
    )
    return _build_ats_response(
        {
            "is_ats": None,
            "confidence": "uncertain",
            "reasoning": f"All {len(uncertain)} check(s) were inconclusive. {reasons}",
            "ats_provider": None,
            "detection_method": "aggregated",
        },
        checked_urls,
        raw_checks,
    )
