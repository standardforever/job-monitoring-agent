from __future__ import annotations

from schemas.agent_state import JobScraperState

from services.grid_session import attach_playwright_to_cdp
from services.tab_manager import ensure_agent_tab
from utils.logging import get_logger, log_event

logger = get_logger("session_bootstrap")


async def bootstrap_browser_node(state: dict) -> dict:
    agent_index = state.get("agent_index", 0)
    cdp_url = state.get("cdp_url")
    session_id = state.get("session_id")
    metadata = dict(state.get("metadata", {}))
    bootstrap_attempt_count = int(metadata.get("bootstrap_attempt_count", 0) or 0) + 1
    domain = ((state.get("assigned_urls") or ["unknown"])[0])
    log_event(
        logger,
        "info",
        "browser_bootstrap_started agent_index=%s attempt=%s",
        agent_index,
        bootstrap_attempt_count,
        domain=domain,
        agent_index=agent_index,
        attempt=bootstrap_attempt_count,
    )

    if not cdp_url:
        errors = list(state.get("errors", []))
        errors.append("Missing shared CDP URL for Playwright attachment")
        log_event(
            logger,
            "error",
            "browser_bootstrap_missing_cdp_url agent_index=%s",
            agent_index,
            domain=domain,
            agent_index=agent_index,
        )
        return {
            **state,
            "agent_index": agent_index,
            "session_established": False,
            "errors": errors,
            "metadata": {
                **metadata,
                "bootstrap_attempt_count": bootstrap_attempt_count,
                "bootstrap_status": "missing_cdp_url",
            },
        }

    session = await attach_playwright_to_cdp(cdp_url)
    if session is None:
        errors = list(state.get("errors", []))
        errors.append("Unable to attach Playwright to the Selenium CDP session")
        log_event(
            logger,
            "error",
            "browser_bootstrap_attach_failed agent_index=%s",
            agent_index,
            domain=domain,
            agent_index=agent_index,
            cdp_url=cdp_url,
        )
        return {
            **state,
            "agent_index": agent_index,
            "session_established": False,
            "errors": errors,
            "metadata": {
                **metadata,
                "bootstrap_attempt_count": bootstrap_attempt_count,
                "bootstrap_status": "attach_failed",
            },
        }

    agent_tab = await ensure_agent_tab(session, agent_index=agent_index)
    log_event(
        logger,
        "info",
        "browser_bootstrap_completed agent_index=%s tab_handle=%s",
        agent_index,
        agent_tab.get("handle"),
        domain=domain,
        agent_index=agent_index,
        tab_handle=agent_tab.get("handle"),
    )
    return {
        **state,
        "agent_index": agent_index,
        "browser_session": session,
        "session_id": session_id or session.session_id,
        "cdp_url": cdp_url,
        "session_established": True,
        "agent_tab": agent_tab,
        "metadata": {
            **metadata,
            "bootstrap_attempt_count": bootstrap_attempt_count,
            "bootstrap_status": "connected",
            "reused_existing_session": bool(metadata.get("reused_existing_session", False)),
        },
    }
