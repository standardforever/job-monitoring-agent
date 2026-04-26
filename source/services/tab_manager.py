from __future__ import annotations

from schemas.agent_tab import AgentTab

from services.grid_session import BrowserSession


async def ensure_agent_tab(session: BrowserSession, agent_index: int) -> AgentTab:
    page = session.page
    await page.bring_to_front()
    return {
        "agent_index": agent_index,
        "handle": session.cdp_url,
        "status": "ready",
    }
