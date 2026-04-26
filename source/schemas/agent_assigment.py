from typing import Any, TypedDict


class AgentAssignment(TypedDict):
    agent_index: int
    status: str
    urls: list[str]
    url_count: int
