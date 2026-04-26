from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


ProcessStatus = Literal["queued", "running", "completed", "failed"]


class JobProcessRequest(BaseModel):
    urls: list[str] = Field(default_factory=list, min_length=1)
    agent_count: int = Field(default=1, ge=1)
    grid_url: str | None = None
    ats_check: bool = True
    task_id: str | None = None


class DomainProcessRecord(BaseModel):
    domain: str
    main_domain: str | None = None
    career_url_extraction: dict[str, Any] = Field(default_factory=dict)
    career_page_result: dict[str, Any] = Field(default_factory=dict)
    ats_detection: dict[str, Any] = Field(default_factory=dict)
    status: str = "completed"
    error: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class WorkerProcessResult(BaseModel):
    agent_index: int
    status: str
    assigned_urls: list[str] = Field(default_factory=list)
    processed_urls: list[str] = Field(default_factory=list)
    domain_results: list[DomainProcessRecord] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class JobProcessDocument(BaseModel):
    process_id: str
    status: ProcessStatus
    request: JobProcessRequest
    assignments: list[dict[str, Any]] = Field(default_factory=list)
    queued_urls: list[str] = Field(default_factory=list)
    running_urls: list[str] = Field(default_factory=list)
    completed_urls: list[str] = Field(default_factory=list)
    failed_urls: list[str] = Field(default_factory=list)
    domain_result_events: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
