from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


ProcessStatus = Literal["queued", "running", "completed", "failed"]
RequestedCapability = Literal["career_page", "ats_check", "job_monitoring"]


class JobProcessRequest(BaseModel):
    client_name: str = Field(default="default_client", min_length=1)
    urls: list[str] = Field(default_factory=list, min_length=1)
    agent_count: int = Field(default=1, ge=1)
    grid_url: str | None = None
    ats_check: bool = True
    job_monitoring: bool = False
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


class ClientDocument(BaseModel):
    client_key: str
    client_name: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ClientDomainDocument(BaseModel):
    client_key: str
    client_name: str
    domain_key: str
    requested_capability: RequestedCapability
    ats_check: bool = True
    job_monitoring: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class CanonicalDomainDocument(BaseModel):
    domain_key: str
    normalized_domain: str
    career_url_extraction: dict[str, Any] = Field(default_factory=dict)
    career_page_result: dict[str, Any] = Field(default_factory=dict)
    ats_detection: dict[str, Any] = Field(default_factory=dict)
    latest_page_fingerprint: str | None = None
    latest_extracted_text: str | None = None
    last_career_discovery_at: datetime | None = None
    last_career_check_at: datetime | None = None
    last_ats_check_at: datetime | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ProcessRunDocument(BaseModel):
    process_id: str
    client_key: str
    client_name: str
    status: ProcessStatus
    request: JobProcessRequest
    assignments: list[dict[str, Any]] = Field(default_factory=list)
    queued_urls: list[str] = Field(default_factory=list)
    running_urls: list[str] = Field(default_factory=list)
    completed_urls: list[str] = Field(default_factory=list)
    failed_urls: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None


class ProcessRunItemDocument(BaseModel):
    process_id: str
    client_key: str
    client_name: str
    raw_url: str
    domain_key: str
    requested_capability: RequestedCapability
    status: ProcessStatus
    error: str | None = None
    agent_index: int | None = None
    result_summary: dict[str, Any] = Field(default_factory=dict)
    result_payload: dict[str, Any] = Field(default_factory=dict)
    domain_check_id: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None


class DomainCheckDocument(BaseModel):
    domain_check_id: str
    process_id: str
    client_key: str
    client_name: str
    raw_url: str
    domain_key: str
    requested_capability: RequestedCapability
    content_changed: bool | None = None
    page_fingerprint: str | None = None
    llm_skipped: bool = False
    result_payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
