from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


ProcessStatus = Literal["queued", "running", "completed", "failed", "stop_requested", "stopped"]
RequestedCapability = Literal["career_page", "ats_check", "job_extract", "ats_and_job_extract", "job_monitoring"]


class JobProcessRequest(BaseModel):
    urls: list[str] = Field(default_factory=list, min_length=1)
    career_page_urls: dict[str, str] = Field(default_factory=dict)
    agent_count: int = Field(default=1, ge=1)
    ats_check: bool = True
    job_extract: bool = False
    job_monitoring: bool = False
    task_id: str | None = None


class UploadDomainRow(BaseModel):
    domain: str = Field(min_length=1)
    career_page_url: str | None = None

class DomainProcessRecord(BaseModel):
    domain: str
    main_domain: str | None = None
    career_url_extraction: dict[str, Any] = Field(default_factory=dict)
    career_page_result: dict[str, Any] = Field(default_factory=dict)
    ats_detection: dict[str, Any] = Field(default_factory=dict)
    apply_url_detection: dict[str, Any] = Field(default_factory=dict)
    jobs_extraction: dict[str, Any] = Field(default_factory=dict)
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
    stopped_urls: list[str] = Field(default_factory=list)
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
    provided_career_page_url: str | None = None
    resolved_career_page_url: str | None = None
    requested_capability: RequestedCapability
    status: ProcessStatus
    error: str | None = None
    agent_index: int | None = None
    result_summary: dict[str, Any] = Field(default_factory=dict)
    result_payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None

class JobDocument(BaseModel):
    job_key: str
    domain_key: str
    source_type: Literal["job_url", "embedded_page"]
    source_url: str
    extraction_strategy: str
    page_fingerprint: str | None = None
    title: str | None = None
    company_name: str | None = None
    structured_job: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class JobExtractionCacheDocument(BaseModel):
    cache_key: str
    domain_key: str
    source_type: Literal["job_url", "embedded_page"]
    source_url: str
    extraction_strategy: str
    page_fingerprint: str | None = None
    jobs: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
