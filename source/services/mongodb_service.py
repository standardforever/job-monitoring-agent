from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from pymongo import ASCENDING, DESCENDING, MongoClient

from core.config import get_settings
from utils.logging import get_logger, log_event

logger = get_logger("mongodb_service")


class MongoDBService:
    def __init__(self) -> None:
        settings = get_settings()
        self._uri = settings.mongodb_uri
        self._database_name = settings.mongodb_database
        self._collection_names = {
            "process_runs": settings.mongodb_process_runs_collection,
            "process_run_items": settings.mongodb_process_run_items_collection,
            "jobs": settings.mongodb_jobs_collection,
            "job_extraction_cache": settings.mongodb_job_extraction_cache_collection,
        }
        self._client: MongoClient | None = None
        self._database = None
        log_event(
            logger,
            "info",
            "mongodb_service_initialized database=%s",
            self._database_name,
            domain="mongodb",
            database=self._database_name,
            collections=self._collection_names,
        )

    def _get_database(self):
        if self._database is None:
            log_event(
                logger,
                "info",
                "mongodb_connecting uri=%s database=%s",
                self._uri,
                self._database_name,
                domain="mongodb",
                database=self._database_name,
            )
            self._client = MongoClient(self._uri)
            self._database = self._client[self._database_name]
        return self._database

    def _get_collection(self, key: str):
        return self._get_database()[self._collection_names[key]]

    async def ensure_indexes(self) -> None:
        await asyncio.to_thread(self._ensure_indexes_sync)

    async def recover_stale_running_processes(self) -> int:
        return await asyncio.to_thread(self._recover_stale_running_processes_sync)

    def _recover_stale_running_processes_sync(self) -> int:
        now = datetime.utcnow()
        result = self._get_collection("process_runs").update_many(
            {"status": {"$in": ["running", "stop_requested"]}},
            {"$set": {
                "status": "failed",
                "completed_at": now,
                "updated_at": now,
                "errors": ["Process interrupted by application restart"],
            }},
        )
        count = result.modified_count
        if count:
            log_event(logger, "warning", "stale_running_processes_recovered count=%s", count, domain="mongodb", count=count)
        self._get_collection("process_run_items").update_many(
            {"status": "running"},
            {"$set": {"status": "failed", "updated_at": now, "error": "Process interrupted by application restart"}},
        )
        return count

    def _ensure_indexes_sync(self) -> None:
        log_event(
            logger,
            "info",
            "mongodb_ensure_indexes_started database=%s",
            self._database_name,
            domain="mongodb",
            database=self._database_name,
        )

        self._get_collection("process_runs").create_index(
            [("process_id", ASCENDING)],
            unique=True,
            name="process_runs_process_id_unique",
        )
        self._get_collection("process_runs").create_index(
            [("status", ASCENDING)],
            name="process_runs_status",
        )

        self._get_collection("process_run_items").create_index(
            [("process_id", ASCENDING), ("raw_url", ASCENDING)],
            unique=True,
            name="process_run_items_process_url_unique",
        )
        self._get_collection("process_run_items").create_index(
            [("process_id", ASCENDING)],
            name="process_run_items_process_id",
        )
        self._get_collection("process_run_items").create_index(
            [("domain_key", ASCENDING), ("updated_at", DESCENDING)],
            name="process_run_items_domain_updated_desc",
        )

        self._get_collection("jobs").create_index(
            [("job_key", ASCENDING)],
            unique=True,
            name="jobs_job_key_unique",
        )
        self._get_collection("jobs").create_index(
            [("domain_key", ASCENDING), ("updated_at", DESCENDING)],
            name="jobs_domain_updated_desc",
        )

        self._get_collection("job_extraction_cache").create_index(
            [("cache_key", ASCENDING)],
            unique=True,
            name="job_extraction_cache_cache_key_unique",
        )

        log_event(
            logger,
            "info",
            "mongodb_ensure_indexes_completed database=%s",
            self._database_name,
            domain="mongodb",
            database=self._database_name,
        )

    async def insert_process_run(self, document: dict[str, Any]) -> None:
        await asyncio.to_thread(self._insert_process_run_sync, document)

    def _insert_process_run_sync(self, document: dict[str, Any]) -> None:
        log_event(
            logger,
            "info",
            "mongodb_insert_process_run process_id=%s",
            document.get("process_id"),
            domain=document.get("client_key", "mongodb"),
            process_id=document.get("process_id"),
        )
        self._get_collection("process_runs").insert_one(document)

    async def insert_process_run_items(self, items: list[dict[str, Any]]) -> None:
        if not items:
            return
        await asyncio.to_thread(self._insert_process_run_items_sync, items)

    def _insert_process_run_items_sync(self, items: list[dict[str, Any]]) -> None:
        log_event(
            logger,
            "info",
            "mongodb_insert_process_run_items process_id=%s item_count=%s",
            items[0].get("process_id"),
            len(items),
            domain=items[0].get("domain_key", "mongodb"),
            process_id=items[0].get("process_id"),
            item_count=len(items),
        )
        self._get_collection("process_run_items").insert_many(items)

    async def update_process_run(self, process_id: str, updates: dict[str, Any]) -> None:
        await asyncio.to_thread(self._update_process_run_sync, process_id, updates)

    def _update_process_run_sync(self, process_id: str, updates: dict[str, Any]) -> None:
        updates = {**updates, "updated_at": datetime.utcnow()}
        log_event(
            logger,
            "info",
            "mongodb_update_process_run process_id=%s fields=%s",
            process_id,
            sorted(updates.keys()),
            domain="mongodb",
            process_id=process_id,
            update_fields=sorted(updates.keys()),
        )
        self._get_collection("process_runs").update_one({"process_id": process_id}, {"$set": updates})

    async def get_process_run(self, process_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_process_run_sync, process_id)

    def _get_process_run_sync(self, process_id: str) -> dict[str, Any] | None:
        return self._get_collection("process_runs").find_one({"process_id": process_id}, {"_id": 0})

    async def get_process_run_with_items(self, process_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_process_run_with_items_sync, process_id)

    def _get_process_run_with_items_sync(self, process_id: str) -> dict[str, Any] | None:
        run = self._get_collection("process_runs").find_one({"process_id": process_id}, {"_id": 0})
        if run is None:
            return None
        items = list(
            self._get_collection("process_run_items").find(
                {"process_id": process_id},
                {"_id": 0},
            )
        )
        run["items"] = items
        return run

    async def list_all_process_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_all_process_runs_sync, limit)

    def _list_all_process_runs_sync(self, limit: int) -> list[dict[str, Any]]:
        cursor = (
            self._get_collection("process_runs")
            .find({}, {"_id": 0})
            .sort("created_at", -1)
            .limit(limit)
        )
        return list(cursor)

    async def get_latest_process_items_by_domain_keys(
        self,
        domain_keys: list[str],
    ) -> dict[str, dict[str, Any]]:
        if not domain_keys:
            return {}
        return await asyncio.to_thread(self._get_latest_process_items_by_domain_keys_sync, domain_keys)

    def _get_latest_process_items_by_domain_keys_sync(
        self,
        domain_keys: list[str],
    ) -> dict[str, dict[str, Any]]:
        normalized_domain_keys = [str(domain_key or "").strip() for domain_key in domain_keys if str(domain_key or "").strip()]
        if not normalized_domain_keys:
            return {}

        cursor = self._get_collection("process_run_items").find(
            {"domain_key": {"$in": normalized_domain_keys}},
            {"_id": 0},
        ).sort("updated_at", -1)

        latest_items: dict[str, dict[str, Any]] = {}
        for item in cursor:
            domain_key = str(item.get("domain_key") or "").strip()
            if not domain_key or domain_key in latest_items:
                continue
            latest_items[domain_key] = item
            if len(latest_items) >= len(set(normalized_domain_keys)):
                break
        return latest_items

    async def update_assignment_status(self, process_id: str, agent_index: int, status: str) -> None:
        await asyncio.to_thread(self._update_assignment_status_sync, process_id, agent_index, status)

    def _update_assignment_status_sync(self, process_id: str, agent_index: int, status: str) -> None:
        now = datetime.utcnow()
        self._get_collection("process_runs").update_one(
            {"process_id": process_id, "assignments.agent_index": agent_index},
            {
                "$set": {
                    "assignments.$.status": status,
                    "updated_at": now,
                }
            },
        )

    async def mark_process_stop_requested(self, process_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._mark_process_stop_requested_sync, process_id)

    def _mark_process_stop_requested_sync(self, process_id: str) -> dict[str, Any] | None:
        now = datetime.utcnow()
        self._get_collection("process_runs").update_one(
            {"process_id": process_id, "status": {"$in": ["queued", "running", "stop_requested"]}},
            {
                "$set": {
                    "status": "stop_requested",
                    "updated_at": now,
                }
            },
        )
        return self._get_process_run_sync(process_id)

    async def reset_process_run_for_rerun(
        self,
        process_id: str,
        *,
        queued_urls: list[str],
        assignments: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        return await asyncio.to_thread(
            self._reset_process_run_for_rerun_sync,
            process_id,
            queued_urls,
            assignments,
        )

    def _reset_process_run_for_rerun_sync(
        self,
        process_id: str,
        queued_urls: list[str],
        assignments: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        now = datetime.utcnow()
        self._get_collection("process_runs").update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "status": "queued",
                    "assignments": assignments,
                    "queued_urls": list(queued_urls),
                    "running_urls": [],
                    "completed_urls": [],
                    "failed_urls": [],
                    "stopped_urls": [],
                    "errors": [],
                    "started_at": None,
                    "completed_at": None,
                    "updated_at": now,
                    "summary.processed_url_count": 0,
                    "summary.completed_domain_count": 0,
                    "summary.failed_domain_count": 0,
                    "summary.queued_url_count": len(queued_urls),
                    "summary.running_url_count": 0,
                    "summary.stopped_url_count": 0,
                    "summary.assigned_agent_count": len(assignments),
                    "summary.total_urls": len(queued_urls),
                }
            },
        )
        self._get_collection("process_run_items").update_many(
            {"process_id": process_id},
            {
                "$set": {
                    "status": "queued",
                    "error": None,
                    "agent_index": None,
                    "result_summary": {},
                    "result_payload": {},
                    "updated_at": now,
                    "started_at": None,
                    "completed_at": None,
                }
            },
        )
        return self._get_process_run_sync(process_id)

    async def update_process_run_item(
        self,
        process_id: str,
        raw_url: str,
        updates: dict[str, Any],
    ) -> None:
        await asyncio.to_thread(self._update_process_run_item_sync, process_id, raw_url, updates)

    def _update_process_run_item_sync(self, process_id: str, raw_url: str, updates: dict[str, Any]) -> None:
        self._get_collection("process_run_items").update_one(
            {"process_id": process_id, "raw_url": raw_url},
            {"$set": {**updates, "updated_at": datetime.utcnow()}},
        )

    async def mark_url_running(self, process_id: str, url: str, agent_index: int) -> None:
        await asyncio.to_thread(self._mark_url_running_sync, process_id, url, agent_index)

    def _mark_url_running_sync(self, process_id: str, url: str, agent_index: int) -> None:
        now = datetime.utcnow()
        self._get_collection("process_runs").update_one(
            {"process_id": process_id},
            {
                "$pull": {"queued_urls": url},
                "$addToSet": {"running_urls": url},
                "$inc": {
                    "summary.queued_url_count": -1,
                    "summary.running_url_count": 1,
                },
                "$set": {"updated_at": now},
            },
        )
        self._get_collection("process_run_items").update_one(
            {"process_id": process_id, "raw_url": url},
            {
                "$set": {
                    "status": "running",
                    "agent_index": agent_index,
                    "started_at": now,
                    "updated_at": now,
                }
            },
        )

    async def mark_url_completed(
        self,
        process_id: str,
        url: str,
        result_summary: dict[str, Any],
        result_payload: dict[str, Any],
        resolved_career_page_url: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._mark_url_completed_sync,
            process_id,
            url,
            result_summary,
            result_payload,
            resolved_career_page_url,
        )

    def _mark_url_completed_sync(
        self,
        process_id: str,
        url: str,
        result_summary: dict[str, Any],
        result_payload: dict[str, Any],
        resolved_career_page_url: str | None,
    ) -> None:
        now = datetime.utcnow()
        self._get_collection("process_runs").update_one(
            {"process_id": process_id},
            {
                "$pull": {"queued_urls": url, "running_urls": url},
                "$addToSet": {"completed_urls": url},
                "$inc": {
                    "summary.running_url_count": -1,
                    "summary.processed_url_count": 1,
                    "summary.completed_domain_count": 1,
                },
                "$set": {"updated_at": now},
            },
        )
        self._get_collection("process_run_items").update_one(
            {"process_id": process_id, "raw_url": url},
            {
                "$set": {
                    "status": "completed",
                    "error": None,
                    "resolved_career_page_url": resolved_career_page_url,
                    "result_summary": result_summary,
                    "result_payload": result_payload,
                    "completed_at": now,
                    "updated_at": now,
                }
            },
        )

    async def mark_url_failed(
        self,
        process_id: str,
        url: str,
        error: str,
        *,
        result_payload: dict[str, Any] | None = None,
        was_running: bool = True,
    ) -> None:
        await asyncio.to_thread(
            self._mark_url_failed_sync,
            process_id,
            url,
            error,
            result_payload or {},
            was_running,
        )

    def _mark_url_failed_sync(
        self,
        process_id: str,
        url: str,
        error: str,
        result_payload: dict[str, Any],
        was_running: bool,
    ) -> None:
        now = datetime.utcnow()
        inc_fields = {
            "summary.processed_url_count": 1,
            "summary.failed_domain_count": 1,
        }
        if was_running:
            inc_fields["summary.running_url_count"] = -1
        else:
            inc_fields["summary.queued_url_count"] = -1

        self._get_collection("process_runs").update_one(
            {"process_id": process_id},
            {
                "$pull": {"queued_urls": url, "running_urls": url},
                "$addToSet": {"failed_urls": url},
                "$inc": inc_fields,
                "$set": {"updated_at": now},
            },
        )
        self._get_collection("process_run_items").update_one(
            {"process_id": process_id, "raw_url": url},
            {
                "$set": {
                    "status": "failed",
                    "error": error,
                    "result_payload": result_payload,
                    "completed_at": now,
                    "updated_at": now,
                }
            },
        )

    async def mark_urls_stopped(
        self,
        process_id: str,
        urls: list[str],
        *,
        agent_index: int | None = None,
        reason: str = "Process stop requested.",
    ) -> None:
        if not urls:
            return
        await asyncio.to_thread(self._mark_urls_stopped_sync, process_id, urls, agent_index, reason)

    def _mark_urls_stopped_sync(
        self,
        process_id: str,
        urls: list[str],
        agent_index: int | None,
        reason: str,
    ) -> None:
        now = datetime.utcnow()
        run = self._get_process_run_sync(process_id) or {}
        queued_urls = set(run.get("queued_urls") or [])
        running_urls = set(run.get("running_urls") or [])
        target_urls = [url for url in urls if url in queued_urls or url in running_urls]
        if not target_urls:
            return

        queued_count = sum(1 for url in target_urls if url in queued_urls)
        running_count = sum(1 for url in target_urls if url in running_urls)
        inc_fields: dict[str, int] = {"summary.stopped_url_count": len(target_urls)}
        if queued_count:
            inc_fields["summary.queued_url_count"] = -queued_count
        if running_count:
            inc_fields["summary.running_url_count"] = -running_count

        self._get_collection("process_runs").update_one(
            {"process_id": process_id},
            {
                "$pull": {"queued_urls": {"$in": target_urls}, "running_urls": {"$in": target_urls}},
                "$addToSet": {"stopped_urls": {"$each": target_urls}},
                "$inc": inc_fields,
                "$set": {"updated_at": now},
            },
        )
        item_updates: dict[str, Any] = {
            "status": "stopped",
            "error": reason,
            "completed_at": now,
            "updated_at": now,
        }
        if agent_index is not None:
            item_updates["agent_index"] = agent_index
        self._get_collection("process_run_items").update_many(
            {
                "process_id": process_id,
                "raw_url": {"$in": target_urls},
                "status": {"$in": ["queued", "running", "stop_requested"]},
            },
            {"$set": item_updates},
        )

    async def get_job_extraction_cache(self, cache_key: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_job_extraction_cache_sync, cache_key)

    def _get_job_extraction_cache_sync(self, cache_key: str) -> dict[str, Any] | None:
        return self._get_collection("job_extraction_cache").find_one({"cache_key": cache_key}, {"_id": 0})

    async def upsert_job_extraction_cache(self, cache_key: str, document: dict[str, Any]) -> None:
        await asyncio.to_thread(self._upsert_job_extraction_cache_sync, cache_key, document)

    def _upsert_job_extraction_cache_sync(self, cache_key: str, document: dict[str, Any]) -> None:
        now = datetime.utcnow()
        self._get_collection("job_extraction_cache").update_one(
            {"cache_key": cache_key},
            {
                "$set": {
                    **document,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "cache_key": cache_key,
                    "created_at": now,
                },
            },
            upsert=True,
        )

    async def upsert_job(self, job_key: str, document: dict[str, Any]) -> None:
        await asyncio.to_thread(self._upsert_job_sync, job_key, document)

    def _upsert_job_sync(self, job_key: str, document: dict[str, Any]) -> None:
        now = datetime.utcnow()
        self._get_collection("jobs").update_one(
            {"job_key": job_key},
            {
                "$set": {
                    **document,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "job_key": job_key,
                    "created_at": now,
                },
            },
            upsert=True,
        )

    async def list_jobs_by_keys(self, job_keys: list[str]) -> list[dict[str, Any]]:
        if not job_keys:
            return []
        return await asyncio.to_thread(self._list_jobs_by_keys_sync, job_keys)

    def _list_jobs_by_keys_sync(self, job_keys: list[str]) -> list[dict[str, Any]]:
        cursor = self._get_collection("jobs").find({"job_key": {"$in": list(job_keys)}}, {"_id": 0})
        return list(cursor)
