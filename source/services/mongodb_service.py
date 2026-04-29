from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from pymongo import MongoClient

from core.config import get_settings
from models.process import RequestedCapability
from utils.logging import get_logger, log_event

logger = get_logger("mongodb_service")


class MongoDBService:
    def __init__(self) -> None:
        settings = get_settings()
        self._uri = settings.mongodb_uri
        self._database_name = settings.mongodb_database
        self._collection_names = {
            "clients": settings.mongodb_clients_collection,
            "client_domains": settings.mongodb_client_domains_collection,
            "domains": settings.mongodb_domains_collection,
            "process_runs": settings.mongodb_process_runs_collection,
            "process_run_items": settings.mongodb_process_run_items_collection,
            "domain_checks": settings.mongodb_domain_checks_collection,
            "jobs": settings.mongodb_jobs_collection,
            "client_jobs": settings.mongodb_client_jobs_collection,
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

    async def ensure_client(self, client_key: str, client_name: str) -> None:
        await asyncio.to_thread(self._ensure_client_sync, client_key, client_name)

    def _ensure_client_sync(self, client_key: str, client_name: str) -> None:
        now = datetime.utcnow()
        log_event(
            logger,
            "info",
            "mongodb_ensure_client client_key=%s",
            client_key,
            domain=client_key,
            client_key=client_key,
        )
        self._get_collection("clients").update_one(
            {"client_key": client_key},
            {
                "$set": {
                    "client_name": client_name,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "client_key": client_key,
                    "created_at": now,
                },
            },
            upsert=True,
        )

    async def get_client(self, client_key: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_client_sync, client_key)

    def _get_client_sync(self, client_key: str) -> dict[str, Any] | None:
        return self._get_collection("clients").find_one({"client_key": client_key}, {"_id": 0})

    async def list_clients(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_clients_sync)

    def _list_clients_sync(self) -> list[dict[str, Any]]:
        cursor = (
            self._get_collection("clients")
            .find({}, {"_id": 0})
            .sort("updated_at", -1)
        )
        return list(cursor)

    async def upsert_client_configuration(
        self,
        *,
        client_key: str,
        client_name: str,
        api_key: str,
        model: str,
        grid_url: str | None,
        api_key_status: str,
        api_key_validation_error: str | None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._upsert_client_configuration_sync,
            client_key,
            client_name,
            api_key,
            model,
            grid_url,
            api_key_status,
            api_key_validation_error,
        )

    def _upsert_client_configuration_sync(
        self,
        client_key: str,
        client_name: str,
        api_key: str,
        model: str,
        grid_url: str | None,
        api_key_status: str,
        api_key_validation_error: str | None,
    ) -> dict[str, Any]:
        now = datetime.utcnow()
        self._get_collection("clients").update_one(
            {"client_key": client_key},
            {
                "$set": {
                    "client_name": client_name,
                    "api_key": api_key,
                    "model": model,
                    "grid_url": grid_url,
                    "api_key_status": api_key_status,
                    "api_key_last_validated_at": now,
                    "api_key_validation_error": api_key_validation_error,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "client_key": client_key,
                    "created_at": now,
                },
            },
            upsert=True,
        )
        return self._get_client_sync(client_key) or {}

    async def update_client_configuration(
        self,
        *,
        current_client_key: str,
        new_client_key: str,
        client_name: str,
        api_key: str,
        model: str,
        grid_url: str | None,
        api_key_status: str,
        api_key_validation_error: str | None,
    ) -> dict[str, Any] | None:
        return await asyncio.to_thread(
            self._update_client_configuration_sync,
            current_client_key,
            new_client_key,
            client_name,
            api_key,
            model,
            grid_url,
            api_key_status,
            api_key_validation_error,
        )

    def _update_client_configuration_sync(
        self,
        current_client_key: str,
        new_client_key: str,
        client_name: str,
        api_key: str,
        model: str,
        grid_url: str | None,
        api_key_status: str,
        api_key_validation_error: str | None,
    ) -> dict[str, Any] | None:
        existing = self._get_client_sync(current_client_key)
        if existing is None:
            return None

        now = datetime.utcnow()
        self._get_collection("clients").update_one(
            {"client_key": current_client_key},
            {
                "$set": {
                    "client_key": new_client_key,
                    "client_name": client_name,
                    "api_key": api_key,
                    "model": model,
                    "grid_url": grid_url,
                    "api_key_status": api_key_status,
                    "api_key_last_validated_at": now,
                    "api_key_validation_error": api_key_validation_error,
                    "updated_at": now,
                },
            },
        )

        if new_client_key != current_client_key:
            rename_filter = {"client_key": current_client_key}
            rename_update = {"$set": {"client_key": new_client_key, "client_name": client_name}}
            self._get_collection("client_domains").update_many(rename_filter, rename_update)
            self._get_collection("process_runs").update_many(
                rename_filter,
                {"$set": {"client_key": new_client_key, "client_name": client_name, "request.client_name": client_name}},
            )
            self._get_collection("process_run_items").update_many(rename_filter, rename_update)
            self._get_collection("domain_checks").update_many(rename_filter, rename_update)
            self._get_collection("client_jobs").update_many(rename_filter, rename_update)
        else:
            rename_filter = {"client_key": current_client_key}
            rename_update = {"$set": {"client_name": client_name}}
            self._get_collection("client_domains").update_many(rename_filter, rename_update)
            self._get_collection("process_runs").update_many(
                rename_filter,
                {"$set": {"client_name": client_name, "request.client_name": client_name}},
            )
            self._get_collection("process_run_items").update_many(rename_filter, rename_update)
            self._get_collection("domain_checks").update_many(rename_filter, rename_update)
            self._get_collection("client_jobs").update_many(rename_filter, rename_update)

        return self._get_client_sync(new_client_key)

    async def upsert_client_domain(
        self,
        client_key: str,
        client_name: str,
        domain_key: str,
        requested_capability: RequestedCapability,
        ats_check: bool,
        job_extract: bool,
        job_monitoring: bool,
    ) -> None:
        await asyncio.to_thread(
            self._upsert_client_domain_sync,
            client_key,
            client_name,
            domain_key,
            requested_capability,
            ats_check,
            job_extract,
            job_monitoring,
        )

    def _upsert_client_domain_sync(
        self,
        client_key: str,
        client_name: str,
        domain_key: str,
        requested_capability: RequestedCapability,
        ats_check: bool,
        job_extract: bool,
        job_monitoring: bool,
    ) -> None:
        now = datetime.utcnow()
        log_event(
            logger,
            "info",
            "mongodb_upsert_client_domain client_key=%s domain_key=%s capability=%s",
            client_key,
            domain_key,
            requested_capability,
            domain=domain_key,
            client_key=client_key,
            domain_key=domain_key,
            requested_capability=requested_capability,
        )
        self._get_collection("client_domains").update_one(
            {"client_key": client_key, "domain_key": domain_key},
            {
                "$set": {
                    "client_name": client_name,
                    "requested_capability": requested_capability,
                    "ats_check": ats_check,
                    "job_extract": job_extract,
                    "job_monitoring": job_monitoring,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "client_key": client_key,
                    "domain_key": domain_key,
                    "created_at": now,
                },
            },
            upsert=True,
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

    async def list_process_runs_for_client(
        self,
        client_key: str,
        *,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        return await asyncio.to_thread(self._list_process_runs_for_client_sync, client_key, page, page_size)

    def _list_process_runs_for_client_sync(
        self,
        client_key: str,
        page: int,
        page_size: int,
    ) -> tuple[list[dict[str, Any]], int]:
        normalized_page = max(1, int(page or 1))
        normalized_page_size = max(1, min(int(page_size or 50), 200))
        skip = (normalized_page - 1) * normalized_page_size
        total = self._get_collection("process_runs").count_documents({"client_key": client_key})
        cursor = (
            self._get_collection("process_runs")
            .find({"client_key": client_key}, {"_id": 0})
            .sort("created_at", -1)
            .skip(skip)
            .limit(normalized_page_size)
        )
        return list(cursor), total

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

    async def get_client_domains(self, client_key: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._get_client_domains_sync, client_key)

    def _get_client_domains_sync(self, client_key: str) -> list[dict[str, Any]]:
        cursor = self._get_collection("client_domains").find({"client_key": client_key}, {"_id": 0})
        return list(cursor)

    async def list_client_jobs(self, client_key: str, limit: int = 500) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_client_jobs_sync, client_key, limit)

    def _list_client_jobs_sync(self, client_key: str, limit: int) -> list[dict[str, Any]]:
        cursor = (
            self._get_collection("client_jobs")
            .find({"client_key": client_key}, {"_id": 0})
            .sort("updated_at", -1)
            .limit(limit)
        )
        return list(cursor)

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
        domain_check_id: str,
    ) -> None:
        await asyncio.to_thread(
            self._mark_url_completed_sync,
            process_id,
            url,
            result_summary,
            result_payload,
            domain_check_id,
        )

    def _mark_url_completed_sync(
        self,
        process_id: str,
        url: str,
        result_summary: dict[str, Any],
        result_payload: dict[str, Any],
        domain_check_id: str,
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
                    "result_summary": result_summary,
                    "result_payload": result_payload,
                    "domain_check_id": domain_check_id,
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

    async def list_client_jobs_for_process(self, process_id: str, limit: int = 500) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_client_jobs_for_process_sync, process_id, limit)

    def _list_client_jobs_for_process_sync(self, process_id: str, limit: int) -> list[dict[str, Any]]:
        cursor = (
            self._get_collection("client_jobs")
            .find({"process_id": process_id}, {"_id": 0})
            .sort("updated_at", -1)
            .limit(limit)
        )
        return list(cursor)

    async def get_domain(self, domain_key: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_domain_sync, domain_key)

    def _get_domain_sync(self, domain_key: str) -> dict[str, Any] | None:
        return self._get_collection("domains").find_one({"domain_key": domain_key}, {"_id": 0})

    async def upsert_domain(self, domain_key: str, updates: dict[str, Any]) -> None:
        await asyncio.to_thread(self._upsert_domain_sync, domain_key, updates)

    def _upsert_domain_sync(self, domain_key: str, updates: dict[str, Any]) -> None:
        now = datetime.utcnow()
        self._get_collection("domains").update_one(
            {"domain_key": domain_key},
            {
                "$set": {
                    **updates,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "domain_key": domain_key,
                    "normalized_domain": domain_key,
                    "created_at": now,
                },
            },
            upsert=True,
        )

    async def insert_domain_check(self, document: dict[str, Any]) -> None:
        await asyncio.to_thread(self._insert_domain_check_sync, document)

    def _insert_domain_check_sync(self, document: dict[str, Any]) -> None:
        self._get_collection("domain_checks").insert_one(document)

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

    async def upsert_client_job(
        self,
        *,
        client_key: str,
        client_name: str,
        domain_key: str,
        raw_url: str,
        process_id: str,
        job_key: str,
        document: dict[str, Any],
    ) -> None:
        await asyncio.to_thread(
            self._upsert_client_job_sync,
            client_key,
            client_name,
            domain_key,
            raw_url,
            process_id,
            job_key,
            document,
        )

    def _upsert_client_job_sync(
        self,
        client_key: str,
        client_name: str,
        domain_key: str,
        raw_url: str,
        process_id: str,
        job_key: str,
        document: dict[str, Any],
    ) -> None:
        now = datetime.utcnow()
        self._get_collection("client_jobs").update_one(
            {
                "client_key": client_key,
                "domain_key": domain_key,
                "job_key": job_key,
            },
            {
                "$set": {
                    **document,
                    "client_name": client_name,
                    "raw_url": raw_url,
                    "process_id": process_id,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "client_key": client_key,
                    "domain_key": domain_key,
                    "job_key": job_key,
                    "created_at": now,
                },
            },
            upsert=True,
        )
