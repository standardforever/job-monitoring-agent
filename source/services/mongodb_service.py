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

    async def upsert_client_domain(
        self,
        client_key: str,
        client_name: str,
        domain_key: str,
        requested_capability: RequestedCapability,
        ats_check: bool,
        job_monitoring: bool,
    ) -> None:
        await asyncio.to_thread(
            self._upsert_client_domain_sync,
            client_key,
            client_name,
            domain_key,
            requested_capability,
            ats_check,
            job_monitoring,
        )

    def _upsert_client_domain_sync(
        self,
        client_key: str,
        client_name: str,
        domain_key: str,
        requested_capability: RequestedCapability,
        ats_check: bool,
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

    async def list_process_runs_for_client(self, client_key: str, limit: int = 50) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_process_runs_for_client_sync, client_key, limit)

    def _list_process_runs_for_client_sync(self, client_key: str, limit: int) -> list[dict[str, Any]]:
        cursor = (
            self._get_collection("process_runs")
            .find({"client_key": client_key}, {"_id": 0})
            .sort("created_at", -1)
            .limit(limit)
        )
        return list(cursor)

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
