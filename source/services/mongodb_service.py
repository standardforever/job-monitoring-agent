from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from core.config import get_settings
from pymongo import MongoClient
from utils.logging import get_logger, log_event

logger = get_logger("mongodb_service")


class MongoDBService:
    def __init__(self) -> None:
        settings = get_settings()
        self._uri = settings.mongodb_uri
        self._database_name = settings.mongodb_database
        self._collection_name = settings.mongodb_process_collection
        self._client: MongoClient | None = None
        self._collection = None
        log_event(
            logger,
            "info",
            "mongodb_service_initialized database=%s collection=%s",
            self._database_name,
            self._collection_name,
            domain="mongodb",
            database=self._database_name,
            collection=self._collection_name,
        )

    def _get_collection(self):
        if self._collection is None:
            log_event(
                logger,
                "info",
                "mongodb_connecting uri=%s database=%s collection=%s",
                self._uri,
                self._database_name,
                self._collection_name,
                domain="mongodb",
                database=self._database_name,
                collection=self._collection_name,
            )
            self._client = MongoClient(self._uri)
            database = self._client[self._database_name]
            self._collection = database[self._collection_name]
            log_event(
                logger,
                "info",
                "mongodb_collection_ready",
                domain="mongodb",
                database=self._database_name,
                collection=self._collection_name,
            )
        return self._collection

    async def insert_process(self, document: dict[str, Any]) -> None:
        await asyncio.to_thread(self._insert_process_sync, document)

    def _insert_process_sync(self, document: dict[str, Any]) -> None:
        log_event(
            logger,
            "info",
            "mongodb_insert_process process_id=%s",
            document.get("process_id"),
            domain=(((document.get("request") or {}).get("urls") or ["mongodb"])[0]),
            process_id=document.get("process_id"),
        )
        self._get_collection().insert_one(document)

    async def update_process(self, process_id: str, updates: dict[str, Any]) -> None:
        await asyncio.to_thread(self._update_process_sync, process_id, updates)

    def _update_process_sync(self, process_id: str, updates: dict[str, Any]) -> None:
        updates = {
            **updates,
            "updated_at": datetime.utcnow(),
        }
        log_event(
            logger,
            "info",
            "mongodb_update_process process_id=%s fields=%s",
            process_id,
            sorted(updates.keys()),
            domain="mongodb",
            process_id=process_id,
            update_fields=sorted(updates.keys()),
        )
        self._get_collection().update_one({"process_id": process_id}, {"$set": updates})

    async def update_assignment_status(self, process_id: str, agent_index: int, status: str) -> None:
        await asyncio.to_thread(self._update_assignment_status_sync, process_id, agent_index, status)

    def _update_assignment_status_sync(self, process_id: str, agent_index: int, status: str) -> None:
        log_event(
            logger,
            "info",
            "mongodb_update_assignment_status process_id=%s agent_index=%s status=%s",
            process_id,
            agent_index,
            status,
            domain="mongodb",
            process_id=process_id,
            agent_index=agent_index,
            status=status,
        )
        self._get_collection().update_one(
            {"process_id": process_id, "assignments.agent_index": agent_index},
            {
                "$set": {
                    "assignments.$.status": status,
                    "updated_at": datetime.utcnow(),
                }
            },
        )

    async def mark_url_running(self, process_id: str, url: str) -> None:
        await asyncio.to_thread(self._mark_url_running_sync, process_id, url)

    def _mark_url_running_sync(self, process_id: str, url: str) -> None:
        log_event(
            logger,
            "info",
            "mongodb_mark_url_running process_id=%s url=%s",
            process_id,
            url,
            domain=url,
            process_id=process_id,
            url=url,
        )
        self._get_collection().update_one(
            {"process_id": process_id},
            {
                "$pull": {"queued_urls": url},
                "$addToSet": {"running_urls": url},
                "$inc": {
                    "summary.queued_url_count": -1,
                    "summary.running_url_count": 1,
                },
                "$set": {"updated_at": datetime.utcnow()},
            },
        )

    async def mark_url_completed(self, process_id: str, url: str) -> None:
        await asyncio.to_thread(self._mark_url_completed_sync, process_id, url)

    def _mark_url_completed_sync(self, process_id: str, url: str) -> None:
        log_event(
            logger,
            "info",
            "mongodb_mark_url_completed process_id=%s url=%s",
            process_id,
            url,
            domain=url,
            process_id=process_id,
            url=url,
        )
        self._get_collection().update_one(
            {"process_id": process_id},
            {
                "$pull": {"queued_urls": url, "running_urls": url},
                "$addToSet": {"completed_urls": url},
                "$inc": {
                    "summary.running_url_count": -1,
                    "summary.processed_url_count": 1,
                    "summary.completed_domain_count": 1,
                },
                "$set": {"updated_at": datetime.utcnow()},
            },
        )

    async def mark_url_failed(self, process_id: str, url: str, *, was_running: bool = True) -> None:
        await asyncio.to_thread(self._mark_url_failed_sync, process_id, url, was_running)

    def _mark_url_failed_sync(self, process_id: str, url: str, was_running: bool) -> None:
        log_event(
            logger,
            "info",
            "mongodb_mark_url_failed process_id=%s url=%s was_running=%s",
            process_id,
            url,
            was_running,
            domain=url,
            process_id=process_id,
            url=url,
            was_running=was_running,
        )
        inc_fields = {
            "summary.processed_url_count": 1,
            "summary.failed_domain_count": 1,
        }
        if was_running:
            inc_fields["summary.running_url_count"] = -1
        else:
            inc_fields["summary.queued_url_count"] = -1
        self._get_collection().update_one(
            {"process_id": process_id},
            {
                "$pull": {"queued_urls": url, "running_urls": url},
                "$addToSet": {"failed_urls": url},
                "$inc": inc_fields,
                "$set": {"updated_at": datetime.utcnow()},
            },
        )

    async def append_domain_result(self, process_id: str, worker_index: int, domain_result: dict[str, Any]) -> None:
        await asyncio.to_thread(self._append_domain_result_sync, process_id, worker_index, domain_result)

    def _append_domain_result_sync(self, process_id: str, worker_index: int, domain_result: dict[str, Any]) -> None:
        now = datetime.utcnow()
        log_event(
            logger,
            "info",
            "mongodb_append_domain_result process_id=%s agent_index=%s status=%s",
            process_id,
            worker_index,
            domain_result.get("status"),
            domain=domain_result.get("main_domain") or domain_result.get("domain") or "unknown",
            process_id=process_id,
            agent_index=worker_index,
            status=domain_result.get("status"),
        )
        self._get_collection().update_one(
            {"process_id": process_id},
            {
                "$push": {
                    "domain_result_events": {
                        "agent_index": worker_index,
                        "domain_result": domain_result,
                        "recorded_at": now,
                    }
                },
                "$set": {
                    "updated_at": now,
                },
            },
        )

    async def get_process(self, process_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_process_sync, process_id)

    def _get_process_sync(self, process_id: str) -> dict[str, Any] | None:
        log_event(
            logger,
            "info",
            "mongodb_get_process process_id=%s",
            process_id,
            domain="mongodb",
            process_id=process_id,
        )
        document = self._get_collection().find_one({"process_id": process_id}, {"_id": 0})
        return document
