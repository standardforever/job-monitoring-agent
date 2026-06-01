from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from utils.logging import get_logger, log_event

logger = get_logger("config")


def load_environment() -> None:
    """Load env files from both repo root and source/ for local API runs."""
    source_env = Path(__file__).resolve().parents[1] / ".env"
    repo_env = Path(__file__).resolve().parents[2] / ".env"
    for env_path in (repo_env, source_env):
        if env_path.exists():
            load_dotenv(env_path, override=False)
            log_event(
                logger,
                "info",
                "environment_loaded env_path=%s",
                str(env_path),
                domain="config",
                env_path=str(env_path),
            )


load_environment()


@dataclass(slots=True)
class Settings:
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-5-nano")
    selenium_remote_url: str = os.getenv("SELENIUM_REMOTE_URL", "http://127.0.0.1:4445/wd/hub")
    default_agent_count: int = int(os.getenv("DEFAULT_AGENT_COUNT", "1"))
    post_navigation_delay_ms: int = int(os.getenv("POST_NAVIGATION_DELAY_MS", "5000"))
    mongodb_uri: str = os.getenv("MONGODB_URI", "mongodb://admin:secret@127.0.0.1:27017")
    mongodb_database: str = os.getenv("MONGODB_DATABASE", "job_monitoring_agent")
    mongodb_process_runs_collection: str = os.getenv("MONGODB_PROCESS_RUNS_COLLECTION", "process_runs")
    mongodb_process_run_items_collection: str = os.getenv("MONGODB_PROCESS_RUN_ITEMS_COLLECTION", "process_run_items")
    mongodb_jobs_collection: str = os.getenv("MONGODB_JOBS_COLLECTION", "jobs")
    mongodb_job_extraction_cache_collection: str = os.getenv("MONGODB_JOB_EXTRACTION_CACHE_COLLECTION", "job_extraction_cache")


def get_settings() -> Settings:
    settings = Settings()
    log_event(
        logger,
        "info",
        "settings_loaded mongodb_database=%s process_runs_collection=%s",
        settings.mongodb_database,
        settings.mongodb_process_runs_collection,
        domain="config",
        mongodb_database=settings.mongodb_database,
        mongodb_process_runs_collection=settings.mongodb_process_runs_collection,
    )
    return settings
