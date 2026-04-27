from __future__ import annotations

from fastapi import FastAPI
import uvicorn

from api.process_routes import router as process_router
from utils.logging import get_logger, log_event

app = FastAPI(title="Job Monitoring Agent", root_path="/ats")
logger = get_logger("app")

app.include_router(process_router, prefix="/api")
log_event(
    logger,
    "info",
    "fastapi_application_initialized",
    domain="api",
)


if __name__ == "__main__":
    log_event(
        logger,
        "info",
        "uvicorn_start_requested host=%s port=%s reload=%s",
        "127.0.0.1",
        8111,
        True,
        domain="api",
        host="127.0.0.1",
        port=8111,
        reload=True,
    )
    uvicorn.run("app:app", host="127.0.0.1", port=9999, reload=True)
