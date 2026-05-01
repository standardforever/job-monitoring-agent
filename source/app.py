from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from api.process_routes import router as process_router
from services.mongodb_service import MongoDBService
from utils.logging import get_logger, log_event

app = FastAPI(title="Job Monitoring Agent", root_path="/ats")
logger = get_logger("app")
ui_directory = Path(__file__).resolve().parent / "ui"
mongodb_service = MongoDBService()

app.include_router(process_router, prefix="/api")
app.mount("/assets", StaticFiles(directory=ui_directory), name="ui-assets")
log_event(
    logger,
    "info",
    "fastapi_application_initialized",
    domain="api",
)


@app.on_event("startup")
async def ensure_mongodb_indexes() -> None:
    await mongodb_service.ensure_indexes()
    log_event(logger, "info", "fastapi_startup_indexes_ensured", domain="mongodb")


def _render_ui_html(request: Request) -> HTMLResponse:
    log_event(logger, "info", "ui_index_requested", domain="ui")
    root_path = str(request.scope.get("root_path", "") or "")
    api_base = f"{request.base_url.scheme}://{request.base_url.netloc}{root_path}/api/"
    asset_base = f"{root_path}/assets/"
    html = (ui_directory / "index.html").read_text(encoding="utf-8")
    html = html.replace("__API_BASE__", api_base)
    html = html.replace("__ASSET_BASE__", asset_base)
    return HTMLResponse(content=html)


@app.get("/", include_in_schema=False)
async def ui_index(request: Request) -> HTMLResponse:
    return _render_ui_html(request)


@app.get("/ui", include_in_schema=False)
async def ui_index_alias(request: Request) -> HTMLResponse:
    return _render_ui_html(request)


@app.get("/ui/index.html", include_in_schema=False)
async def ui_index_file_alias(request: Request) -> HTMLResponse:
    return _render_ui_html(request)


if __name__ == "__main__":
    log_event(
        logger,
        "info",
        "uvicorn_start_requested host=%s port=%s reload=%s",
        "127.0.0.1",
        8110,
        True,
        domain="api",
        host="127.0.0.1",
        port=8110,
        reload=True,
    )
    uvicorn.run("app:app", host="127.0.0.1", port=8110, reload=True)
