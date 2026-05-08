"""Simple Flask dashboard for dealer pipeline status."""

from __future__ import annotations

import os
import threading
import time

from flask import Flask, jsonify, render_template, request

from app.bigquery_client import get_bigquery_client
from app.bigquery_repository import BigQueryRepository
from app.config import get_settings
from app.dashboard_service import DashboardService
from app.logging_utils import configure_logging, get_logger


configure_logging()
logger = get_logger(__name__)

app = Flask(__name__, template_folder="templates", static_folder="static")

_DASHBOARD_CACHE_TTL_SECONDS = 120
_dashboard_cache_lock = threading.Lock()
_dashboard_cache_payload: dict | None = None
_dashboard_cache_generated_at: float = 0.0


def _get_dashboard_service() -> DashboardService:
    """Build the shared dashboard service."""

    settings = get_settings()
    repository = BigQueryRepository(get_bigquery_client())
    return DashboardService(repository, settings)


def _get_cached_dashboard_data(force_refresh: bool = False) -> dict:
    """Return cached dashboard data with a short TTL to protect the live route."""

    global _dashboard_cache_payload
    global _dashboard_cache_generated_at

    now = time.time()
    with _dashboard_cache_lock:
        cache_is_fresh = (
            not force_refresh
            and _dashboard_cache_payload is not None
            and (now - _dashboard_cache_generated_at) < _DASHBOARD_CACHE_TTL_SECONDS
        )
        if cache_is_fresh:
            return _dashboard_cache_payload

    try:
        fresh_payload = _get_dashboard_service().get_dashboard_data()
    except Exception:
        with _dashboard_cache_lock:
            if _dashboard_cache_payload is not None:
                logger.exception("Dashboard refresh failed; serving cached payload instead.")
                return _dashboard_cache_payload
        raise

    with _dashboard_cache_lock:
        _dashboard_cache_payload = fresh_payload
        _dashboard_cache_generated_at = time.time()
        return fresh_payload


@app.get("/")
def dashboard() -> str:
    """Render the one-page dashboard."""

    data = _get_cached_dashboard_data()
    return render_template("dashboard.html", data=data)


@app.get("/client-records")
def client_records() -> str:
    """Render a read-only page for client records in the prospect lead table."""

    page = request.args.get("page", default=1, type=int) or 1
    page_size = request.args.get("page_size", default=100, type=int) or 100
    data = _get_dashboard_service().get_client_records_page(page=page, page_size=page_size)
    return render_template("client_records.html", data=data)


@app.get("/discovery-candidates")
def discovery_candidates() -> str:
    """Render a read-only page for domain discovery candidate rows."""

    page = request.args.get("page", default=1, type=int) or 1
    page_size = request.args.get("page_size", default=100, type=int) or 100
    data = _get_dashboard_service().get_discovery_candidates_page(page=page, page_size=page_size)
    return render_template("discovery_candidates.html", data=data)


@app.get("/healthz")
def healthcheck():
    """Return a minimal health check for load balancers and Cloud Run."""

    try:
        data = _get_cached_dashboard_data(force_refresh=True)
        return jsonify(
            {
                "status": "ok",
                "project_id": data["project_id"],
                "dataset": data["dataset"],
            }
        )
    except Exception as exc:  # pragma: no cover - operational safety path
        logger.exception("Dashboard health check failed.")
        return jsonify({"status": "error", "message": str(exc)}), 500


def run_dashboard() -> None:
    """Run the dashboard with Flask's built-in server for local development."""

    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    run_dashboard()
