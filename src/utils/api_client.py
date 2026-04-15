"""
Generic HTTP client for the Barcelona Open Data portal (CKAN API).

All network calls in this project go through this module — never use
`requests` directly in extraction scripts.

Features:
- Session reuse with configurable keep-alive
- Automatic retries with exponential back-off (429, 5xx)
- Optional App-Id / App-Code headers for authenticated endpoints
- Streaming downloads for large files (ZIP, 7z archives)
- Structured logging
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 30          # seconds per request
_DOWNLOAD_TIMEOUT = 120        # seconds for large file downloads
_MAX_RETRIES = 3
_BACKOFF_FACTOR = 1            # wait 1s, 2s, 4s between retries
_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)


class ApiClient:
    """Session-backed HTTP client with retry logic and optional auth headers."""

    def __init__(
        self,
        base_url: str,
        app_id: str = "",
        app_code: str = "",
        timeout: int = _DEFAULT_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = self._build_session(app_id, app_code)

    # ── Public methods ────────────────────────────────────────────────────────

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        """GET a JSON endpoint relative to base_url."""
        url = urljoin(self.base_url + "/", path.lstrip("/"))
        logger.debug("GET %s params=%s", url, params)
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_url(self, url: str, params: dict[str, Any] | None = None) -> dict:
        """GET an absolute URL and return JSON."""
        logger.debug("GET %s", url)
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def download(self, url: str, dest: str | Path) -> Path:
        """Stream-download a file (CSV, ZIP, 7z) to *dest* on disk.

        Uses chunked streaming to avoid loading large archives into memory.
        Returns the destination path.
        """
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading %s → %s", url, dest)
        with self.session.get(
            url, stream=True, timeout=_DOWNLOAD_TIMEOUT
        ) as response:
            response.raise_for_status()
            with dest.open("wb") as fh:
                for chunk in response.iter_content(chunk_size=65_536):
                    fh.write(chunk)
        logger.info("Download complete: %s (%.1f MB)", dest, dest.stat().st_size / 1e6)
        return dest

    def ckan_package(self, package_id: str) -> dict:
        """Return the full CKAN package metadata for *package_id*."""
        return self.get("action/package_show", params={"id": package_id})

    def ckan_resources(self, package_id: str) -> list[dict]:
        """Return the list of CKAN resources for *package_id*."""
        data = self.ckan_package(package_id)
        return data.get("result", {}).get("resources", [])

    # ── Private helpers ───────────────────────────────────────────────────────

    def _build_session(self, app_id: str, app_code: str) -> requests.Session:
        session = requests.Session()

        retry = Retry(
            total=_MAX_RETRIES,
            backoff_factor=_BACKOFF_FACTOR,
            status_forcelist=_RETRY_STATUS_CODES,
            allowed_methods={"GET"},
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        headers: dict[str, str] = {"Accept": "application/json, text/csv, */*"}
        if app_id:
            headers["App-Id"] = app_id
        if app_code:
            headers["App-Code"] = app_code
        session.headers.update(headers)
        return session


def build_client() -> ApiClient:
    """Construct an ApiClient from environment variables.

    Expected env vars (see .env.example):
        BCN_API_BASE_URL   — CKAN API base URL
        BCN_API_APP_ID     — optional App-Id header
        BCN_API_APP_CODE   — optional App-Code header
    """
    return ApiClient(
        base_url=os.environ["BCN_API_BASE_URL"],
        app_id=os.getenv("BCN_API_APP_ID", ""),
        app_code=os.getenv("BCN_API_APP_CODE", ""),
    )
