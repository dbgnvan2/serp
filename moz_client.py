"""
moz_client.py
~~~~~~~~~~~~~
Moz Links API v2 client with SQLite caching.

Fetches Domain Authority (DA) and Page Authority (PA) for lists of URLs
using the Moz url_metrics endpoint, and caches results locally to avoid
redundant API calls (DA changes slowly — monthly granularity is sufficient).

Usage
-----
::

    from moz_client import MozClient

    client = MozClient()                      # reads env vars
    metrics = client.get_moz_metrics([
        "https://psychologytoday.com/",
        "https://livingsystems.ca/",
    ])
    # {"https://psychologytoday.com/": {"da": 91, "pa": 64, "fetched_at": "..."},
    #  "https://livingsystems.ca/":    {"da": 19, "pa": 29, "fetched_at": "..."}}

Environment variables
---------------------
MOZ_TOKEN   Moz API token (required) — generated in the Moz API dashboard.

Read at instantiation time.  A ``RuntimeError`` is raised if absent so the
calling pipeline can set ``MOZ_AVAILABLE = False`` and degrade gracefully
rather than fail silently mid-run.

API reference
-------------
Endpoint : POST https://lsapi.seomoz.com/v2/url_metrics
Auth     : x-moz-token header
Batch    : up to 50 URLs per request (MOZ_BATCH_SIZE)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Iterator

import requests

logger = logging.getLogger(__name__)

# Python 3.11+ exposes datetime.UTC; earlier versions need timezone.utc
try:
    from datetime import UTC as _UTC
except ImportError:
    from datetime import timezone as _tz
    _UTC = _tz.utc

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MOZ_ENDPOINT = "https://lsapi.seomoz.com/v2/url_metrics"

#: Maximum URLs per Moz API request (hard limit imposed by Moz).
MOZ_BATCH_SIZE: int = 50

#: Request timeout in seconds.
REQUEST_TIMEOUT: int = 30

#: SQLite table used for caching.
CACHE_TABLE = "moz_cache"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class MozClient:
    """Thin wrapper around the Moz Links API v2 url_metrics endpoint.

    Parameters
    ----------
    db_path:
        Path to the SQLite database used for caching.  Defaults to
        ``"serp_data.db"`` (same DB as the rest of the pipeline).
    cache_ttl_days:
        Number of days before a cached result is considered stale.
        Defaults to 30.  DA changes slowly so frequent refreshes are wasteful.

    Raises
    ------
    RuntimeError
        If the ``MOZ_TOKEN`` environment variable is not set.  Callers should
        catch this and set a ``MOZ_AVAILABLE`` flag.
    """

    def __init__(self, db_path: str = "serp_data.db", cache_ttl_days: int = 30) -> None:
        token = os.getenv("MOZ_TOKEN")
        if not token:
            raise RuntimeError(
                "Moz credentials not found. Set MOZ_TOKEN in your .env file "
                "(generate a token in the Moz API dashboard)."
            )
        self._auth_header = {"x-moz-token": token}
        self._db_path = db_path
        self._cache_ttl = timedelta(days=cache_ttl_days)
        self._init_cache_table()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_moz_metrics(self, url_list: list[str]) -> dict[str, dict]:
        """Return DA and PA for each URL in *url_list*.

        Results are served from the local cache when available and not
        expired.  Uncached or expired URLs are fetched from the Moz API in
        batches of up to :data:`MOZ_BATCH_SIZE`.

        Parameters
        ----------
        url_list:
            List of URLs to look up.  Duplicates are silently deduplicated.

        Returns
        -------
        dict mapping each URL to ``{"da": int, "pa": int, "fetched_at": str}``.
        URLs that could not be fetched (HTTP error, timeout, etc.) are omitted
        from the result rather than raising an exception so a partial batch
        failure doesn't abort the pipeline.
        """
        if not url_list:
            return {}

        unique_urls = list(dict.fromkeys(url_list))  # deduplicate, preserve order
        cached, to_fetch = self._cache_lookup(unique_urls)

        fresh: dict[str, dict] = {}
        if to_fetch:
            for batch in self._batches(to_fetch):
                batch_result = self._fetch_batch(batch)
                fresh.update(batch_result)
            if fresh:
                self._cache_store(fresh)

        return {**cached, **fresh}

    # ------------------------------------------------------------------
    # Internal: API
    # ------------------------------------------------------------------

    def _fetch_batch(self, urls: list[str]) -> dict[str, dict]:
        """POST a single batch of ≤ :data:`MOZ_BATCH_SIZE` URLs to Moz.

        Returns a dict of ``{url: {da, pa}}`` on success, or an empty dict
        if the request fails (error is logged as a warning).
        """
        payload = {"targets": urls}
        try:
            response = requests.post(
                MOZ_ENDPOINT,
                headers={**self._auth_header, "Content-Type": "application/json"},
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
            if not response.ok:
                try:
                    body = response.json()
                except ValueError:
                    body = response.text[:300]
                logger.warning(
                    "Moz API %d error for batch of %d URLs: %s",
                    response.status_code, len(urls), body,
                )
                return {}
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Moz API request failed for batch of %d URLs: %s", len(urls), exc)
            return {}

        try:
            data = response.json()
        except ValueError as exc:
            logger.warning("Moz API returned non-JSON response: %s", exc)
            return {}

        results: dict[str, dict] = {}
        fetched_at = datetime.now(_UTC).isoformat()
        for item in data.get("results", []):
            url = item.get("url") or item.get("page_url") or item.get("page")
            if not url:
                continue
            results[url] = {
                "da": int(item.get("domain_authority") or 0),
                "pa": int(item.get("page_authority") or 0),
                "fetched_at": fetched_at,
            }
        return results

    # ------------------------------------------------------------------
    # Internal: cache
    # ------------------------------------------------------------------

    def _init_cache_table(self) -> None:
        """Create the moz_cache table if it doesn't exist."""
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {CACHE_TABLE} (
                    url               TEXT PRIMARY KEY,
                    domain_authority  INTEGER,
                    page_authority    INTEGER,
                    fetched_at        TEXT
                )
            """)
            conn.commit()

    def _cache_lookup(self, urls: list[str]) -> tuple[dict[str, dict], list[str]]:
        """Split *urls* into (cached_results, urls_needing_fetch).

        A cached entry is considered fresh if its ``fetched_at`` timestamp is
        within :attr:`_cache_ttl` of now.
        """
        cached: dict[str, dict] = {}
        to_fetch: list[str] = []
        cutoff = (datetime.now(_UTC) - self._cache_ttl).isoformat()

        placeholders = ",".join("?" * len(urls))
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                f"SELECT url, domain_authority, page_authority, fetched_at "
                f"FROM {CACHE_TABLE} WHERE url IN ({placeholders})",
                urls,
            ).fetchall()

        fresh_urls = set()
        for url, da, pa, fetched_at in rows:
            if fetched_at and fetched_at >= cutoff:
                cached[url] = {"da": da, "pa": pa, "fetched_at": fetched_at}
                fresh_urls.add(url)

        to_fetch = [u for u in urls if u not in fresh_urls]
        return cached, to_fetch

    def _cache_store(self, results: dict[str, dict]) -> None:
        """Upsert *results* into the cache table."""
        rows = [
            (url, v["da"], v["pa"], v["fetched_at"])
            for url, v in results.items()
        ]
        with sqlite3.connect(self._db_path) as conn:
            conn.executemany(
                f"INSERT OR REPLACE INTO {CACHE_TABLE} "
                f"(url, domain_authority, page_authority, fetched_at) VALUES (?,?,?,?)",
                rows,
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Internal: utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _batches(items: list, size: int = MOZ_BATCH_SIZE) -> Iterator[list]:
        """Yield successive *size*-length chunks from *items*."""
        for i in range(0, len(items), size):
            yield items[i: i + size]
