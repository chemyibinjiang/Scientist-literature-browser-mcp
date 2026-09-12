#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from collections import deque
import re
import secrets
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import unquote, urlparse

try:
    from scientist_literature_mcp.literature_browser_resources import (
        CachedMemoryGuard,
        MemoryCapacityPolicy,
    )
except ImportError:
    from literature_browser_resources import CachedMemoryGuard, MemoryCapacityPolicy


HOST = os.environ.get("LITERATURE_POOL_HOST", "0.0.0.0")
PORT = int(os.environ.get("LITERATURE_POOL_PORT", "9030"))
DEFAULT_TIMEOUT_SECONDS = int(
    os.environ.get("LITERATURE_POOL_DEFAULT_TIMEOUT_SECONDS", "600")
)
MAX_TIMEOUT_SECONDS = int(
    os.environ.get("LITERATURE_POOL_MAX_TIMEOUT_SECONDS", "600")
)
LEGACY_MAX_QUEUE_DEPTH = int(
    os.environ.get("LITERATURE_POOL_MAX_QUEUE_DEPTH", "100")
)
RAW_LOGICAL_REQUEST_CAPACITY = os.environ.get(
    "LITERATURE_POOL_LOGICAL_REQUEST_CAPACITY", ""
).strip()
MAX_BODY_BYTES = int(os.environ.get("LITERATURE_POOL_MAX_BODY_BYTES", "65536"))
BACKEND_COOLDOWN_SECONDS = int(
    os.environ.get("LITERATURE_POOL_BACKEND_COOLDOWN_SECONDS", "300")
)
BACKEND_REPAIR_TIMEOUT_SECONDS = int(
    os.environ.get("LITERATURE_POOL_BACKEND_REPAIR_TIMEOUT_SECONDS", "90")
)
MAX_BROWSER_COUNT = 150
DEFAULT_BROWSER_COUNT = 20
BACKEND_READY_TIMEOUT_SECONDS = 1.5
BACKEND_POLICY_CACHE_SECONDS = 60.0
AFFINITY_KEY_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")
LEASE_OPERATIONS = {
    "serving_external",
    "checking",
    "warming",
}
ADMINISTRATIVE_STATES = {
    "draining",
    "replacing",
    "warming",
    "retired",
}
ENGINE_ADMIN_TOKEN_FILE = Path(
    os.environ.get(
        "LITERATURE_ENGINE_ADMIN_TOKEN_FILE",
        "/run/secrets/literature-engine/admin-token",
    )
)
POOL_ADMIN_TOKEN_FILE = Path(
    os.environ.get(
        "LITERATURE_POOL_ADMIN_TOKEN_FILE",
        "/run/secrets/literature-engine/admin-token",
    )
)
RAW_PUBLISHER_STATE_FILE = os.environ.get(
    "LITERATURE_POOL_PUBLISHER_STATE_FILE",
    "",
).strip()
PUBLISHER_STATE_FILE = (
    Path(RAW_PUBLISHER_STATE_FILE) if RAW_PUBLISHER_STATE_FILE else None
)


def configured_float(
    name: str,
    default: float,
    *,
    minimum: float,
    maximum: float,
) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


class LiteraturePoolError(RuntimeError):
    status = 500


class LiteraturePoolBusy(LiteraturePoolError):
    status = 429


class LiteraturePoolAffinityNotReady(LiteraturePoolError):
    status = 403


class LiteraturePoolRequestError(LiteraturePoolError):
    status = 400


class LiteraturePoolUpstreamError(LiteraturePoolError):
    def __init__(self, status: int, payload: dict[str, Any]):
        super().__init__(str(payload.get("message") or "literature browser failed"))
        self.status = status
        self.payload = payload


def parse_backend_urls(raw: str) -> tuple[str, ...]:
    urls: list[str] = []
    for item in raw.replace("\n", ",").split(","):
        url = item.strip().rstrip("/")
        if not url:
            continue
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError(f"invalid literature browser backend URL: {url}")
        if url not in urls:
            urls.append(url)
    if not urls:
        raise ValueError("at least one literature browser backend is required")
    return tuple(urls)


def generated_backend_urls(
    count: int,
    engine_base_url: str = "",
    engine_shard_size: int = 0,
) -> tuple[str, ...]:
    bounded = max(1, min(int(count), MAX_BROWSER_COUNT))
    if engine_base_url.strip():
        bases = parse_backend_urls(engine_base_url)
        if len(bases) == 1:
            return tuple(
                f"{bases[0]}/profiles/{index:03d}"
                for index in range(1, bounded + 1)
            )
        shard_size = max(
            1,
            min(
                int(engine_shard_size or ((bounded + len(bases) - 1) // len(bases))),
                MAX_BROWSER_COUNT,
            ),
        )
        urls: list[str] = []
        for local_index in range(1, shard_size + 1):
            for shard_index, base in enumerate(bases):
                global_index = shard_index * shard_size + local_index
                if global_index <= bounded:
                    urls.append(f"{base}/profiles/{global_index:03d}")
        if len(urls) != bounded:
            raise ValueError("literature browser engine shards do not cover capacity")
        return tuple(urls)
    indices = (
        [*range(3, bounded + 1), 1, 2]
        if bounded >= 3
        else list(range(1, bounded + 1))
    )
    return tuple(
        "http://campus-literature-browser:9020"
        if index == 1
        else f"http://campus-literature-browser-{index:02d}:9020"
        for index in indices
    )


def configured_browser_count(raw: Any) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = DEFAULT_BROWSER_COUNT
    return max(1, min(value, MAX_BROWSER_COUNT))


def resolved_backend_urls(
    raw_urls: str,
    raw_count: Any,
    engine_base_url: str = "",
    engine_shard_size: int = 0,
) -> tuple[str, ...]:
    if raw_urls.strip():
        return parse_backend_urls(raw_urls)
    return generated_backend_urls(
        configured_browser_count(raw_count),
        engine_base_url,
        engine_shard_size,
    )


def browser_id_for_url(url: str, fallback_index: int) -> str:
    parsed = urlparse(url)
    profile_match = re.search(r"/profiles/([0-9]{3})$", parsed.path)
    if profile_match:
        return f"browser-{int(profile_match.group(1)):03d}"
    host = (parsed.hostname or "").lower()
    if host == "campus-literature-browser":
        return "browser-001"
    prefix = "campus-literature-browser-"
    if host.startswith(prefix):
        suffix = host[len(prefix) :]
        if suffix.isdigit():
            return f"browser-{int(suffix):03d}"
    return f"browser-{fallback_index:03d}"


def shard_id_for_url(url: str) -> str:
    parsed = urlparse(url)
    return (parsed.hostname or "unknown").lower()


def parse_browser_id_sequence(raw: str) -> tuple[str, ...]:
    found: list[str] = []
    for item in raw.replace("\n", ",").split(","):
        value = item.strip().lower()
        if not value:
            continue
        range_match = re.fullmatch(
            r"browser-(\d{3})\s*(?:\.\.|-)\s*browser-(\d{3})",
            value,
        )
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            step = 1 if end >= start else -1
            for index in range(start, end + step, step):
                browser_id = f"browser-{index:03d}"
                if browser_id not in found:
                    found.append(browser_id)
            continue
        if not re.fullmatch(r"browser-\d{3}", value):
            raise ValueError(f"invalid browser id in warm profile set: {item}")
        if value not in found:
            found.append(value)
    return tuple(found)


def configured_reserved_affinity_ids() -> dict[str, tuple[str, ...]]:
    rsc = parse_browser_id_sequence(
        os.environ.get("LITERATURE_POOL_RSC_WARM_BROWSER_IDS", "")
    )
    return {"rsc": rsc} if rsc else {}


def affinity_warm_lease_seconds() -> int:
    try:
        value = int(os.environ.get("LITERATURE_POOL_AFFINITY_WARM_LEASE_SECONDS", "600"))
    except (TypeError, ValueError):
        value = 600
    return max(60, min(value, 3600))


PUBLISHER_HOST_AFFINITIES = (
    ("acs", ("pubs.acs.org", "acs.org", "acs.figshare.com")),
    ("rsc", ("pubs.rsc.org", "rsc.org", "rscj.silverchair-cdn.com")),
    ("wiley", ("onlinelibrary.wiley.com", "wiley.com")),
    ("elsevier", ("sciencedirect.com", "elsevier.com", "elsevier.io", "els-cdn.com")),
    ("nature-springer", ("nature.com", "springer.com", "springernature.com")),
    ("science", ("science.org",)),
    ("pnas", ("pnas.org",)),
)
PUBLISHER_DOI_AFFINITIES = (
    ("acs", ("10.1021/",)),
    ("rsc", ("10.1039/",)),
    ("wiley", ("10.1002/",)),
    ("elsevier", ("10.1016/",)),
    ("nature-springer", ("10.1038/", "10.1007/")),
    ("science", ("10.1126/",)),
    ("pnas", ("10.1073/",)),
)


def host_matches(host: str, domains: tuple[str, ...]) -> bool:
    normalized = host.lower().rstrip(".")
    return any(
        normalized == domain or normalized.endswith(f".{domain}")
        for domain in domains
    )


def publisher_affinity_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    for publisher_id, domains in PUBLISHER_HOST_AFFINITIES:
        if host_matches(host, domains):
            return publisher_id
    if host in {"doi.org", "dx.doi.org"}:
        doi = unquote(parsed.path.lstrip("/")).lower()
        for publisher_id, prefixes in PUBLISHER_DOI_AFFINITIES:
            if doi.startswith(prefixes):
                return publisher_id
    return ""


def publisher_capability_affinity(
    affinity_key: str,
    resource_kind: str,
) -> str:
    normalized = affinity_key.strip().lower()
    if not normalized:
        return ""
    return (
        f"{normalized}-si"
        if resource_kind == "supplementary_pdf"
        else normalized
    )


class BackendPolicySummary:
    """Cache browser policy metadata without slowing the pool health endpoint."""

    def __init__(self, ttl_seconds: float = BACKEND_POLICY_CACHE_SECONDS):
        self.ttl_seconds = max(1.0, float(ttl_seconds))
        self._lock = threading.Lock()
        self._expires_at = 0.0
        self._allowed_domain_count = 0

    def allowed_domain_count(self, urls: tuple[str, ...]) -> int:
        now = time.monotonic()
        with self._lock:
            if now < self._expires_at:
                return self._allowed_domain_count

        discovered = 0
        for backend in urls[:2]:
            request = urllib.request.Request(f"{backend}/ready", method="GET")
            try:
                with urllib.request.urlopen(
                    request,
                    timeout=BACKEND_READY_TIMEOUT_SECONDS,
                ) as response:
                    value = json.loads(response.read().decode("utf-8", errors="replace"))
                if isinstance(value, dict):
                    discovered = max(0, int(value.get("allowed_domain_count") or 0))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            if discovered:
                break

        with self._lock:
            if discovered:
                self._allowed_domain_count = discovered
            self._expires_at = time.monotonic() + self.ttl_seconds
            return self._allowed_domain_count


def pool_admin_authorized(handler: BaseHTTPRequestHandler) -> bool:
    try:
        expected = POOL_ADMIN_TOKEN_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        expected = ""
    if expected:
        supplied = handler.headers.get("X-Scientist-Pool-Admin", "")
        return secrets.compare_digest(supplied, expected)
    return handler.client_address[0] in {"127.0.0.1", "::1"}


def engine_admin_request(
    browser_id: str,
    action: str,
    payload: dict[str, Any],
    *,
    pool: "BrowserLeasePool",
) -> dict[str, Any]:
    if action not in {"begin", "commit", "rollback"}:
        raise LiteraturePoolRequestError("invalid replacement action")
    backend = pool.backend_url(browser_id)
    try:
        token = ENGINE_ADMIN_TOKEN_FILE.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise LiteraturePoolRequestError(
            "literature engine admin token is unavailable"
        ) from exc
    if not token:
        raise LiteraturePoolRequestError("literature engine admin token is empty")
    request = urllib.request.Request(
        f"{backend}/v1/admin/replacement/{action}",
        data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Scientist-Engine-Admin": token,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            value = json.loads(response.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        raise LiteraturePoolBusy(
            f"literature engine rejected replacement with HTTP {exc.code}: "
            f"{detail[:300]}"
        ) from exc
    except (OSError, TimeoutError, json.JSONDecodeError) as exc:
        raise LiteraturePoolBusy(
            f"literature engine replacement request failed: {type(exc).__name__}"
        ) from exc
    if not isinstance(value, dict) or value.get("success") is not True:
        raise LiteraturePoolBusy("literature engine replacement request failed")
    return value


BACKEND_URLS = resolved_backend_urls(
    os.environ.get("LITERATURE_BROWSER_URLS", ""),
    os.environ.get("LITERATURE_POOL_BROWSER_COUNT", "20"),
    os.environ.get("LITERATURE_BROWSER_ENGINE_URLS", "")
    or os.environ.get("LITERATURE_BROWSER_ENGINE_URL", ""),
    int(os.environ.get("LITERATURE_POOL_BROWSER_SHARD_SIZE", "0") or "0"),
)
CONFIGURED_BROWSER_COUNT = len(BACKEND_URLS)
try:
    LOGICAL_REQUEST_CAPACITY = int(RAW_LOGICAL_REQUEST_CAPACITY)
except ValueError:
    LOGICAL_REQUEST_CAPACITY = CONFIGURED_BROWSER_COUNT + LEGACY_MAX_QUEUE_DEPTH
LOGICAL_REQUEST_CAPACITY = max(
    CONFIGURED_BROWSER_COUNT,
    min(LOGICAL_REQUEST_CAPACITY, 2000),
)
MAX_QUEUE_DEPTH = LOGICAL_REQUEST_CAPACITY - CONFIGURED_BROWSER_COUNT
MEMORY_GUARD = CachedMemoryGuard(
    MemoryCapacityPolicy.from_environ(CONFIGURED_BROWSER_COUNT),
    sample_seconds=configured_float(
        "LITERATURE_MEMORY_SAMPLE_SECONDS",
        5.0,
        minimum=0.1,
        maximum=300.0,
    ),
)


class BrowserLeasePool:
    def __init__(
        self,
        urls: tuple[str, ...],
        *,
        memory_decision: Any | None = None,
        reserved_affinity_ids: dict[str, tuple[str, ...]] | None = None,
        warm_lease_seconds: int | None = None,
        publisher_state_path: Path | None = None,
    ):
        self.urls = urls
        self._available = deque(urls)
        self._leased: set[str] = set()
        self._lease_operations: dict[str, str] = {}
        self._quarantined: set[str] = set()
        self._administrative_states: dict[str, str] = {}
        self._condition = threading.Condition()
        self._operator_limit = max(
            1,
            min(
                configured_browser_count(
                    os.environ.get("LITERATURE_POOL_INITIAL_OPERATOR_LIMIT", len(urls))
                ),
                len(urls),
            ),
        )
        self._memory_decision = memory_decision or (
            lambda: {
                "pressure": "normal",
                "target": len(urls),
                "total_bytes": None,
                "available_bytes": None,
                "available_ratio": None,
            }
        )
        self._waiting = 0
        self._waiting_by_operation = {
            operation: 0 for operation in LEASE_OPERATIONS
        }
        self._completed = 0
        self._failed = 0
        self._publisher_failed = 0
        self._repair_payloads: dict[str, dict[str, Any]] = {}
        self._repair_timers: dict[str, threading.Timer] = {}
        self._affinity: dict[str, str] = {}
        self._publisher_capabilities: dict[str, dict[str, dict[str, Any]]] = {}
        self._affinity_warm_lease_seconds = (
            warm_lease_seconds
            if warm_lease_seconds is not None
            else affinity_warm_lease_seconds()
        )
        self._records = {
            url: {
                "browser_id": browser_id_for_url(url, index),
                "shard_id": shard_id_for_url(url),
                "leases": 0,
                "completed": 0,
                "failed": 0,
                "publisher_failures": 0,
                "consecutive_failures": 0,
                "repair_attempts": 0,
                "repair_failures": 0,
                "last_used_at": None,
                "last_success_at": None,
                "last_failure_at": None,
                "last_failure_host": None,
                "last_publisher_failure_at": None,
                "last_publisher_failure_host": None,
                "last_repair_at": None,
                "last_repair_status": "not_needed",
            }
            for index, url in enumerate(urls, start=1)
        }
        configured_reserved = (
            reserved_affinity_ids
            if reserved_affinity_ids is not None
            else configured_reserved_affinity_ids()
        )
        self._affinity_reserved: dict[str, tuple[str, ...]] = {}
        for affinity_key, browser_ids in configured_reserved.items():
            normalized_key = str(affinity_key or "").strip().lower()
            if not normalized_key or not AFFINITY_KEY_RE.fullmatch(normalized_key):
                continue
            reserved_urls: list[str] = []
            for browser_id in browser_ids:
                url = self._url_for_browser_id(str(browser_id).strip().lower())
                if url and url not in reserved_urls:
                    reserved_urls.append(url)
            if reserved_urls:
                self._affinity_reserved[normalized_key] = tuple(reserved_urls)
        self._reserved_urls = {
            url
            for urls_for_affinity in self._affinity_reserved.values()
            for url in urls_for_affinity
        }
        self._affinity_warm: dict[str, dict[str, dict[str, Any]]] = {
            affinity_key: {
                url: {
                    "status": "cold",
                    "last_success_at": None,
                    "next_refresh_due_at": None,
                    "warm_until_epoch": 0.0,
                    "consecutive_failures": 0,
                }
                for url in reserved_urls
            }
            for affinity_key, reserved_urls in self._affinity_reserved.items()
        }
        self._publisher_state_path = publisher_state_path
        self._publisher_state_loaded = False
        self._publisher_state_error = ""
        self._load_publisher_state()

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    @staticmethod
    def _timestamp_from_epoch(value: float) -> str:
        return datetime.fromtimestamp(value, timezone.utc).isoformat(
            timespec="seconds"
        )

    def _url_for_browser_id(self, browser_id: str) -> str | None:
        for url, record in self._records.items():
            if record["browser_id"] == browser_id:
                return url
        return None

    @staticmethod
    def _bounded_counter(value: Any) -> int:
        try:
            return max(0, min(int(value or 0), 1_000_000_000))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _bounded_epoch(value: Any) -> float:
        try:
            return max(0.0, min(float(value or 0.0), 32_503_680_000.0))
        except (TypeError, ValueError):
            return 0.0

    def _load_publisher_state(self) -> None:
        path = self._publisher_state_path
        if path is None:
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError) as exc:
            self._publisher_state_error = type(exc).__name__
            return
        if not isinstance(payload, dict) or payload.get("schema") != (
            "scientist_literature_publisher_capabilities/v1"
        ):
            self._publisher_state_error = "invalid_state_schema"
            return
        capabilities = payload.get("publisher_capabilities")
        if isinstance(capabilities, dict):
            for affinity_key, records in capabilities.items():
                normalized_key = str(affinity_key or "").strip().lower()
                if not AFFINITY_KEY_RE.fullmatch(normalized_key) or not isinstance(
                    records, dict
                ):
                    continue
                restored: dict[str, dict[str, Any]] = {}
                for browser_id, raw in records.items():
                    backend = self._url_for_browser_id(str(browser_id).strip().lower())
                    if backend is None or not isinstance(raw, dict):
                        continue
                    restored[backend] = {
                        "status": str(raw.get("status") or "unknown")[:24],
                        "successes": self._bounded_counter(raw.get("successes")),
                        "failures": self._bounded_counter(raw.get("failures")),
                        "consecutive_failures": self._bounded_counter(
                            raw.get("consecutive_failures")
                        ),
                        "last_success_at": raw.get("last_success_at"),
                        "last_failure_at": raw.get("last_failure_at"),
                        "warm_until_epoch": self._bounded_epoch(
                            raw.get("warm_until_epoch")
                        ),
                    }
                if restored:
                    self._publisher_capabilities[normalized_key] = restored
        warm_affinities = payload.get("warm_affinities")
        if isinstance(warm_affinities, dict):
            for affinity_key, records in warm_affinities.items():
                normalized_key = str(affinity_key or "").strip().lower()
                configured = self._affinity_warm.get(normalized_key)
                if configured is None or not isinstance(records, dict):
                    continue
                for browser_id, raw in records.items():
                    backend = self._url_for_browser_id(str(browser_id).strip().lower())
                    if backend not in configured or not isinstance(raw, dict):
                        continue
                    configured[backend].update(
                        {
                            "status": str(raw.get("status") or "cold")[:24],
                            "last_success_at": raw.get("last_success_at"),
                            "next_refresh_due_at": raw.get("next_refresh_due_at"),
                            "warm_until_epoch": self._bounded_epoch(
                                raw.get("warm_until_epoch")
                            ),
                            "consecutive_failures": self._bounded_counter(
                                raw.get("consecutive_failures")
                            ),
                        }
                    )
        administrative_states = payload.get("administrative_states")
        if isinstance(administrative_states, dict):
            for browser_id, raw_state in administrative_states.items():
                backend = self._url_for_browser_id(str(browser_id).strip().lower())
                state = str(raw_state or "").strip().lower()
                if backend is None or state not in ADMINISTRATIVE_STATES:
                    continue
                self._administrative_states[backend] = state
                try:
                    self._available.remove(backend)
                except ValueError:
                    pass
        now = time.time()
        for affinity_key, records in self._publisher_capabilities.items():
            candidates = [
                (float(record.get("warm_until_epoch") or 0.0), backend)
                for backend, record in records.items()
                if float(record.get("warm_until_epoch") or 0.0) > now
            ]
            if candidates:
                self._affinity[affinity_key] = max(candidates)[1]
        for affinity_key, records in self._affinity_warm.items():
            candidates = [
                (float(record.get("warm_until_epoch") or 0.0), backend)
                for backend, record in records.items()
                if float(record.get("warm_until_epoch") or 0.0) > now
            ]
            if candidates:
                self._affinity[affinity_key] = max(candidates)[1]
        self._publisher_state_loaded = True
        self._publisher_state_error = ""

    def _persist_publisher_state(self) -> None:
        path = self._publisher_state_path
        if path is None:
            return
        capabilities: dict[str, dict[str, dict[str, Any]]] = {}
        for affinity_key, records in self._publisher_capabilities.items():
            capabilities[affinity_key] = {
                self._records[backend]["browser_id"]: dict(record)
                for backend, record in records.items()
                if backend in self._records
            }
        warm_affinities: dict[str, dict[str, dict[str, Any]]] = {}
        for affinity_key, records in self._affinity_warm.items():
            warm_affinities[affinity_key] = {
                self._records[backend]["browser_id"]: dict(record)
                for backend, record in records.items()
                if backend in self._records
            }
        payload = {
            "schema": "scientist_literature_publisher_capabilities/v1",
            "saved_at": self._timestamp(),
            "publisher_capabilities": capabilities,
            "warm_affinities": warm_affinities,
            "administrative_states": {
                self._records[backend]["browser_id"]: state
                for backend, state in self._administrative_states.items()
                if backend in self._records
            },
        }
        temporary = path.with_name(f".{path.name}.tmp")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
            self._publisher_state_error = ""
        except OSError as exc:
            self._publisher_state_error = type(exc).__name__
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    def backend_url(self, browser_id: str) -> str:
        with self._condition:
            url = self._url_for_browser_id(browser_id)
        if url is None:
            raise LiteraturePoolRequestError("unknown browser_id")
        return url

    def _resource_decision(self) -> dict[str, Any]:
        decision = self._memory_decision()
        if not isinstance(decision, dict):
            decision = {}
        try:
            target = int(decision.get("target") or len(self.urls))
        except (TypeError, ValueError):
            target = len(self.urls)
        return {
            "pressure": str(decision.get("pressure") or "unknown"),
            "target": max(1, min(target, len(self.urls))),
            "total_bytes": decision.get("total_bytes"),
            "available_bytes": decision.get("available_bytes"),
            "available_ratio": decision.get("available_ratio"),
        }

    def _effective_limit(self, decision: dict[str, Any] | None = None) -> int:
        value = decision or self._resource_decision()
        return max(1, min(self._operator_limit, int(value["target"])))

    def _next_available(
        self,
        preferred: str | None = None,
        *,
        affinity_key: str = "",
        allow_reserved_cold_fallback: bool = False,
    ) -> str | None:
        normalized_affinity = affinity_key.strip().lower()
        if normalized_affinity in self._affinity_reserved:
            reserved = self._next_reserved_affinity_available(normalized_affinity)
            if reserved is not None or not allow_reserved_cold_fallback:
                return reserved
        if normalized_affinity:
            capable = self._next_capable_affinity_available(
                normalized_affinity,
                preferred=preferred,
            )
            if capable:
                return capable
        if (
            preferred
            and preferred in self._available
            and preferred not in self._quarantined
        ):
            self._available.remove(preferred)
            return preferred
        prefer_unreserved = bool(self._reserved_urls)
        for url in tuple(self._available):
            if url in self._quarantined:
                continue
            if prefer_unreserved and url in self._reserved_urls:
                continue
            self._available.remove(url)
            return url
        if prefer_unreserved:
            for url in tuple(self._available):
                if url in self._quarantined:
                    continue
                self._available.remove(url)
                return url
        return None

    def _next_reserved_affinity_available(self, affinity_key: str) -> str | None:
        now = time.time()
        warm_records = self._affinity_warm.get(affinity_key, {})
        candidates = []
        for url in self._affinity_reserved.get(affinity_key, ()):
            if url not in self._available or url in self._quarantined:
                continue
            record = warm_records.get(url, {})
            if float(record.get("warm_until_epoch") or 0.0) <= now:
                continue
            candidates.append(
                (
                    float(record.get("warm_until_epoch") or 0.0),
                    self._records[url]["browser_id"],
                    url,
                )
            )
        if not candidates:
            return None
        url = min(candidates)[2]
        self._available.remove(url)
        return url

    def _reserved_affinity_has_warm_profile(self, affinity_key: str) -> bool:
        now = time.time()
        warm_records = self._affinity_warm.get(affinity_key, {})
        return any(
            url not in self._quarantined
            and float(warm_records.get(url, {}).get("warm_until_epoch") or 0.0)
            > now
            for url in self._affinity_reserved.get(affinity_key, ())
        )

    def _publisher_capability_state(
        self,
        affinity_key: str,
        url: str,
        *,
        now: float | None = None,
    ) -> str:
        record = self._publisher_capabilities.get(affinity_key, {}).get(url)
        if not record:
            return "unknown"
        current = time.time() if now is None else now
        if (
            record.get("status") == "warm"
            and float(record.get("warm_until_epoch") or 0.0) > current
        ):
            return "warm"
        if int(record.get("successes") or 0) > 0 and int(
            record.get("consecutive_failures") or 0
        ) == 0:
            return "stale"
        if int(record.get("failures") or 0) > 0:
            return "failed"
        return "unknown"

    def _publisher_capability_sort_key(
        self,
        affinity_key: str,
        url: str,
        *,
        preferred: str | None = None,
        now: float | None = None,
    ) -> tuple[int, int, int, int, str]:
        state = self._publisher_capability_state(affinity_key, url, now=now)
        rank = {"warm": 0, "stale": 1, "unknown": 2, "failed": 3}.get(state, 2)
        capability = self._publisher_capabilities.get(affinity_key, {}).get(url, {})
        return (
            rank,
            0 if preferred and url == preferred else 1,
            int(self._records[url].get("leases") or 0),
            int(capability.get("consecutive_failures") or 0),
            self._records[url]["browser_id"],
        )

    def _next_capable_affinity_available(
        self,
        affinity_key: str,
        *,
        preferred: str | None = None,
    ) -> str | None:
        now = time.time()
        candidates: list[tuple[tuple[int, int, int, int, str], str]] = []
        prefer_unreserved = bool(self._reserved_urls)
        for url in tuple(self._available):
            if url in self._quarantined:
                continue
            if prefer_unreserved and url in self._reserved_urls:
                continue
            candidates.append(
                (
                    self._publisher_capability_sort_key(
                        affinity_key,
                        url,
                        preferred=preferred,
                        now=now,
                    ),
                    url,
                )
            )
        if not candidates and prefer_unreserved:
            for url in tuple(self._available):
                if url in self._quarantined:
                    continue
                candidates.append(
                    (
                        self._publisher_capability_sort_key(
                            affinity_key,
                            url,
                            preferred=preferred,
                            now=now,
                        ),
                        url,
                    )
                )
        if not candidates:
            return None
        url = min(candidates)[1]
        self._available.remove(url)
        return url

    def _mark_publisher_capability(
        self,
        affinity_key: str,
        backend_url: str,
        *,
        success: bool,
    ) -> None:
        normalized_affinity = affinity_key.strip().lower()
        if not normalized_affinity or not AFFINITY_KEY_RE.fullmatch(
            normalized_affinity
        ):
            return
        publisher = self._publisher_capabilities.setdefault(normalized_affinity, {})
        record = publisher.setdefault(
            backend_url,
            {
                "status": "unknown",
                "successes": 0,
                "failures": 0,
                "consecutive_failures": 0,
                "last_success_at": None,
                "last_failure_at": None,
                "warm_until_epoch": 0.0,
            },
        )
        now = time.time()
        if success:
            warm_until = now + self._affinity_warm_lease_seconds
            record.update(
                {
                    "status": "warm",
                    "successes": int(record.get("successes") or 0) + 1,
                    "consecutive_failures": 0,
                    "last_success_at": self._timestamp_from_epoch(now),
                    "warm_until_epoch": warm_until,
                }
            )
            self._affinity[normalized_affinity] = backend_url
        else:
            record.update(
                {
                    "status": "failed",
                    "failures": int(record.get("failures") or 0) + 1,
                    "consecutive_failures": int(
                        record.get("consecutive_failures") or 0
                    )
                    + 1,
                    "last_failure_at": self._timestamp_from_epoch(now),
                    "warm_until_epoch": 0.0,
                }
            )
            if self._affinity.get(normalized_affinity) == backend_url:
                self._affinity.pop(normalized_affinity, None)

    def mark_affinity_warm(
        self,
        affinity_key: str,
        backend_url: str,
        *,
        success: bool,
    ) -> None:
        normalized_affinity = affinity_key.strip().lower()
        if not normalized_affinity:
            return
        with self._condition:
            self._mark_publisher_capability(
                normalized_affinity,
                backend_url,
                success=success,
            )
            record = self._affinity_warm.get(normalized_affinity, {}).get(
                backend_url
            )
            if record is not None:
                now = time.time()
                if success:
                    warm_until = now + self._affinity_warm_lease_seconds
                    record.update(
                        {
                            "status": "warm",
                            "last_success_at": self._timestamp_from_epoch(now),
                            "next_refresh_due_at": self._timestamp_from_epoch(
                                warm_until
                            ),
                            "warm_until_epoch": warm_until,
                            "consecutive_failures": 0,
                        }
                    )
                    self._affinity[normalized_affinity] = backend_url
                else:
                    record.update(
                        {
                            "status": "stale",
                            "next_refresh_due_at": self._timestamp_from_epoch(now),
                            "warm_until_epoch": 0.0,
                            "consecutive_failures": int(
                                record.get("consecutive_failures") or 0
                            )
                            + 1,
                        }
                    )
                    if self._affinity.get(normalized_affinity) == backend_url:
                        self._affinity.pop(normalized_affinity, None)
            self._persist_publisher_state()

    def _schedule_repair(self, url: str, *, delay: int | None = None) -> None:
        with self._condition:
            existing = self._repair_timers.get(url)
            if existing is not None and existing.is_alive():
                return
            timer = threading.Timer(
                max(1, BACKEND_COOLDOWN_SECONDS if delay is None else delay),
                self._attempt_repair,
                args=(url,),
            )
            timer.daemon = True
            self._repair_timers[url] = timer
            timer.start()

    def _attempt_repair(self, url: str) -> None:
        with self._condition:
            self._repair_timers.pop(url, None)
            payload = dict(self._repair_payloads.get(url) or {})
            record = self._records[url]
            record["repair_attempts"] += 1
            record["last_repair_at"] = self._timestamp()
            record["last_repair_status"] = "running"
        repair_status = "recovered"
        try:
            # Determine browser health without replaying the publisher request.
            # Replaying a Cloudflare timeout can create a solver stampede while
            # proving nothing about the browser container itself.
            request = urllib.request.Request(f"{url}/ready", method="GET")
            with urllib.request.urlopen(
                request,
                timeout=max(10, BACKEND_REPAIR_TIMEOUT_SECONDS),
            ) as response:
                response.read()
            if payload:
                repair_status = "backend_healthy_request_deferred"
        except Exception:
            with self._condition:
                record = self._records[url]
                record["repair_failures"] += 1
                record["last_repair_status"] = "failed"
            self._schedule_repair(
                url,
                delay=min(max(1, BACKEND_COOLDOWN_SECONDS) * 2, 900),
            )
            return
        with self._condition:
            record = self._records[url]
            record["consecutive_failures"] = 0
            record["last_repair_status"] = repair_status
            self._repair_payloads.pop(url, None)
            self._quarantined.discard(url)
            if url not in self._leased and url not in self._available:
                self._available.append(url)
            self._condition.notify_all()

    def request_repair(
        self,
        browser_id: str,
        *,
        backend_only: bool = False,
    ) -> dict[str, Any]:
        with self._condition:
            url = self._url_for_browser_id(browser_id)
            if url is None:
                raise LiteraturePoolRequestError("unknown browser_id")
            existing = self._repair_timers.pop(url, None)
            if existing is not None:
                existing.cancel()
            if backend_only:
                # A container recreation proves the browser service itself. Do not
                # reclassify a persistent publisher failure as another bad browser.
                self._repair_payloads.pop(url, None)
            self._quarantined.add(url)
            record = self._records[url]
            record["last_repair_status"] = "scheduled"
        self._schedule_repair(url, delay=1)
        return self.status()

    def set_capacity(self, count: int) -> dict[str, Any]:
        if count < 1 or count > len(self.urls):
            raise LiteraturePoolRequestError(
                f"browser capacity must be between 1 and {len(self.urls)}"
            )
        with self._condition:
            self._operator_limit = int(count)
            self._condition.notify_all()
        return self.status()

    def set_browser_state(
        self,
        browser_id: str,
        state: str,
        *,
        expected_state: str = "",
    ) -> dict[str, Any]:
        """Apply an atomic administrative state transition to one profile."""
        normalized = state.strip().lower()
        expected = expected_state.strip().lower()
        if normalized not in ADMINISTRATIVE_STATES | {"available"}:
            raise LiteraturePoolRequestError("invalid browser administrative state")
        with self._condition:
            url = self._url_for_browser_id(browser_id)
            if url is None:
                raise LiteraturePoolRequestError("unknown browser_id")
            current = self._administrative_states.get(url, "available")
            if expected and current != expected:
                raise LiteraturePoolBusy(
                    f"{browser_id} state changed from expected {expected} to {current}"
                )
            if url in self._leased:
                raise LiteraturePoolBusy(
                    f"{browser_id} is active and cannot change administrative state"
                )
            allowed = {
                "available": {"draining", "retired"},
                "draining": {"available", "replacing", "retired"},
                "replacing": {"available", "warming", "retired"},
                "warming": {"available", "replacing", "retired"},
                "retired": {"available"},
            }
            if normalized != current and normalized not in allowed.get(current, set()):
                raise LiteraturePoolRequestError(
                    f"invalid browser state transition {current} -> {normalized}"
                )
            if normalized == "available":
                self._administrative_states.pop(url, None)
                if url not in self._quarantined and url not in self._available:
                    self._available.append(url)
            else:
                self._administrative_states[url] = normalized
                try:
                    self._available.remove(url)
                except ValueError:
                    pass
            self._persist_publisher_state()
            self._condition.notify_all()
            return {
                "browser_id": browser_id,
                "previous_state": current,
                "state": normalized,
            }

    def browser_state(self, browser_id: str) -> str:
        with self._condition:
            url = self._url_for_browser_id(browser_id)
            if url is None:
                raise LiteraturePoolRequestError("unknown browser_id")
            if url in self._leased:
                return self._lease_operations.get(url, "active")
            if url in self._quarantined:
                return "quarantined"
            return self._administrative_states.get(url, "available")

    def operator_limit(self) -> int:
        with self._condition:
            return self._operator_limit

    def effective_limit(self) -> int:
        decision = self._resource_decision()
        with self._condition:
            return self._effective_limit(decision)

    def is_reserved_affinity(self, affinity_key: str) -> bool:
        with self._condition:
            return affinity_key.strip().lower() in self._affinity_reserved

    def clear_affinity(self, affinity_key: str, backend_url: str) -> None:
        if not affinity_key:
            return
        with self._condition:
            if self._affinity.get(affinity_key) == backend_url:
                self._affinity.pop(affinity_key, None)

    @contextmanager
    def lease(
        self,
        timeout: float,
        *,
        request_host: str = "",
        repair_payload: dict[str, Any] | None = None,
        affinity_key: str = "",
        allow_reserved_cold_fallback: bool = False,
    ) -> Iterator[str]:
        deadline = time.monotonic() + max(0.001, timeout)
        with self._condition:
            self._waiting += 1
            self._waiting_by_operation["serving_external"] += 1
        try:
            with self._condition:
                while True:
                    if (
                        affinity_key in self._affinity_reserved
                        and not allow_reserved_cold_fallback
                        and not self._reserved_affinity_has_warm_profile(
                            affinity_key
                        )
                    ):
                        raise LiteraturePoolAffinityNotReady(
                            "publisher profile refresh required; no verified "
                            "warm profile is currently available"
                        )
                    decision = self._resource_decision()
                    limit = self._effective_limit(decision)
                    if len(self._leased) < limit:
                        url = self._next_available(
                            self._affinity.get(affinity_key)
                            if affinity_key
                            else None,
                            affinity_key=affinity_key,
                            allow_reserved_cold_fallback=(
                                allow_reserved_cold_fallback
                            ),
                        )
                        if url is not None:
                            self._leased.add(url)
                            self._lease_operations[url] = "serving_external"
                            record = self._records[url]
                            record["leases"] += 1
                            record["last_used_at"] = self._timestamp()
                            break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise LiteraturePoolBusy(
                            "all literature browsers remained busy until the request deadline"
                        )
                    self._condition.wait(timeout=min(1.0, remaining))
        finally:
            with self._condition:
                self._waiting -= 1
                self._waiting_by_operation["serving_external"] -= 1
        succeeded = False
        quarantine = False
        publisher_failed = False
        try:
            yield url
            succeeded = True
        except LiteraturePoolUpstreamError as exc:
            quarantine = upstream_error_profile_failure(exc)
            publisher_failed = not quarantine
            raise
        finally:
            with self._condition:
                self._leased.discard(url)
                self._lease_operations.pop(url, None)
                if succeeded:
                    self._completed += 1
                    record = self._records[url]
                    record["completed"] += 1
                    record["consecutive_failures"] = 0
                    record["last_success_at"] = self._timestamp()
                    record["last_repair_status"] = "not_needed"
                    if affinity_key and affinity_key not in self._affinity_reserved:
                        self._affinity[affinity_key] = url
                elif publisher_failed:
                    self._publisher_failed += 1
                    record = self._records[url]
                    record["publisher_failures"] += 1
                    record["last_publisher_failure_at"] = self._timestamp()
                    record["last_publisher_failure_host"] = request_host or None
                else:
                    self._failed += 1
                    record = self._records[url]
                    record["failed"] += 1
                    record["consecutive_failures"] += 1
                    record["last_failure_at"] = self._timestamp()
                    record["last_failure_host"] = request_host or None
                    if (
                        affinity_key
                        and affinity_key not in self._affinity_reserved
                        and self._affinity.get(affinity_key) == url
                    ):
                        self._affinity.pop(affinity_key, None)
                if quarantine:
                    self._quarantined.add(url)
                    self._repair_payloads[url] = dict(repair_payload or {})
                    record["last_repair_status"] = "scheduled"
                elif url not in self._available:
                    self._available.append(url)
                self._condition.notify_all()
            if quarantine:
                self._schedule_repair(url)

    @contextmanager
    def lease_specific(
        self,
        browser_id: str,
        timeout: float,
        *,
        request_host: str = "",
        repair_payload: dict[str, Any] | None = None,
        operation_kind: str = "checking",
    ) -> Iterator[str]:
        """Lease one named browser for loopback-only maintenance probes."""
        operation = operation_kind.strip().lower()
        if operation not in {"checking", "warming"}:
            raise LiteraturePoolRequestError("invalid maintenance operation kind")
        url = self._url_for_browser_id(browser_id)
        if url is None:
            raise LiteraturePoolRequestError("unknown browser_id")
        deadline = time.monotonic() + max(0.001, timeout)
        with self._condition:
            self._waiting += 1
            self._waiting_by_operation[operation] += 1
        try:
            with self._condition:
                while True:
                    limit = self._effective_limit(self._resource_decision())
                    administrative_state = self._administrative_states.get(
                        url, "available"
                    )
                    state_ready = (
                        administrative_state == "available"
                        if operation == "checking"
                        else administrative_state == "warming"
                    )
                    ready = (
                        len(self._leased) < limit
                        and url not in self._leased
                        and url not in self._quarantined
                        and state_ready
                        and (
                            url in self._available
                            if operation == "checking"
                            else url not in self._available
                        )
                        and len(self._leased)
                        < max(
                            0,
                            limit
                            - min(
                                limit,
                                self._waiting_by_operation["serving_external"],
                            ),
                        )
                    )
                    if ready:
                        if operation == "checking":
                            self._available.remove(url)
                        self._leased.add(url)
                        self._lease_operations[url] = operation
                        record = self._records[url]
                        record["leases"] += 1
                        record["last_used_at"] = self._timestamp()
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise LiteraturePoolBusy(
                            f"{browser_id} remained unavailable until the probe deadline"
                        )
                    self._condition.wait(timeout=min(1.0, remaining))
        finally:
            with self._condition:
                self._waiting -= 1
                self._waiting_by_operation[operation] -= 1

        succeeded = False
        quarantine = False
        publisher_failed = False
        try:
            yield url
            succeeded = True
        except LiteraturePoolUpstreamError as exc:
            quarantine = upstream_error_profile_failure(exc)
            publisher_failed = not quarantine
            raise
        finally:
            with self._condition:
                self._leased.discard(url)
                self._lease_operations.pop(url, None)
                record = self._records[url]
                if succeeded:
                    self._completed += 1
                    record["completed"] += 1
                    record["consecutive_failures"] = 0
                    record["last_success_at"] = self._timestamp()
                    record["last_repair_status"] = "not_needed"
                elif publisher_failed:
                    self._publisher_failed += 1
                    record["publisher_failures"] += 1
                    record["last_publisher_failure_at"] = self._timestamp()
                    record["last_publisher_failure_host"] = request_host or None
                else:
                    self._failed += 1
                    record["failed"] += 1
                    record["consecutive_failures"] += 1
                    record["last_failure_at"] = self._timestamp()
                    record["last_failure_host"] = request_host or None
                if quarantine:
                    self._quarantined.add(url)
                    self._repair_payloads[url] = dict(repair_payload or {})
                    record["last_repair_status"] = "scheduled"
                elif (
                    self._administrative_states.get(url, "available") == "available"
                    and url not in self._available
                ):
                    self._available.append(url)
                self._condition.notify_all()
            if quarantine:
                self._schedule_repair(url)

    def status(self) -> dict[str, Any]:
        decision = self._resource_decision()
        with self._condition:
            effective = self._effective_limit(decision)
            now = time.time()
            available = sum(
                url not in self._quarantined
                for url in self._available
            )
            draining = max(0, len(self._leased) - effective)
            browsers = []
            for url in self.urls:
                record = dict(self._records[url])
                if url in self._leased:
                    state = self._lease_operations.get(url, "active")
                elif url in self._quarantined:
                    state = "quarantined"
                elif url in self._administrative_states:
                    state = self._administrative_states[url]
                elif url in self._available:
                    state = "available"
                else:
                    state = "unavailable"
                capabilities = {
                    affinity_key: {
                        **dict(records[url]),
                        "state": self._publisher_capability_state(
                            affinity_key,
                            url,
                            now=now,
                        ),
                    }
                    for affinity_key, records in self._publisher_capabilities.items()
                    if url in records
                }
                browsers.append(
                    {
                        **record,
                        "state": state,
                        "publisher_capabilities": capabilities,
                    }
                )
            shards: dict[str, dict[str, int]] = {}
            for browser in browsers:
                shard_id = str(browser.get("shard_id") or "unknown")
                shard = shards.setdefault(
                    shard_id,
                    {
                        "configured": 0,
                        "available": 0,
                        "active": 0,
                        "quarantined": 0,
                        "unavailable": 0,
                    },
                )
                shard["configured"] += 1
                state = str(browser.get("state") or "unavailable")
                if state == "available":
                    shard["available"] += 1
                elif state in LEASE_OPERATIONS or state == "active":
                    shard["active"] += 1
                elif state == "quarantined":
                    shard["quarantined"] += 1
                else:
                    shard["unavailable"] += 1
            publisher_capabilities: dict[str, Any] = {}
            for affinity_key, records in sorted(self._publisher_capabilities.items()):
                counts = {"warm": 0, "stale": 0, "unknown": 0, "failed": 0}
                successes = 0
                failures = 0
                warm_ids: list[str] = []
                stale_ids: list[str] = []
                failed_sample_ids: list[str] = []
                for url, capability in records.items():
                    state = self._publisher_capability_state(
                        affinity_key,
                        url,
                        now=now,
                    )
                    counts[state] = counts.get(state, 0) + 1
                    successes += int(capability.get("successes") or 0)
                    failures += int(capability.get("failures") or 0)
                    browser_id = self._records[url]["browser_id"]
                    if state == "warm" and len(warm_ids) < 50:
                        warm_ids.append(browser_id)
                    elif state == "stale" and len(stale_ids) < 50:
                        stale_ids.append(browser_id)
                    elif state == "failed" and len(failed_sample_ids) < 25:
                        failed_sample_ids.append(browser_id)
                publisher_capabilities[affinity_key] = {
                    "known": len(records),
                    "warm": counts.get("warm", 0),
                    "stale": counts.get("stale", 0),
                    "failed": counts.get("failed", 0),
                    "unknown": counts.get("unknown", 0),
                    "successes": successes,
                    "failures": failures,
                    "warm_browser_ids": warm_ids,
                    "stale_browser_ids": stale_ids,
                    "failed_sample_browser_ids": failed_sample_ids,
                }
            warm_affinities: dict[str, Any] = {}
            for affinity_key, reserved_urls in self._affinity_reserved.items():
                items = []
                warm = 0
                due = 0
                stale = 0
                active = 0
                for url in reserved_urls:
                    warm_record = dict(
                        self._affinity_warm.get(affinity_key, {}).get(url, {})
                    )
                    runtime_state = (
                        "active"
                        if url in self._leased
                        else "quarantined"
                        if url in self._quarantined
                        else "available"
                        if url in self._available
                        else "unavailable"
                    )
                    warm_until = float(warm_record.get("warm_until_epoch") or 0.0)
                    is_warm = warm_until > now and warm_record.get("status") == "warm"
                    if is_warm:
                        warm += 1
                    else:
                        stale += 1
                    if warm_until <= now:
                        due += 1
                    if runtime_state == "active":
                        active += 1
                    items.append(
                        {
                            "browser_id": self._records[url]["browser_id"],
                            "runtime_state": runtime_state,
                            "warm_state": "warm" if is_warm else str(
                                warm_record.get("status") or "cold"
                            ),
                            "last_success_at": warm_record.get("last_success_at"),
                            "next_refresh_due_at": warm_record.get(
                                "next_refresh_due_at"
                            ),
                            "consecutive_failures": int(
                                warm_record.get("consecutive_failures") or 0
                            ),
                        }
                    )
                warm_affinities[affinity_key] = {
                    "configured": len(reserved_urls),
                    "warm": warm,
                    "due": due,
                    "stale": stale,
                    "active": active,
                    "items": items,
                }
            return {
                "size": len(self.urls),
                "enabled": len(self.urls),
                "assignment_limit": effective,
                "operator_limit": self._operator_limit,
                "memory_limit": int(decision["target"]),
                "memory_pressure": decision["pressure"],
                "memory_total_bytes": decision["total_bytes"],
                "memory_available_bytes": decision["available_bytes"],
                "memory_available_ratio": decision["available_ratio"],
                "available": available,
                "active": len(self._leased),
                "waiting": self._waiting,
                "waiting_by_operation": dict(self._waiting_by_operation),
                "active_by_operation": {
                    operation: sum(
                        active == operation
                        for active in self._lease_operations.values()
                    )
                    for operation in sorted(LEASE_OPERATIONS)
                },
                "disabled": 0,
                "draining": draining,
                "completed": self._completed,
                "failed": self._failed,
                "publisher_failed": self._publisher_failed,
                "request_failures": self._failed + self._publisher_failed,
                "quarantined": len(self._quarantined),
                "shards": shards,
                "affinity_count": len(self._affinity),
                "publisher_state": {
                    "enabled": self._publisher_state_path is not None,
                    "loaded": self._publisher_state_loaded,
                    "error": self._publisher_state_error or None,
                },
                "publisher_capabilities": publisher_capabilities,
                "warm_affinities": warm_affinities,
                "browsers": browsers,
            }


BROWSER_POOL = BrowserLeasePool(
    BACKEND_URLS,
    memory_decision=MEMORY_GUARD.decision,
    publisher_state_path=PUBLISHER_STATE_FILE,
)
BACKEND_POLICY = BackendPolicySummary()
REQUEST_SLOTS = threading.BoundedSemaphore(LOGICAL_REQUEST_CAPACITY)


def bounded_timeout(payload: dict[str, Any]) -> int:
    try:
        requested = int(payload.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)
    except (TypeError, ValueError) as exc:
        raise LiteraturePoolRequestError("timeout_seconds must be an integer") from exc
    return max(45, min(requested, MAX_TIMEOUT_SECONDS))


def browser_payload(
    payload: dict[str, Any],
    *,
    allow_maintenance_session_refresh: bool = False,
) -> dict[str, Any]:
    url = str(payload.get("url") or "").strip()
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise LiteraturePoolRequestError("literature URL must use http or https")
    try:
        wait_ms = max(0, min(int(payload.get("wait_ms") or 5000), 20000))
        max_chars = int(payload.get("max_chars", 0))
        max_figures = max(1, min(int(payload.get("max_figures") or 12), 30))
        figure_offset = int(payload.get("figure_offset") or 0)
    except (TypeError, ValueError) as exc:
        raise LiteraturePoolRequestError(
            "wait_ms, max_chars, max_figures, and figure_offset must be integers"
        ) from exc
    if max_chars != 0 and max_chars < 1000:
        raise LiteraturePoolRequestError(
            "max_chars must be 0 for full text or at least 1000"
        )
    include_figure_images = payload.get("include_figure_images", False)
    if not isinstance(include_figure_images, bool):
        raise LiteraturePoolRequestError(
            "include_figure_images must be a boolean"
        )
    maintenance_session_refresh = payload.get(
        "maintenance_session_refresh",
        False,
    )
    if not isinstance(maintenance_session_refresh, bool):
        raise LiteraturePoolRequestError(
            "maintenance_session_refresh must be a boolean"
        )
    if maintenance_session_refresh and not allow_maintenance_session_refresh:
        raise LiteraturePoolRequestError(
            "maintenance_session_refresh requires the admin browser probe"
        )
    if not 0 <= figure_offset <= 1000:
        raise LiteraturePoolRequestError(
            "figure_offset must be between 0 and 1000"
        )
    affinity_key = str(payload.get("affinity_key") or "").strip().lower()
    resource_kind = str(payload.get("resource_kind") or "article").strip()
    if resource_kind not in {"article", "supplementary_pdf"}:
        raise LiteraturePoolRequestError(
            "resource_kind must be article or supplementary_pdf"
        )
    landing_url_hint = str(payload.get("landing_url_hint") or "").strip()
    if landing_url_hint:
        parsed_hint = urlparse(landing_url_hint)
        if (
            parsed_hint.scheme not in {"http", "https"}
            or not parsed_hint.hostname
            or len(landing_url_hint) > 2048
        ):
            raise LiteraturePoolRequestError(
                "landing_url_hint must be a bounded http or https URL"
            )
    if not affinity_key:
        affinity_key = publisher_affinity_from_url(url)
        if not affinity_key and landing_url_hint:
            affinity_key = publisher_affinity_from_url(landing_url_hint)
    if affinity_key and not AFFINITY_KEY_RE.fullmatch(affinity_key):
        raise LiteraturePoolRequestError(
            "affinity_key must be a lowercase publisher identifier"
        )
    return {
        "url": url,
        "wait_ms": wait_ms,
        "max_chars": max_chars,
        "include_figure_images": include_figure_images,
        "max_figures": max_figures,
        "figure_offset": figure_offset,
        "affinity_key": affinity_key,
        "resource_kind": resource_kind,
        "landing_url_hint": landing_url_hint,
        "maintenance_session_refresh": maintenance_session_refresh,
        "request_priority": (
            "maintenance"
            if allow_maintenance_session_refresh
            else "production"
        ),
    }


def upstream_attempt_timeout(
    url: str,
    remaining: float,
    attempts_left: int,
    *,
    resource_kind: str = "article",
) -> float:
    if attempts_left <= 1:
        return remaining
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    springer_article = (
        host == "link.springer.com"
        or (
            host == "doi.org"
            and parsed.path.lower().startswith("/10.1007/")
        )
    )
    if springer_article:
        # Springer HTML normally resolves in well under a minute. Reserve a
        # complete second-browser attempt when one profile has become stale.
        return max(45.0, min(75.0, remaining * 0.45))
    if resource_kind == "supplementary_pdf":
        # SI paths are often session-bound. Preserve a complete attempt on a
        # later profile instead of spending the whole request budget on stale
        # cookie jars. A healthy publisher-affine profile normally returns
        # within this window; stalled profiles must not consume the retry
        # budget reserved for the remaining attempts.
        return max(
            45.0,
            min(75.0, remaining / max(1, attempts_left)),
        )
    return max(45.0, remaining * 0.9)


def retryable_publisher_session_error(exc: LiteraturePoolUpstreamError) -> bool:
    """Retry a challenge-session failure without declaring the browser broken."""
    if exc.status not in {403, 429}:
        return False
    message = str(exc.payload.get("message") or "").lower()
    return any(
        marker in message
        for marker in (
            "challenge solver",
            "cloudflare",
            "turnstile",
            "cf_clearance",
            "publisher origin challenge",
            "publisher same-origin si fetch reached",
            "publisher supplementary pdf fetch failed",
            "same-origin si fetch reached",
        )
    )


def publisher_affinity_not_ready(
    result: dict[str, Any],
    *,
    affinity_key: str,
    resource_kind: str,
) -> bool:
    if not affinity_key:
        return False
    if resource_kind == "supplementary_pdf":
        pdf = result.get("pdf_extraction")
        pdf = pdf if isinstance(pdf, dict) else {}
        try:
            pdf_bytes = int(pdf.get("bytes") or 0)
        except (TypeError, ValueError):
            pdf_bytes = 0
        return (
            result.get("success") is not True
            or pdf.get("success") is not True
            or pdf_bytes < 4096
        )
    if resource_kind != "article":
        return False
    page_state = str(result.get("page_state") or "").strip().lower()
    access_state = str(result.get("access_state") or "").strip().lower()
    if page_state == "challenge" or access_state == "challenge":
        return True
    return affinity_key == "rsc" and access_state == "metadata_or_abstract"


def publisher_affinity_ready(
    result: dict[str, Any],
    *,
    affinity_key: str,
    resource_kind: str,
) -> bool:
    if not affinity_key or result.get("success") is not True:
        return False
    return not publisher_affinity_not_ready(
        result,
        affinity_key=affinity_key,
        resource_kind=resource_kind,
    )


def publisher_result_proves_profile_capability(result: dict[str, Any]) -> bool:
    """Return true only when the selected browser profile produced the result."""
    text_source = str(result.get("text_source") or "").strip().lower()
    if text_source.startswith(("europe_pmc_", "elsevier_api_")):
        return False
    pdf_extraction = result.get("pdf_extraction")
    pdf_extraction = pdf_extraction if isinstance(pdf_extraction, dict) else {}
    strategy = str(pdf_extraction.get("strategy") or "").strip().lower()
    return not strategy.startswith(("europe_pmc_", "elsevier_api_"))


def stateless_solver_exhausted(
    result: dict[str, Any],
    *,
    resource_kind: str,
) -> bool:
    if resource_kind != "article":
        return False
    challenge_bypass = result.get("challenge_bypass")
    if not isinstance(challenge_bypass, dict):
        return False
    return (
        challenge_bypass.get("attempted") is True
        and challenge_bypass.get("success") is not True
    )


PROFILE_FAILURE_MARKERS = (
    "browser has been closed",
    "browser closed",
    "cannot open display",
    "connection refused",
    "connection reset",
    "context has been closed",
    "context or browser has been closed",
    "context closed",
    "econnrefused",
    "econnreset",
    "invalid browser response",
    "literature browser returned invalid json",
    "literature browser returned non-object json",
    "malformed backend response",
    "maxclients",
    "maximum number of clients",
    "no route to host",
    "page has been closed",
    "page closed",
    "remote end closed connection",
    "target page, context or browser has been closed",
    "target closed",
    "x server",
    "x-server",
    "xvfb",
)


PUBLISHER_FAILURE_MARKERS = (
    "challenge solver",
    "cloudflare",
    "cf_clearance",
    "direct pdf fallback reached the request time budget",
    "direct supplementary pdf fetch failed",
    "literature browser transport failed: timed out",
    "publisher navigation reached the request time budget",
    "publisher origin challenge",
    "publisher same-origin si fetch reached",
    "publisher supplementary pdf fetch failed",
    "same-origin si fetch reached",
    "supplementary pdf fetch reached the request time budget",
    "timed out",
    "turnstile",
)


def upstream_error_message(exc: LiteraturePoolUpstreamError) -> str:
    payload = exc.payload if isinstance(exc.payload, dict) else {}
    try:
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        serialized = str(payload)
    return serialized.lower()


def upstream_error_profile_failure(exc: LiteraturePoolUpstreamError) -> bool:
    """Return true only for browser-worker/profile failures worth quarantine."""
    message = upstream_error_message(exc)
    if any(marker in message for marker in PROFILE_FAILURE_MARKERS):
        return True
    if any(marker in message for marker in PUBLISHER_FAILURE_MARKERS):
        return False
    return False


def retryable_upstream_error(
    exc: LiteraturePoolUpstreamError,
    *,
    resource_kind: str = "article",
) -> bool:
    message = upstream_error_message(exc)
    if resource_kind == "article" and "challenge solver" in message:
        # The browser worker already made its bounded fresh-browser attempts.
        # Repeating the same stateless solver work on another profile only
        # multiplies publisher latency; profile rotation cannot improve it.
        return False
    return (
        exc.status in {500, 502, 503, 504}
        or retryable_publisher_session_error(exc)
    )


def proxy_read(
    payload: dict[str, Any],
    *,
    pool: BrowserLeasePool = BROWSER_POOL,
) -> dict[str, Any]:
    timeout = bounded_timeout(payload)
    upstream_payload = browser_payload(payload)
    upstream_payload.pop("maintenance_session_refresh", None)
    affinity_key = str(upstream_payload.pop("affinity_key", ""))
    resource_kind = str(upstream_payload.pop("resource_kind", "article"))
    capability_affinity = publisher_capability_affinity(
        affinity_key,
        resource_kind,
    )
    session_affinity = affinity_key or capability_affinity
    lease_affinity = (
        affinity_key
        if pool.is_reserved_affinity(affinity_key)
        else session_affinity
    )
    allow_reserved_cold_fallback = (
        affinity_key == "rsc"
        and resource_kind in {"article", "supplementary_pdf"}
    )
    request_host = (urlparse(str(upstream_payload["url"])).hostname or "").lower()
    deadline = time.monotonic() + timeout
    acs_solver_bank_request = (
        affinity_key == "acs" and resource_kind == "supplementary_pdf"
    )
    requested_attempts = (
        1
        if acs_solver_bank_request
        else 3 if resource_kind == "supplementary_pdf" else 2
    )
    max_attempts = min(requested_attempts, pool.effective_limit())
    last_error: LiteraturePoolUpstreamError | None = None
    data: dict[str, Any] | None = None
    for attempt in range(max_attempts):
        remaining = deadline - time.monotonic()
        if remaining < 1:
            break
        attempts_left = max_attempts - attempt
        upstream_timeout = upstream_attempt_timeout(
            str(upstream_payload["url"]),
            remaining,
            attempts_left,
            resource_kind=resource_kind,
        )
        attempt_payload = {
            **upstream_payload,
            "timeout_seconds": max(45, int(upstream_timeout) - 5),
        }
        leased_backend = ""
        try:
            with pool.lease(
                remaining,
                request_host=request_host,
                repair_payload=attempt_payload,
                affinity_key=lease_affinity,
                allow_reserved_cold_fallback=allow_reserved_cold_fallback,
            ) as backend:
                leased_backend = backend
                request = urllib.request.Request(
                    f"{backend}/v1/literature/read",
                    data=json.dumps(attempt_payload, ensure_ascii=False).encode("utf-8"),
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    method="POST",
                )
                try:
                    with urllib.request.urlopen(
                        request,
                        timeout=upstream_timeout,
                    ) as response:
                        raw = response.read().decode("utf-8", errors="replace")
                except urllib.error.HTTPError as exc:
                    error_raw = exc.read().decode("utf-8", errors="replace")
                    try:
                        data = json.loads(error_raw)
                    except json.JSONDecodeError:
                        data = {
                            "success": False,
                            "message": f"literature browser returned HTTP {exc.code}",
                        }
                    if not isinstance(data, dict):
                        data = {
                            "success": False,
                            "message": "invalid browser response",
                        }
                    raise LiteraturePoolUpstreamError(exc.code, data) from exc
                except (TimeoutError, urllib.error.URLError) as exc:
                    raise LiteraturePoolUpstreamError(
                        502,
                        {
                            "success": False,
                            "message": (
                                f"literature browser transport failed: {exc}"
                            )[:800],
                        },
                    ) from exc
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise LiteraturePoolUpstreamError(
                        502,
                        {
                            "success": False,
                            "message": "literature browser returned invalid JSON",
                        },
                    ) from exc
                if not isinstance(parsed, dict):
                    raise LiteraturePoolUpstreamError(
                        502,
                        {
                            "success": False,
                            "message": "literature browser returned non-object JSON",
                        },
                        )
                data = parsed
            not_ready = publisher_affinity_not_ready(
                data,
                affinity_key=affinity_key,
                resource_kind=resource_kind,
            )
            if not_ready:
                pool.clear_affinity(capability_affinity, leased_backend)
                if session_affinity != capability_affinity:
                    pool.clear_affinity(session_affinity, leased_backend)
                pool.mark_affinity_warm(
                    capability_affinity,
                    leased_backend,
                    success=False,
                )
                if session_affinity != capability_affinity:
                    pool.mark_affinity_warm(
                        session_affinity,
                        leased_backend,
                        success=False,
                    )
                if (
                    attempt + 1 < max_attempts
                    and not stateless_solver_exhausted(
                        data,
                        resource_kind=resource_kind,
                    )
                ):
                    continue
            else:
                if publisher_result_proves_profile_capability(data):
                    affinity_success = publisher_affinity_ready(
                        data,
                        affinity_key=affinity_key,
                        resource_kind=resource_kind,
                    )
                    pool.mark_affinity_warm(
                        capability_affinity,
                        leased_backend,
                        success=affinity_success,
                    )
                    if session_affinity != capability_affinity:
                        pool.mark_affinity_warm(
                            session_affinity,
                            leased_backend,
                            success=affinity_success,
                        )
            break
        except LiteraturePoolUpstreamError as exc:
            last_error = exc
            if (
                capability_affinity
                and leased_backend
                and not upstream_error_profile_failure(exc)
            ):
                pool.mark_affinity_warm(
                    capability_affinity,
                    leased_backend,
                    success=False,
                )
                if session_affinity != capability_affinity:
                    pool.mark_affinity_warm(
                        session_affinity,
                        leased_backend,
                        success=False,
                    )
            retryable = retryable_upstream_error(
                exc,
                resource_kind=resource_kind,
            )
            if not retryable or attempt + 1 >= max_attempts:
                raise
    else:
        if last_error is not None:
            raise last_error
    if data is None:
        if last_error is not None:
            raise last_error
        raise LiteraturePoolBusy(
            "literature request expired before a browser returned"
        )
    return data


def proxy_read_on_browser(
    payload: dict[str, Any],
    browser_id: str,
    *,
    pool: BrowserLeasePool = BROWSER_POOL,
    lease_timeout_seconds: float = 2.0,
    operation_kind: str = "checking",
) -> dict[str, Any]:
    """Run one maintenance read on an explicitly selected browser profile."""
    timeout = bounded_timeout(payload)
    upstream_payload = browser_payload(
        payload,
        allow_maintenance_session_refresh=True,
    )
    affinity_key = str(upstream_payload.pop("affinity_key", ""))
    resource_kind = str(upstream_payload.pop("resource_kind", "article"))
    capability_affinity = publisher_capability_affinity(
        affinity_key,
        resource_kind,
    )
    upstream_payload["timeout_seconds"] = max(45, timeout - 5)
    request_host = (urlparse(str(upstream_payload["url"])).hostname or "").lower()
    data: dict[str, Any] | None = None
    with pool.lease_specific(
        browser_id,
        max(0.1, min(float(lease_timeout_seconds), 5.0)),
        request_host=request_host,
        repair_payload=upstream_payload,
        operation_kind=operation_kind,
    ) as backend:
        request = urllib.request.Request(
            f"{backend}/v1/literature/read",
            data=json.dumps(upstream_payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            error_raw = exc.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(error_raw)
            except json.JSONDecodeError:
                data = {
                    "success": False,
                    "message": f"literature browser returned HTTP {exc.code}",
                }
            if not isinstance(data, dict):
                data = {"success": False, "message": "invalid browser response"}
            upstream_error = LiteraturePoolUpstreamError(exc.code, data)
            if not upstream_error_profile_failure(upstream_error):
                pool.mark_affinity_warm(
                    capability_affinity,
                    backend,
                    success=False,
                )
            raise upstream_error from exc
        except (TimeoutError, urllib.error.URLError) as exc:
            upstream_error = LiteraturePoolUpstreamError(
                502,
                {
                    "success": False,
                    "message": f"literature browser transport failed: {exc}"[:800],
                },
            )
            if not upstream_error_profile_failure(upstream_error):
                pool.mark_affinity_warm(
                    capability_affinity,
                    backend,
                    success=False,
                )
            raise upstream_error from exc
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise LiteraturePoolUpstreamError(
                502,
                {
                    "success": False,
                    "message": "literature browser returned invalid JSON",
                },
            ) from exc
        if not isinstance(parsed, dict):
            raise LiteraturePoolUpstreamError(
                502,
                {
                    "success": False,
                    "message": "literature browser returned non-object JSON",
                },
        )
        data = parsed
    pool.mark_affinity_warm(
        capability_affinity,
        pool.backend_url(browser_id),
        success=publisher_affinity_ready(
            data,
            affinity_key=affinity_key,
            resource_kind=resource_kind,
        ),
    )
    if data is None:
        raise LiteraturePoolBusy(
            "literature request expired before the selected browser returned"
        )
    return data


def recycle_profile_worker(
    browser_id: str,
    *,
    pool: BrowserLeasePool = BROWSER_POOL,
) -> dict[str, Any]:
    backend = pool.backend_url(browser_id)
    try:
        token = ENGINE_ADMIN_TOKEN_FILE.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise LiteraturePoolRequestError(
            "literature engine admin token is unavailable"
        ) from exc
    if not token:
        raise LiteraturePoolRequestError(
            "literature engine admin token is empty"
        )
    request = urllib.request.Request(
        f"{backend}/v1/admin/recycle",
        data=b"{}",
        headers={
            "Content-Type": "application/json",
            "X-Scientist-Engine-Admin": token,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            value = json.loads(response.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise LiteraturePoolUpstreamError(
            exc.code,
            {
                "success": False,
                "message": f"literature engine recycle failed: {detail}"[:800],
            },
        ) from exc
    except (OSError, TimeoutError, json.JSONDecodeError) as exc:
        raise LiteraturePoolUpstreamError(
            502,
            {
                "success": False,
                "message": (
                    "literature engine recycle transport failed: "
                    f"{type(exc).__name__}"
                ),
            },
        ) from exc
    if not isinstance(value, dict) or value.get("success") is not True:
        raise LiteraturePoolUpstreamError(
            502,
            {"success": False, "message": "literature engine rejected recycle"},
        )
    return value


def probe_summary(result: dict[str, Any]) -> dict[str, Any]:
    extraction = result.get("pdf_extraction")
    if not isinstance(extraction, dict):
        extraction = {}
    return {
        "success": bool(result.get("success")),
        "access_state": str(result.get("access_state") or ""),
        "page_state": str(result.get("page_state") or ""),
        "text_source": str(result.get("text_source") or ""),
        "content_chars": len(str(result.get("text") or "")),
        "download_success": bool(extraction.get("success")),
        "download_status": extraction.get("status"),
        "download_bytes": int(extraction.get("bytes") or 0),
        "pdf_pages": extraction.get("pages"),
    }


def json_response(
    handler: BaseHTTPRequestHandler,
    status: int,
    payload: dict[str, Any],
) -> bool:
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(raw)))
        handler.end_headers()
        handler.wfile.write(raw)
    except ConnectionError:
        return False
    return True


def pool_ready_status(pool_status: dict[str, Any], allowed_domain_count: int) -> bool:
    enabled = int(pool_status.get("enabled") or 0)
    quarantined = int(pool_status.get("quarantined") or 0)
    return allowed_domain_count > 0 and enabled > 0 and quarantined < enabled


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        if urlparse(self.path).path == "/ready":
            pool_status = BROWSER_POOL.status()
            allowed_domain_count = BACKEND_POLICY.allowed_domain_count(
                BROWSER_POOL.urls
            )
            json_response(
                self,
                200,
                {
                    "ready": pool_ready_status(pool_status, allowed_domain_count),
                    "service": "literature-browser-pool",
                    "allowed_domain_count": allowed_domain_count,
                    "allowed_domains_known": allowed_domain_count > 0,
                    "logical_request_capacity": LOGICAL_REQUEST_CAPACITY,
                    "queue_capacity": max(
                        0,
                        LOGICAL_REQUEST_CAPACITY
                        - int(pool_status.get("assignment_limit") or 0),
                    ),
                    "configured_browser_count": CONFIGURED_BROWSER_COUNT,
                    "maximum_browser_count": MAX_BROWSER_COUNT,
                    "pool": pool_status,
                },
            )
            return
        json_response(self, 404, {"success": False, "message": "not found"})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path == "/v1/admin/browser-replacement":
            if not pool_admin_authorized(self):
                json_response(
                    self,
                    403,
                    {"success": False, "message": "loopback access required"},
                )
                return
            browser_id = ""
            action = ""
            try:
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise LiteraturePoolRequestError("invalid request body size")
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(payload, dict):
                    raise LiteraturePoolRequestError("request body must be an object")
                browser_id = str(payload.get("browser_id") or "").strip()
                action = str(payload.get("action") or "").strip().lower()
                replacement_id = str(
                    payload.get("replacement_id") or ""
                ).strip()
                if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", replacement_id):
                    raise LiteraturePoolRequestError("invalid replacement_id")
                current_state = BROWSER_POOL.browser_state(browser_id)
                if action == "begin":
                    if current_state == "available":
                        BROWSER_POOL.set_browser_state(
                            browser_id,
                            "draining",
                            expected_state="available",
                        )
                        BROWSER_POOL.set_browser_state(
                            browser_id,
                            "replacing",
                            expected_state="draining",
                        )
                        current_state = "replacing"
                    elif current_state not in {"replacing", "warming"}:
                        raise LiteraturePoolBusy(
                            f"{browser_id} cannot begin replacement from {current_state}"
                        )
                elif action not in {"commit", "rollback"}:
                    raise LiteraturePoolRequestError("invalid replacement action")
                result = engine_admin_request(
                    browser_id,
                    action,
                    {
                        "replacement_id": replacement_id,
                        "expected_generation_id": str(
                            payload.get("expected_generation_id") or ""
                        ).strip(),
                    },
                    pool=BROWSER_POOL,
                )
                if action == "begin" and current_state == "replacing":
                    BROWSER_POOL.set_browser_state(
                        browser_id,
                        "warming",
                        expected_state="replacing",
                    )
                elif action != "begin" and current_state != "available":
                    BROWSER_POOL.set_browser_state(
                        browser_id,
                        "available",
                        expected_state=current_state,
                    )
                json_response(self, 200, result)
            except LiteraturePoolError as exc:
                json_response(
                    self,
                    exc.status,
                    {"success": False, "message": str(exc)[:1200]},
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                json_response(
                    self,
                    400,
                    {"success": False, "message": "invalid request body"},
                )
            return
        if path == "/v1/admin/browser-state":
            if not pool_admin_authorized(self):
                json_response(
                    self,
                    403,
                    {"success": False, "message": "loopback access required"},
                )
                return
            try:
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise LiteraturePoolRequestError("invalid request body size")
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(payload, dict):
                    raise LiteraturePoolRequestError("request body must be an object")
                transition = BROWSER_POOL.set_browser_state(
                    str(payload.get("browser_id") or "").strip(),
                    str(payload.get("state") or "").strip(),
                    expected_state=str(payload.get("expected_state") or "").strip(),
                )
                json_response(self, 200, {"success": True, **transition})
            except LiteraturePoolError as exc:
                json_response(
                    self,
                    exc.status,
                    {"success": False, "message": str(exc)[:1200]},
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                json_response(
                    self,
                    400,
                    {"success": False, "message": "invalid request body"},
                )
            return
        if path == "/v1/admin/browser-probe":
            if not pool_admin_authorized(self):
                json_response(
                    self,
                    403,
                    {"success": False, "message": "loopback access required"},
                )
                return
            try:
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise LiteraturePoolRequestError("invalid request body size")
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(payload, dict):
                    raise LiteraturePoolRequestError("request body must be an object")
                browser_id = str(payload.pop("browser_id", "") or "").strip()
                if not browser_id:
                    raise LiteraturePoolRequestError("browser_id is required")
                raw_lease_timeout = payload.pop("lease_timeout_seconds", 2)
                operation_kind = str(
                    payload.pop("operation_kind", "checking") or "checking"
                ).strip()
                if isinstance(raw_lease_timeout, bool):
                    raise LiteraturePoolRequestError(
                        "lease_timeout_seconds must be numeric"
                    )
                lease_timeout = float(raw_lease_timeout)
                result = proxy_read_on_browser(
                    payload,
                    browser_id,
                    lease_timeout_seconds=lease_timeout,
                    operation_kind=operation_kind,
                )
                json_response(
                    self,
                    200,
                    {
                        "success": True,
                        "browser_id": browser_id,
                        "probe": probe_summary(result),
                    },
                )
            except LiteraturePoolUpstreamError as exc:
                json_response(self, exc.status, exc.payload)
            except LiteraturePoolError as exc:
                json_response(
                    self,
                    exc.status,
                    {"success": False, "message": str(exc)[:1200]},
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                json_response(
                    self,
                    400,
                    {"success": False, "message": "invalid request body"},
                )
            return
        if path == "/v1/admin/browser-repair":
            if not pool_admin_authorized(self):
                json_response(
                    self,
                    403,
                    {"success": False, "message": "loopback access required"},
                )
                return
            try:
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise LiteraturePoolRequestError("invalid request body size")
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(payload, dict):
                    raise LiteraturePoolRequestError("request body must be an object")
                browser_id = str(payload.get("browser_id") or "").strip()
                if not browser_id:
                    raise LiteraturePoolRequestError("browser_id is required")
                backend_only = payload.get("backend_only") is True
                recycled = None
                if payload.get("restart_worker") is True:
                    recycled = recycle_profile_worker(browser_id)
                json_response(
                    self,
                    202,
                    {
                        "success": True,
                        "browser_id": browser_id,
                        "worker_recycle": recycled,
                        "pool": BROWSER_POOL.request_repair(
                            browser_id,
                            backend_only=backend_only,
                        ),
                    },
                )
            except LiteraturePoolUpstreamError as exc:
                json_response(self, exc.status, exc.payload)
            except LiteraturePoolError as exc:
                json_response(
                    self,
                    exc.status,
                    {"success": False, "message": str(exc)[:1200]},
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                json_response(
                    self,
                    400,
                    {"success": False, "message": "invalid request body"},
                )
            return
        if path == "/v1/admin/capacity":
            if not pool_admin_authorized(self):
                json_response(
                    self,
                    403,
                    {"success": False, "message": "loopback access required"},
                )
                return
            try:
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise LiteraturePoolRequestError("invalid request body size")
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(payload, dict):
                    raise LiteraturePoolRequestError(
                        "request body must be an object"
                    )
                raw_count = payload.get("count")
                if isinstance(raw_count, bool):
                    raise LiteraturePoolRequestError("count must be an integer")
                count = int(raw_count)
                json_response(
                    self,
                    200,
                    {
                        "success": True,
                        "pool": BROWSER_POOL.set_capacity(count),
                    },
                )
            except (TypeError, ValueError):
                json_response(
                    self,
                    400,
                    {"success": False, "message": "count must be an integer"},
                )
            except LiteraturePoolError as exc:
                json_response(
                    self,
                    exc.status,
                    {"success": False, "message": str(exc)[:1200]},
                )
            return
        if path != "/v1/literature/read":
            json_response(self, 404, {"success": False, "message": "not found"})
            return
        if not REQUEST_SLOTS.acquire(blocking=False):
            json_response(
                self,
                429,
                {"success": False, "message": "literature browser queue is full"},
            )
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0 or length > MAX_BODY_BYTES:
                raise LiteraturePoolRequestError("invalid request body size")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(payload, dict):
                raise LiteraturePoolRequestError("request body must be an object")
            json_response(self, 200, proxy_read(payload))
        except LiteraturePoolUpstreamError as exc:
            json_response(self, exc.status, exc.payload)
        except LiteraturePoolError as exc:
            json_response(
                self,
                exc.status,
                {"success": False, "message": str(exc)[:1200]},
            )
        except Exception as exc:
            json_response(
                self,
                500,
                {"success": False, "message": f"literature pool failed: {exc}"[:1200]},
            )
        finally:
            REQUEST_SLOTS.release()


class LiteraturePoolServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = min(LOGICAL_REQUEST_CAPACITY, 256)


def main() -> None:
    server = LiteraturePoolServer((HOST, PORT), Handler)
    print(
        "[literature-browser-pool] "
        f"listen={HOST}:{PORT} browsers={len(BACKEND_URLS)} "
        f"logical_capacity={LOGICAL_REQUEST_CAPACITY}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
