#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - Linux production only
    fcntl = None


SCHEMA = "literature_browser_profile_patrol/v1"
MERGED_SCHEMA = "scientist_literature_maintenance_snapshot/v1"
ACCEPTED_ACCESS_STATES = {
    "institutional_full_text",
    "publisher_full_text",
    "open_access_full_text",
    "full_text_visible",
}


class ProfilePatrolError(RuntimeError):
    pass


_THREAD_LOCKS: dict[str, threading.Lock] = {}
_THREAD_LOCKS_GUARD = threading.Lock()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.chmod(temporary, 0o640)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


@contextlib.contextmanager
def locked_state(path: Path) -> Iterator[dict[str, Any]]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    if fcntl is None:
        with _THREAD_LOCKS_GUARD:
            process_lock = _THREAD_LOCKS.setdefault(
                str(lock_path.resolve()), threading.Lock()
            )
        with process_lock:
            state = load_json(path, {})
            if not isinstance(state, dict) or state.get("schema") != SCHEMA:
                state = {
                    "schema": SCHEMA,
                    "created_at": iso(utc_now()),
                    "profiles": {},
                    "lanes": {},
                }
            yield state
            state["updated_at"] = iso(utc_now())
            save_json(path, state)
        return
    with lock_path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = load_json(path, {})
        if not isinstance(state, dict) or state.get("schema") != SCHEMA:
            state = {
                "schema": SCHEMA,
                "created_at": iso(utc_now()),
                "profiles": {},
                "lanes": {},
            }
        yield state
        state["updated_at"] = iso(utc_now())
        save_json(path, state)


def docker_container(service: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            "label=com.docker.compose.project=lark-codex",
            "--filter",
            f"label=com.docker.compose.service={service}",
        ],
        check=True,
        text=True,
        capture_output=True,
        timeout=15,
    )
    container = next((line for line in result.stdout.splitlines() if line), "")
    if not container:
        raise ProfilePatrolError(f"{service} is not running")
    return container


def pool_request(
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: int = 300,
) -> dict[str, Any]:
    direct_url = os.environ.get("SCIENTIST_LITERATURE_POOL_URL", "").strip()
    if direct_url:
        headers: dict[str, str] = {}
        if payload is None:
            data = None
            method = "GET"
        else:
            data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
            method = "POST"
        token_path = Path(
            os.environ.get(
                "SCIENTIST_LITERATURE_POOL_ADMIN_TOKEN_FILE",
                "/run/secrets/literature-engine/admin-token",
            )
        )
        try:
            token = token_path.read_text(encoding="utf-8").strip()
        except OSError:
            token = ""
        if token:
            headers["X-Scientist-Pool-Admin"] = token
        request = urllib.request.Request(
            f"{direct_url.rstrip('/')}/{path.lstrip('/')}",
            data=data,
            headers=headers,
            method=method,
        )
        status = 200
        try:
            with urllib.request.urlopen(
                request,
                timeout=max(30, timeout),
            ) as response:
                status = int(getattr(response, "status", 200) or 200)
                raw = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            status = exc.code
            raw = exc.read().decode("utf-8", "replace")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ProfilePatrolError(
                "literature browser pool returned invalid JSON"
            ) from exc
        if not isinstance(value, dict):
            raise ProfilePatrolError(
                "literature browser pool returned invalid JSON"
            )
        if payload is None:
            return value
        return {"http_status": status, "body": value}

    container = docker_container("literature-browser-pool")
    if payload is None:
        script = (
            "import json,urllib.request;"
            f"print(json.dumps(json.load(urllib.request.urlopen('http://127.0.0.1:9030{path}',timeout=15))))"
        )
    else:
        encoded = json.dumps(payload, separators=(",", ":"))
        script = (
            "import json,urllib.error,urllib.request;"
            f"data={encoded!r}.encode('utf-8');"
            f"req=urllib.request.Request('http://127.0.0.1:9030{path}',data=data,headers={{'Content-Type':'application/json'}},method='POST');"
            "status=200;"
            "\ntry:\n raw=urllib.request.urlopen(req,timeout="
            f"{max(30, timeout)}).read().decode('utf-8','replace')"
            "\nexcept urllib.error.HTTPError as exc:\n status=exc.code; raw=exc.read().decode('utf-8','replace')"
            "\ntry:\n body=json.loads(raw)"
            "\nexcept json.JSONDecodeError:\n body={'success':False,'message':'invalid pool response'}"
            "\nprint(json.dumps({'http_status':status,'body':body}))"
        )
    result = subprocess.run(
        ["docker", "exec", container, "python3", "-c", script],
        check=True,
        text=True,
        capture_output=True,
        timeout=max(45, timeout + 30),
    )
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        raise ProfilePatrolError("literature browser pool returned invalid JSON")
    return value


def browser_ids_from_pool(
    snapshot: dict[str, Any],
    *,
    states: set[str] | None = None,
) -> list[str]:
    pool = snapshot.get("pool")
    if not isinstance(pool, dict):
        return []
    browsers = pool.get("browsers")
    if not isinstance(browsers, list):
        return []
    found = []
    for browser in browsers:
        if not isinstance(browser, dict) or browser.get("state") == "disabled":
            continue
        if states is not None and str(browser.get("state") or "") not in states:
            continue
        browser_id = str(browser.get("browser_id") or "").strip()
        if browser_id and browser_id not in found:
            found.append(browser_id)
    return sorted(found)


def sync_runtime_pool_state(
    state: dict[str, Any],
    snapshot: dict[str, Any],
    *,
    now: datetime,
) -> None:
    pool = snapshot.get("pool")
    browsers = pool.get("browsers") if isinstance(pool, dict) else None
    if not isinstance(browsers, list):
        return
    profiles = state.setdefault("profiles", {})
    for browser in browsers:
        if not isinstance(browser, dict):
            continue
        browser_id = str(browser.get("browser_id") or "").strip()
        if not browser_id:
            continue
        profile = profiles.setdefault(browser_id, {})
        runtime_state = str(browser.get("state") or "unknown")
        failures = int(browser.get("consecutive_failures") or 0)
        profile.update(
            {
                "pool_state": runtime_state,
                "pool_consecutive_failures": failures,
                "pool_last_failure_at": browser.get("last_failure_at"),
                "pool_last_failure_host": browser.get("last_failure_host"),
                "pool_observed_at": iso(now),
                "needs_debug": runtime_state in {"quarantined", "unavailable"}
                or failures > 0,
            }
        )


def browser_number(browser_id: str) -> int:
    try:
        return int(browser_id.rsplit("-", 1)[1])
    except (IndexError, ValueError) as exc:
        raise ProfilePatrolError(f"invalid browser id: {browser_id}") from exc


def lane_for_browser(browser_id: str, lane_count: int) -> int:
    return ((browser_number(browser_id) - 1) % max(1, lane_count)) + 1


def lane_browser_ids(
    browser_ids: list[str],
    lane: int,
    lane_count: int,
) -> list[str]:
    return [
        browser_id
        for browser_id in sorted(browser_ids)
        if lane_for_browser(browser_id, lane_count) == lane
    ]


def configured_browser_ids(raw: Any) -> list[str]:
    if raw is None or raw == "":
        return []
    values = raw if isinstance(raw, list) else str(raw).replace("\n", ",").split(",")
    found: list[str] = []
    for item in values:
        value = str(item or "").strip().lower()
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
            raise ProfilePatrolError("profile patrol browser_id is invalid")
        if value not in found:
            found.append(value)
    return found


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    probes = config.get("probes")
    if not isinstance(probes, list) or not probes:
        raise ProfilePatrolError("profile patrol config requires probes")
    normalized = []
    seen = set()
    for raw in probes:
        if not isinstance(raw, dict):
            raise ProfilePatrolError("profile patrol probe must be an object")
        probe_id = str(raw.get("id") or "").strip()
        url = str(raw.get("url") or "").strip()
        landing_url_hint = str(raw.get("landing_url_hint") or "").strip()
        resource_kind = str(raw.get("resource_kind") or "article").strip()
        if (
            not probe_id
            or probe_id in seen
            or not url.startswith(("https://", "http://"))
            or (
                landing_url_hint
                and not landing_url_hint.startswith(("https://", "http://"))
            )
            or resource_kind not in {"article", "supplementary_pdf"}
        ):
            raise ProfilePatrolError("profile patrol probe is invalid")
        seen.add(probe_id)
        browser_ids = configured_browser_ids(
            raw.get("browser_ids", raw.get("browser_id", ""))
        )
        normalized.append(
            {
                **raw,
                "id": probe_id,
                "url": url,
                "landing_url_hint": landing_url_hint,
                "resource_kind": resource_kind,
                "browser_ids": browser_ids,
                "minimum_content_chars": max(
                    1000,
                    int(raw.get("minimum_content_chars") or 12000),
                ),
                "minimum_download_bytes": max(
                    1024,
                    int(raw.get("minimum_download_bytes") or 4096),
                ),
            }
        )
    refreshes = config.get("session_refreshes") or []
    if not isinstance(refreshes, list):
        raise ProfilePatrolError("profile patrol session_refreshes must be a list")
    normalized_refreshes = []
    seen_refreshes = set()
    for raw in refreshes:
        if not isinstance(raw, dict):
            raise ProfilePatrolError("profile patrol session refresh must be an object")
        refresh_id = str(raw.get("id") or "").strip()
        publisher_id = str(raw.get("publisher_id") or "").strip().lower()
        url = str(raw.get("url") or "").strip()
        landing_url_hint = str(raw.get("landing_url_hint") or "").strip()
        resource_kind = str(raw.get("resource_kind") or "article").strip()
        browser_ids = configured_browser_ids(
            raw.get("browser_ids", raw.get("browser_id", ""))
        )
        if (
            not refresh_id
            or not publisher_id
            or not url.startswith(("https://", "http://"))
            or (
                landing_url_hint
                and not landing_url_hint.startswith(("https://", "http://"))
            )
            or resource_kind != "article"
        ):
            raise ProfilePatrolError("profile patrol session refresh is invalid")
        interval_seconds = max(300, int(raw.get("interval_seconds") or 600))
        retry_seconds = max(
            60,
            int(raw.get("retry_seconds") or config.get("retry_seconds") or 300),
        )
        expanded_ids = browser_ids or [""]
        for index, browser_id in enumerate(expanded_ids):
            effective_id = (
                f"{refresh_id}-{browser_id}"
                if browser_id
                else refresh_id
            )
            if effective_id in seen_refreshes:
                raise ProfilePatrolError("profile patrol session refresh is invalid")
            seen_refreshes.add(effective_id)
            normalized_refreshes.append(
                {
                    **raw,
                    "id": effective_id,
                    "refresh_group_id": refresh_id,
                    "browser_id": browser_id,
                    "stagger_index": index,
                    "stagger_count": len(expanded_ids),
                    "publisher_id": publisher_id,
                    "url": url,
                    "landing_url_hint": landing_url_hint,
                    "resource_kind": resource_kind,
                    "interval_seconds": interval_seconds,
                    "retry_seconds": retry_seconds,
                    "timeout_seconds": max(
                        45,
                        min(int(raw.get("timeout_seconds") or 180), 900),
                    ),
                    "wait_ms": max(
                        0,
                        min(int(raw.get("wait_ms") or 15000), 20000),
                    ),
                    "max_chars": max(1000, int(raw.get("max_chars") or 1000)),
                    "minimum_content_chars": max(
                        0,
                        int(raw.get("minimum_content_chars") or 1000),
                    ),
                }
            )
    lease_hours = max(0.25, float(config.get("lease_hours") or 3))
    warning_minutes = max(
        0,
        min(
            int(config.get("cycle_warning_minutes") or 15),
            int(lease_hours * 60),
        ),
    )
    probe_set_hash = hashlib.sha256(
        "\n".join(
            f"{probe['id']}:{probe['resource_kind']}:{probe['url']}:"
            f"{probe.get('landing_url_hint', '')}:"
            f"{','.join(probe['browser_ids'])}"
            for probe in normalized
        ).encode("utf-8")
    ).hexdigest()
    return {
        **config,
        "probes": normalized,
        "session_refreshes": normalized_refreshes,
        "browser_ids": configured_browser_ids(config.get("browser_ids", "")),
        "lane_count": max(1, int(config.get("lane_count") or 2)),
        "lease_hours": lease_hours,
        "cycle_deadline_hours": max(
            0.25,
            float(config.get("cycle_deadline_hours") or lease_hours),
        ),
        "cycle_warning_minutes": warning_minutes,
        "profile_repair_workers": max(
            1, int(config.get("profile_repair_workers") or 10)
        ),
        "publisher_maintainers": max(
            1, int(config.get("publisher_maintainers") or 2)
        ),
        "verification_slots": max(1, int(config.get("verification_slots") or 2)),
        "probe_set_hash": probe_set_hash,
    }


def ensure_probe_set(state: dict[str, Any], probe_set_hash: str) -> bool:
    previous = str(state.get("probe_set_hash") or "")
    if previous == probe_set_hash:
        return False
    for profile in state.get("profiles", {}).values():
        if not isinstance(profile, dict):
            continue
        for key in (
            "status",
            "checked_at",
            "last_result",
            "completed_checks",
            "consecutive_failures",
            "lease_expires_at",
            "next_due_at",
            "probe_id",
            "probe_resource_kind",
            "claim_expires_at",
        ):
            profile.pop(key, None)
    state["lanes"] = {}
    state["probe_set_hash"] = probe_set_hash
    state["probe_set_changed_at"] = iso(utc_now())
    return True


def select_probe(
    probes: list[dict[str, Any]],
    profile: dict[str, Any],
    browser_id: str,
) -> dict[str, Any]:
    eligible = [
        probe
        for probe in probes
        if not probe.get("browser_ids") or browser_id in probe["browser_ids"]
    ]
    if not eligible:
        raise ProfilePatrolError(
            f"no profile patrol probe is assigned to {browser_id}"
        )
    completed = int(profile.get("completed_checks") or 0)
    offset = browser_number(browser_id) - 1
    return eligible[(completed + offset) % len(eligible)]


def probe_passed(probe: dict[str, Any], result: dict[str, Any]) -> bool:
    if int(result.get("http_status") or 0) != 200:
        return False
    body = result.get("body")
    if not isinstance(body, dict) or body.get("success") is not True:
        return False
    summary = body.get("probe")
    if not isinstance(summary, dict) or summary.get("success") is not True:
        return False
    if probe["resource_kind"] == "supplementary_pdf":
        return bool(summary.get("download_success")) and int(
            summary.get("download_bytes") or 0
        ) >= int(probe["minimum_download_bytes"])
    return (
        str(summary.get("access_state") or "") in ACCEPTED_ACCESS_STATES
        and int(summary.get("content_chars") or 0)
        >= int(probe["minimum_content_chars"])
    )


def sanitized_probe_result(result: dict[str, Any]) -> dict[str, Any]:
    body = result.get("body")
    summary = body.get("probe") if isinstance(body, dict) else None
    raw_message = ""
    if isinstance(body, dict):
        raw_message = str(body.get("message") or body.get("error") or "")
    if not raw_message:
        raw_message = str(result.get("error_message") or "")
    sanitized_message = re.sub(
        r"(?i)(authorization|cookie|token|api[_-]?key)\s*[:=]\s*[^\s,;]+",
        r"\1=[redacted]",
        raw_message,
    )
    sanitized_message = re.sub(r"([?&][^=\s]+)=([^&\s]+)", r"\1=[redacted]", sanitized_message)
    return {
        "http_status": int(result.get("http_status") or 0),
        "error_type": result.get("error_type"),
        "error_message": sanitized_message[:500] or None,
        "probe": summary if isinstance(summary, dict) else None,
    }


def session_refresh_passed(
    refresh: dict[str, Any],
    result: dict[str, Any],
) -> bool:
    if int(result.get("http_status") or 0) != 200:
        return False
    body = result.get("body")
    if not isinstance(body, dict) or body.get("success") is not True:
        return False
    summary = body.get("probe")
    if isinstance(summary, dict):
        return (
            summary.get("success") is True
            and str(summary.get("access_state") or "") in ACCEPTED_ACCESS_STATES
            and str(summary.get("page_state") or "") != "challenge"
            and int(summary.get("content_chars") or 0)
            >= int(refresh.get("minimum_content_chars") or 0)
        )
    return (
        str(body.get("access_state") or "") in ACCEPTED_ACCESS_STATES
        and str(body.get("page_state") or "") != "challenge"
        and len(str(body.get("text") or ""))
        >= int(refresh.get("minimum_content_chars") or 0)
    )


def sanitized_session_refresh_result(result: dict[str, Any]) -> dict[str, Any]:
    body = result.get("body")
    if not isinstance(body, dict):
        body = {}
    summary = body.get("probe")
    if isinstance(summary, dict):
        return {
            "http_status": int(result.get("http_status") or 0),
            "error_type": result.get("error_type"),
            "success": summary.get("success") is True,
            "access_state": str(summary.get("access_state") or ""),
            "page_state": str(summary.get("page_state") or ""),
            "text_source": str(summary.get("text_source") or ""),
            "content_chars": int(summary.get("content_chars") or 0),
            "pdf_success": summary.get("download_success"),
            "pdf_pages": summary.get("pdf_pages"),
        }
    extraction = body.get("pdf_extraction")
    if not isinstance(extraction, dict):
        extraction = {}
    return {
        "http_status": int(result.get("http_status") or 0),
        "error_type": result.get("error_type"),
        "success": body.get("success") is True,
        "access_state": str(body.get("access_state") or ""),
        "page_state": str(body.get("page_state") or ""),
        "text_source": str(body.get("text_source") or ""),
        "content_chars": len(str(body.get("text") or "")),
        "pdf_success": extraction.get("success"),
        "pdf_pages": extraction.get("pages"),
    }


def session_refresh_due_at(entry: dict[str, Any]) -> datetime:
    return parse_time(entry.get("next_due_at")) or datetime.min.replace(
        tzinfo=timezone.utc
    )


def ensure_session_refresh_schedule(
    state: dict[str, Any],
    refreshes: list[dict[str, Any]],
    *,
    now: datetime,
) -> None:
    records = state.setdefault("session_refreshes", {})
    for refresh in refreshes:
        record = records.setdefault(refresh["id"], {})
        if record.get("next_due_at") or record.get("checked_at"):
            continue
        stagger_count = max(1, int(refresh.get("stagger_count") or 1))
        stagger_index = max(0, int(refresh.get("stagger_index") or 0))
        offset = int(int(refresh["interval_seconds"]) * stagger_index / stagger_count)
        record.update(
            {
                "status": "never_checked",
                "publisher_id": refresh["publisher_id"],
                "browser_id": refresh.get("browser_id") or None,
                "next_due_at": iso(now + timedelta(seconds=offset)),
            }
        )


def claim_session_refresh(
    state: dict[str, Any],
    refreshes: list[dict[str, Any]],
    *,
    now: datetime,
    claim_seconds: int,
) -> dict[str, Any] | None:
    if not refreshes:
        return None
    records = state.setdefault("session_refreshes", {})
    ensure_session_refresh_schedule(state, refreshes, now=now)
    candidates = []
    for refresh in refreshes:
        record = records.setdefault(refresh["id"], {})
        claim_until = parse_time(record.get("claim_expires_at"))
        if claim_until is not None and claim_until > now:
            continue
        due = session_refresh_due_at(record)
        if due <= now:
            candidates.append((due, refresh["id"], refresh))
    if not candidates:
        return None
    refresh = min(candidates)[2]
    record = records.setdefault(refresh["id"], {})
    record.update(
        {
            "status": "checking",
            "publisher_id": refresh["publisher_id"],
            "browser_id": refresh.get("browser_id") or None,
            "claimed_at": iso(now),
            "claim_expires_at": iso(now + timedelta(seconds=claim_seconds)),
            "attempts": int(record.get("attempts") or 0) + 1,
        }
    )
    return refresh


def finish_session_refresh(
    state: dict[str, Any],
    refresh: dict[str, Any],
    result: dict[str, Any],
    *,
    now: datetime,
) -> str:
    passed = session_refresh_passed(refresh, result)
    deferred = int(result.get("http_status") or 0) == 429
    status = "healthy" if passed else "deferred" if deferred else "degraded"
    record = state.setdefault("session_refreshes", {}).setdefault(
        refresh["id"],
        {},
    )
    previous_failures = int(record.get("consecutive_failures") or 0)
    retry_seconds = int(refresh["retry_seconds"])
    if passed:
        interval = int(refresh["interval_seconds"])
    elif deferred:
        interval = max(60, min(300, retry_seconds))
    else:
        interval = min(
            3600,
            retry_seconds * (2 ** min(previous_failures, 2)),
        )
    consecutive_failures = 0 if passed else previous_failures + 1
    record.update(
        {
            "status": status,
            "publisher_id": refresh["publisher_id"],
            "browser_id": refresh.get("browser_id") or None,
            "checked_at": iso(now),
            "claim_expires_at": None,
            "last_result": sanitized_session_refresh_result(result),
            "consecutive_failures": consecutive_failures,
            "next_due_at": iso(now + timedelta(seconds=interval)),
        }
    )
    return status


def due_at(profile: dict[str, Any]) -> datetime:
    return parse_time(profile.get("next_due_at")) or datetime.min.replace(
        tzinfo=timezone.utc
    )


def configured_pool_browser_ids(
    snapshot: dict[str, Any],
    config: dict[str, Any],
    *,
    states: set[str] | None = None,
) -> list[str]:
    browser_ids = browser_ids_from_pool(snapshot, states=states)
    configured_ids = set(config.get("browser_ids") or [])
    if configured_ids:
        browser_ids = [
            browser_id for browser_id in browser_ids if browser_id in configured_ids
        ]
    return browser_ids


def lane_session_refreshes(
    refreshes: list[dict[str, Any]],
    *,
    lane: int,
    lane_count: int,
) -> list[dict[str, Any]]:
    selected = []
    for refresh in refreshes:
        browser_id = str(refresh.get("browser_id") or "").strip()
        if browser_id:
            if lane_for_browser(browser_id, lane_count) == lane:
                selected.append(refresh)
        elif lane == 1:
            selected.append(refresh)
    return selected


def lane_has_due_profile(
    state: dict[str, Any],
    browser_ids: list[str],
    *,
    lane: int,
    lane_count: int,
    now: datetime,
) -> bool:
    profiles = state.setdefault("profiles", {})
    for browser_id in lane_browser_ids(browser_ids, lane, lane_count):
        profile = profiles.setdefault(browser_id, {})
        claim_until = parse_time(profile.get("claim_expires_at"))
        if claim_until is not None and claim_until > now:
            continue
        if due_at(profile) <= now:
            return True
    return False


def claim_profile(
    state: dict[str, Any],
    browser_ids: list[str],
    probes: list[dict[str, Any]],
    *,
    lane: int,
    lane_count: int,
    now: datetime,
    claim_seconds: int,
    cycle_deadline_seconds: int,
) -> tuple[str, dict[str, Any]] | None:
    profiles = state.setdefault("profiles", {})
    lane_state = state.setdefault("lanes", {}).setdefault(str(lane), {})
    cycle_profiles = set(lane_state.get("cycle_profiles") or [])
    candidates = []
    for browser_id in lane_browser_ids(browser_ids, lane, lane_count):
        profile = profiles.setdefault(browser_id, {})
        claim_until = parse_time(profile.get("claim_expires_at"))
        if claim_until is not None and claim_until > now:
            continue
        if due_at(profile) <= now:
            # A failed or busy profile becomes due again quickly. Finish covering
            # the rest of the lane before retrying it, otherwise one bad publisher
            # can prevent a three-hour cycle from ever completing.
            candidates.append(
                (
                    browser_id in cycle_profiles,
                    due_at(profile),
                    browser_number(browser_id),
                    browser_id,
                )
            )
    if not candidates:
        return None
    browser_id = min(candidates)[3]
    profile = profiles[browser_id]
    probe = select_probe(probes, profile, browser_id)
    profile.update(
        {
            "status": "checking",
            "lane": lane,
            "claimed_at": iso(now),
            "claim_expires_at": iso(now + timedelta(seconds=claim_seconds)),
            "probe_id": probe["id"],
            "probe_resource_kind": probe["resource_kind"],
            "attempts": int(profile.get("attempts") or 0) + 1,
        }
    )
    if not lane_state.get("cycle_started_at"):
        lane_state["cycle_started_at"] = iso(now)
        lane_state["cycle_deadline_at"] = iso(
            now + timedelta(seconds=cycle_deadline_seconds)
        )
        lane_state["cycle_profiles"] = []
        lane_state["cycle_degraded_profiles"] = []
    lane_state.update(
        {
            "status": "checking",
            "current_browser_id": browser_id,
            "last_claimed_at": iso(now),
        }
    )
    return browser_id, probe


def finish_profile(
    state: dict[str, Any],
    browser_ids: list[str],
    browser_id: str,
    probe: dict[str, Any],
    result: dict[str, Any],
    *,
    lane: int,
    lane_count: int,
    now: datetime,
    lease_seconds: int,
    cycle_deadline_seconds: int,
    retry_seconds: int,
) -> bool:
    passed = probe_passed(probe, result)
    profile = state.setdefault("profiles", {}).setdefault(browser_id, {})
    profile.update(
        {
            "status": "healthy" if passed else "degraded",
            "checked_at": iso(now),
            "claim_expires_at": None,
            "last_result": sanitized_probe_result(result),
            "completed_checks": int(profile.get("completed_checks") or 0) + 1,
            "consecutive_failures": (
                0 if passed else int(profile.get("consecutive_failures") or 0) + 1
            ),
            "lease_expires_at": iso(now + timedelta(seconds=lease_seconds))
            if passed
            else None,
            "next_due_at": iso(
                now + timedelta(seconds=lease_seconds if passed else retry_seconds)
            ),
        }
    )
    lane_state = state.setdefault("lanes", {}).setdefault(str(lane), {})
    cycle_profiles = lane_state.setdefault("cycle_profiles", [])
    if browser_id not in cycle_profiles:
        cycle_profiles.append(browser_id)
    degraded_profiles = lane_state.setdefault("cycle_degraded_profiles", [])
    if not passed and browser_id not in degraded_profiles:
        degraded_profiles.append(browser_id)
    lane_state.update(
        {
            "status": "idle",
            "current_browser_id": None,
            "last_finished_at": iso(now),
            "last_result": "healthy" if passed else "degraded",
            "last_work_kind": "profile_probe",
            "checks_completed": int(lane_state.get("checks_completed") or 0) + 1,
        }
    )
    owned = lane_browser_ids(browser_ids, lane, lane_count)
    if owned and set(owned).issubset(set(cycle_profiles)):
        started = parse_time(lane_state.get("cycle_started_at")) or now
        duration = max(0.0, (now - started).total_seconds())
        lane_state.update(
            {
                "last_cycle_started_at": iso(started),
                "last_cycle_finished_at": iso(now),
                "last_cycle_duration_seconds": round(duration, 3),
                "last_cycle_deadline_met": duration <= cycle_deadline_seconds,
                "last_cycle_status": (
                    "completed_degraded"
                    if degraded_profiles
                    else "completed_healthy"
                ),
                "last_cycle_profile_count": len(cycle_profiles),
                "last_cycle_degraded_count": len(degraded_profiles),
                "cycle_number": int(lane_state.get("cycle_number") or 0) + 1,
                "cycle_started_at": None,
                "cycle_deadline_at": None,
                "cycle_profiles": [],
                "cycle_degraded_profiles": [],
            }
        )
    return passed


def defer_busy_profile(
    state: dict[str, Any],
    browser_id: str,
    *,
    lane: int,
    now: datetime,
    retry_seconds: int = 60,
) -> None:
    profile = state.setdefault("profiles", {}).setdefault(browser_id, {})
    previous_status = str(profile.get("status") or "never_checked")
    profile.update(
        {
            "status": previous_status if previous_status != "checking" else "deferred",
            "claim_expires_at": None,
            "last_deferred_at": iso(now),
            "last_deferred_reason": "busy_with_user_work",
            "next_due_at": iso(now + timedelta(seconds=retry_seconds)),
        }
    )
    lane_state = state.setdefault("lanes", {}).setdefault(str(lane), {})
    lane_state.update(
        {
            "status": "idle",
            "current_browser_id": None,
            "last_finished_at": iso(now),
            "last_result": "skipped_busy",
            "last_work_kind": "profile_probe",
        }
    )


def aggregate_state(
    state: dict[str, Any],
    browser_ids: list[str],
    *,
    now: datetime,
    lane_count: int,
    lease_seconds: int,
    cycle_deadline_seconds: int | None = None,
    cycle_warning_seconds: int = 900,
) -> dict[str, Any]:
    profiles = state.get("profiles") if isinstance(state.get("profiles"), dict) else {}
    counts = {
        "healthy": 0,
        "degraded": 0,
        "checking": 0,
        "due": 0,
        "never_checked": 0,
        "needs_debug": 0,
    }
    for browser_id in browser_ids:
        profile = profiles.get(browser_id, {})
        status = str(profile.get("status") or "")
        needs_debug = profile.get("needs_debug") is True
        if status in counts and not (status == "healthy" and needs_debug):
            counts[status] += 1
        if not profile.get("checked_at"):
            counts["never_checked"] += 1
        if needs_debug:
            counts["needs_debug"] += 1
        if due_at(profile) <= now:
            counts["due"] += 1
    failure_evidence = []
    for browser_id in browser_ids:
        profile = profiles.get(browser_id, {})
        status = str(profile.get("status") or "")
        if status == "degraded" or profile.get("needs_debug") is True:
            failure_evidence.append(
                f"{browser_id}:{profile.get('probe_id') or 'runtime'}:{status}"
            )
    failure_fingerprint = (
        hashlib.sha256("\n".join(sorted(failure_evidence)).encode("utf-8")).hexdigest()
        if failure_evidence
        else None
    )
    deadline_seconds = cycle_deadline_seconds or lease_seconds
    lanes = state.get("lanes") if isinstance(state.get("lanes"), dict) else {}
    lane_summaries: dict[str, dict[str, Any]] = {}
    active_states: list[str] = []
    completed_states: list[str] = []
    for lane in range(1, lane_count + 1):
        lane_state = lanes.get(str(lane), {})
        started = parse_time(lane_state.get("cycle_started_at"))
        owned = lane_browser_ids(browser_ids, lane, lane_count)
        visited = set(lane_state.get("cycle_profiles") or [])
        if started is not None:
            age_seconds = max(0.0, (now - started).total_seconds())
            if age_seconds >= deadline_seconds:
                status = "cycle_overdue"
            elif age_seconds >= max(0, deadline_seconds - cycle_warning_seconds):
                status = "cycle_late"
            else:
                status = "running"
            active_states.append(status)
        else:
            age_seconds = None
            status = str(lane_state.get("last_cycle_status") or "initializing")
            completed_states.append(status)
        lane_summaries[str(lane)] = {
            "status": status,
            "cycle_number": int(lane_state.get("cycle_number") or 0),
            "cycle_started_at": lane_state.get("cycle_started_at"),
            "cycle_deadline_at": lane_state.get("cycle_deadline_at"),
            "cycle_age_seconds": (
                round(age_seconds, 3) if age_seconds is not None else None
            ),
            "checked_this_cycle": len(set(owned).intersection(visited)),
            "owned_profiles": len(owned),
            "remaining_this_cycle": max(
                0, len(owned) - len(set(owned).intersection(visited))
            ),
            "last_cycle_finished_at": lane_state.get("last_cycle_finished_at"),
            "last_cycle_duration_seconds": lane_state.get(
                "last_cycle_duration_seconds"
            ),
            "last_cycle_deadline_met": lane_state.get(
                "last_cycle_deadline_met"
            ),
            "last_cycle_degraded_count": int(
                lane_state.get("last_cycle_degraded_count") or 0
            ),
        }
    if "cycle_overdue" in active_states:
        cycle_status = "cycle_overdue"
    elif "cycle_late" in active_states:
        cycle_status = "cycle_late"
    elif active_states:
        cycle_status = "running"
    elif "completed_degraded" in completed_states:
        cycle_status = "completed_degraded"
    elif completed_states and all(
        value == "completed_healthy" for value in completed_states
    ):
        cycle_status = "completed_healthy"
    else:
        cycle_status = "initializing"
    return {
        "total_profiles": len(browser_ids),
        **counts,
        "lane_count": lane_count,
        "lease_seconds": lease_seconds,
        "cycle_deadline_seconds": deadline_seconds,
        "cycle_warning_seconds": cycle_warning_seconds,
        "cycle_status": cycle_status,
        "lane_cycles": lane_summaries,
        "failure_fingerprint": failure_fingerprint,
        "healthy_lease_coverage": (
            round(counts["healthy"] / len(browser_ids), 4) if browser_ids else 0.0
        ),
    }


def aggregate_session_refreshes(
    state: dict[str, Any],
    refreshes: list[dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any]:
    records = (
        state.get("session_refreshes")
        if isinstance(state.get("session_refreshes"), dict)
        else {}
    )
    counts = {
        "healthy": 0,
        "degraded": 0,
        "checking": 0,
        "deferred": 0,
        "due": 0,
        "never_checked": 0,
    }
    items: dict[str, Any] = {}
    for refresh in refreshes:
        refresh_id = str(refresh["id"])
        record = records.get(refresh_id, {})
        status = str(record.get("status") or "never_checked")
        if status in counts and status != "never_checked":
            counts[status] += 1
        if not record.get("checked_at"):
            counts["never_checked"] += 1
        if session_refresh_due_at(record) <= now:
            counts["due"] += 1
        items[refresh_id] = {
            "publisher_id": refresh["publisher_id"],
            "refresh_group_id": refresh.get("refresh_group_id"),
            "browser_id": refresh.get("browser_id") or record.get("browser_id"),
            "status": status,
            "checked_at": record.get("checked_at"),
            "next_due_at": record.get("next_due_at"),
            "consecutive_failures": int(record.get("consecutive_failures") or 0),
            "last_result": record.get("last_result"),
        }
    return {
        "configured": len(refreshes),
        **counts,
        "items": items,
    }


def merged_snapshot(
    state: dict[str, Any],
    browser_ids: list[str],
    *,
    now: datetime,
    lane_count: int,
    lease_seconds: int,
    cycle_deadline_seconds: int | None = None,
    cycle_warning_seconds: int = 900,
    profile_repair_workers: int = 10,
    publisher_maintainers: int = 2,
    verification_slots: int = 2,
    publisher_report: dict[str, Any] | None = None,
    maintenance_report: dict[str, Any] | None = None,
    profile_repair_report: dict[str, Any] | None = None,
    session_refreshes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    publisher_report = publisher_report or {}
    maintenance_report = maintenance_report or {}
    profile_repair_report = profile_repair_report or {}
    session_refreshes = session_refreshes or []
    return {
        "schema": MERGED_SCHEMA,
        "updated_at": iso(now),
        "profile_patrol": {
            "aggregate": aggregate_state(
                state,
                browser_ids,
                now=now,
                lane_count=lane_count,
                lease_seconds=lease_seconds,
                cycle_deadline_seconds=cycle_deadline_seconds,
                cycle_warning_seconds=cycle_warning_seconds,
            ),
            "lanes": state.get("lanes", {}),
        },
        "session_refresh": aggregate_session_refreshes(
            state,
            session_refreshes,
            now=now,
        ),
        "maintenance_topology": {
            "patrol_lanes": lane_count,
            "profile_repair_workers": profile_repair_workers,
            "publisher_maintainers": publisher_maintainers,
            "verification_slots": verification_slots,
            "deployment_concurrency": 1,
        },
        "profile_repair": {
            "observed_at": profile_repair_report.get("observed_at"),
            "repair_workers": profile_repair_report.get("repair_workers"),
            "verification_slots": profile_repair_report.get("verification_slots"),
            "candidate_count": profile_repair_report.get("candidate_count"),
            "repair_count": len(profile_repair_report.get("repairs") or []),
        },
        "publisher_access": {
            "status": publisher_report.get("overall_status"),
            "checked_at": publisher_report.get("checked_at"),
            "healthy_count": publisher_report.get("healthy_count"),
            "probe_count": publisher_report.get("probe_count"),
        },
        "code_maintenance": {
            "status": maintenance_report.get("status"),
            "started_at": maintenance_report.get("started_at"),
            "finished_at": maintenance_report.get("finished_at"),
            "repair_group_count": maintenance_report.get("repair_group_count"),
        },
    }


def run_probe(
    browser_id: str,
    probe: dict[str, Any],
    *,
    timeout_seconds: int,
    wait_ms: int,
    operation_kind: str = "checking",
) -> dict[str, Any]:
    payload = {
        "browser_id": browser_id,
        "url": probe["url"],
        "landing_url_hint": str(probe.get("landing_url_hint") or ""),
        "wait_ms": wait_ms,
        "max_chars": 0,
        "timeout_seconds": timeout_seconds,
        "lease_timeout_seconds": 2,
        "affinity_key": probe.get("publisher_id", ""),
        "resource_kind": probe["resource_kind"],
        "operation_kind": operation_kind,
    }
    try:
        return pool_request(
            "/v1/admin/browser-probe",
            payload,
            timeout=timeout_seconds,
        )
    except Exception as exc:
        return {
            "http_status": 0,
            "body": {"success": False},
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }


def run_session_refresh(
    refresh: dict[str, Any],
) -> dict[str, Any]:
    timeout_seconds = int(refresh["timeout_seconds"])
    browser_id = str(refresh.get("browser_id") or "").strip()
    payload = {
        "url": refresh["url"],
        "landing_url_hint": str(refresh.get("landing_url_hint") or ""),
        "wait_ms": int(refresh["wait_ms"]),
        "max_chars": int(refresh["max_chars"]),
        "timeout_seconds": timeout_seconds,
        "affinity_key": refresh["publisher_id"],
        "resource_kind": refresh["resource_kind"],
        "maintenance_session_refresh": refresh["publisher_id"] == "rsc",
    }
    path = "/v1/literature/read"
    if browser_id:
        path = "/v1/admin/browser-probe"
        payload.update(
            {
                "browser_id": browser_id,
                "lease_timeout_seconds": 2,
            }
        )
    try:
        return pool_request(
            path,
            payload,
            timeout=timeout_seconds,
        )
    except Exception as exc:
        return {
            "http_status": 0,
            "body": {"success": False},
            "error_type": type(exc).__name__,
        }


def run_lane_once(
    state_path: Path,
    merged_path: Path,
    config: dict[str, Any],
    *,
    lane: int,
    lane_count: int,
    requester: Callable[[], dict[str, Any]] = lambda: pool_request("/ready"),
    prober: Callable[..., dict[str, Any]] = run_probe,
    refresher: Callable[..., dict[str, Any]] = run_session_refresh,
    now: datetime | None = None,
) -> dict[str, Any]:
    started = now or utc_now()
    snapshot = requester()
    browser_ids = configured_pool_browser_ids(snapshot, config)
    if not browser_ids:
        raise ProfilePatrolError("browser pool reported no configured patrol profiles")
    available_ids = configured_pool_browser_ids(
        snapshot,
        config,
        states={"available"},
    )
    probes = list(config["probes"])
    timeout_seconds = int(config.get("timeout_seconds") or 240)
    lease_seconds = int(float(config.get("lease_hours") or 3) * 3600)
    cycle_deadline_seconds = int(
        float(config.get("cycle_deadline_hours") or config.get("lease_hours") or 3)
        * 3600
    )
    cycle_warning_seconds = int(config.get("cycle_warning_minutes") or 15) * 60
    retry_seconds = int(config.get("retry_seconds") or 300)
    session_refreshes = list(config.get("session_refreshes") or [])
    lane_refreshes = lane_session_refreshes(
        session_refreshes,
        lane=lane,
        lane_count=lane_count,
    )
    refresh_outcome: dict[str, Any] | None = None
    if lane_refreshes:
        with locked_state(state_path) as state:
            ensure_probe_set(state, str(config["probe_set_hash"]))
            sync_runtime_pool_state(state, snapshot, now=started)
            lane_state = state.setdefault("lanes", {}).setdefault(str(lane), {})
            profile_due = lane_has_due_profile(
                state,
                available_ids,
                lane=lane,
                lane_count=lane_count,
                now=started,
            )
            allow_refresh = (
                lane_state.get("last_work_kind") != "session_refresh"
                or not profile_due
            )
            claimed_refresh = (
                claim_session_refresh(
                    state,
                    lane_refreshes,
                    now=started,
                    claim_seconds=max(
                        int(refresh["timeout_seconds"])
                        for refresh in lane_refreshes
                    )
                    + 120,
                )
                if allow_refresh
                else None
            )
        if claimed_refresh is not None:
            refresh_result = refresher(claimed_refresh)
            refresh_finished = now or utc_now()
            with locked_state(state_path) as state:
                refresh_status = finish_session_refresh(
                    state,
                    claimed_refresh,
                    refresh_result,
                    now=refresh_finished,
                )
                state.setdefault("lanes", {}).setdefault(str(lane), {}).update(
                    {
                        "last_work_kind": "session_refresh",
                        "last_refresh_id": claimed_refresh["id"],
                        "last_refresh_finished_at": iso(refresh_finished),
                    }
                )
            refresh_outcome = {
                "id": claimed_refresh["id"],
                "publisher_id": claimed_refresh["publisher_id"],
                "browser_id": claimed_refresh.get("browser_id") or None,
                "status": refresh_status,
            }
            with locked_state(state_path) as state:
                merged = merged_snapshot(
                    state,
                    browser_ids,
                    now=refresh_finished,
                    lane_count=lane_count,
                    lease_seconds=lease_seconds,
                    cycle_deadline_seconds=cycle_deadline_seconds,
                    cycle_warning_seconds=cycle_warning_seconds,
                    profile_repair_workers=int(config["profile_repair_workers"]),
                    publisher_maintainers=int(config["publisher_maintainers"]),
                    verification_slots=int(config["verification_slots"]),
                    publisher_report=load_json(
                        Path(str(config.get("publisher_report_path") or "")), {}
                    ),
                    maintenance_report=load_json(
                        Path(str(config.get("maintenance_report_path") or "")), {}
                    ),
                    profile_repair_report=load_json(
                        Path(str(config.get("profile_repair_report_path") or "")), {}
                    ),
                    session_refreshes=session_refreshes,
                )
                save_json(merged_path, merged)
            return {
                "status": f"session_refresh_{refresh_status}",
                "browser_count": len(browser_ids),
                "session_refresh": refresh_outcome,
            }
    with locked_state(state_path) as state:
        ensure_probe_set(state, str(config["probe_set_hash"]))
        sync_runtime_pool_state(state, snapshot, now=started)
        claimed = claim_profile(
            state,
            available_ids,
            probes,
            lane=lane,
            lane_count=lane_count,
            now=started,
            claim_seconds=timeout_seconds + 120,
            cycle_deadline_seconds=cycle_deadline_seconds,
        )
    if claimed is None:
        with locked_state(state_path) as state:
            merged = merged_snapshot(
                state,
                browser_ids,
                now=started,
                lane_count=lane_count,
                lease_seconds=lease_seconds,
                cycle_deadline_seconds=cycle_deadline_seconds,
                cycle_warning_seconds=cycle_warning_seconds,
                profile_repair_workers=int(config["profile_repair_workers"]),
                publisher_maintainers=int(config["publisher_maintainers"]),
                verification_slots=int(config["verification_slots"]),
                publisher_report=load_json(
                    Path(str(config.get("publisher_report_path") or "")), {}
                ),
                maintenance_report=load_json(
                    Path(str(config.get("maintenance_report_path") or "")), {}
                ),
                profile_repair_report=load_json(
                    Path(str(config.get("profile_repair_report_path") or "")), {}
                ),
                session_refreshes=session_refreshes,
            )
            save_json(merged_path, merged)
        return {
            "status": "idle",
            "browser_count": len(browser_ids),
            "session_refresh": refresh_outcome,
        }
    browser_id, probe = claimed
    result = prober(
        browser_id,
        probe,
        timeout_seconds=timeout_seconds,
        wait_ms=int(config.get("wait_ms") or 5000),
    )
    finished = now or utc_now()
    with locked_state(state_path) as state:
        if int(result.get("http_status") or 0) == 429:
            defer_busy_profile(
                state,
                browser_id,
                lane=lane,
                now=finished,
            )
            outcome_status = "skipped_busy"
        else:
            passed = finish_profile(
                state,
                browser_ids,
                browser_id,
                probe,
                result,
                lane=lane,
                lane_count=lane_count,
                now=finished,
                lease_seconds=lease_seconds,
                cycle_deadline_seconds=cycle_deadline_seconds,
                retry_seconds=retry_seconds,
            )
            outcome_status = "healthy" if passed else "degraded"
        merged = merged_snapshot(
            state,
            browser_ids,
            now=finished,
            lane_count=lane_count,
            lease_seconds=lease_seconds,
            cycle_deadline_seconds=cycle_deadline_seconds,
            cycle_warning_seconds=cycle_warning_seconds,
            profile_repair_workers=int(config["profile_repair_workers"]),
            publisher_maintainers=int(config["publisher_maintainers"]),
            verification_slots=int(config["verification_slots"]),
            publisher_report=load_json(
                Path(str(config.get("publisher_report_path") or "")), {}
            ),
            maintenance_report=load_json(
                Path(str(config.get("maintenance_report_path") or "")), {}
            ),
            profile_repair_report=load_json(
                Path(str(config.get("profile_repair_report_path") or "")), {}
            ),
            session_refreshes=session_refreshes,
        )
        save_json(merged_path, merged)
    return {
        "status": outcome_status,
        "browser_id": browser_id,
        "probe_id": probe["id"],
        "browser_count": len(browser_ids),
        "session_refresh": refresh_outcome,
    }


def next_lane_due(
    state_path: Path,
    browser_ids: list[str],
    *,
    lane: int,
    lane_count: int,
    now: datetime,
    session_refreshes: list[dict[str, Any]] | None = None,
) -> datetime:
    with locked_state(state_path) as state:
        profiles = state.get("profiles", {})
        values = [
            max(
                due_at(profiles.get(browser_id, {})),
                parse_time(profiles.get(browser_id, {}).get("claim_expires_at"))
                or datetime.min.replace(tzinfo=timezone.utc),
            )
            for browser_id in lane_browser_ids(browser_ids, lane, lane_count)
        ]
        refreshes = session_refreshes or []
        ensure_session_refresh_schedule(state, refreshes, now=now)
        records = state.get("session_refreshes", {})
        values.extend(
            max(
                session_refresh_due_at(records.get(refresh["id"], {})),
                parse_time(records.get(refresh["id"], {}).get("claim_expires_at"))
                or datetime.min.replace(tzinfo=timezone.utc),
            )
            for refresh in refreshes
        )
    return min(values) if values else now + timedelta(seconds=60)


def bounded_daemon_delay(
    planned_delay: float,
    next_due: datetime,
    *,
    now: datetime,
    idle_seconds: int,
) -> float:
    until_due = (next_due - now).total_seconds()
    if until_due <= 0:
        return float(idle_seconds)
    return max(0.0, min(planned_delay, until_due))


def run_daemon(
    state_path: Path,
    merged_path: Path,
    config: dict[str, Any],
    *,
    lane: int,
    lane_count: int,
) -> None:
    lease_seconds = int(float(config.get("lease_hours") or 3) * 3600)
    idle_seconds = max(1, int(config.get("idle_poll_seconds") or 15))
    while True:
        started = time.monotonic()
        try:
            outcome = run_lane_once(
                state_path,
                merged_path,
                config,
                lane=lane,
                lane_count=lane_count,
            )
            snapshot = pool_request("/ready")
            browser_ids = configured_pool_browser_ids(snapshot, config)
            owned = lane_browser_ids(browser_ids, lane, lane_count)
            pace = lease_seconds / max(1, len(owned))
            lane_refreshes = lane_session_refreshes(
                list(config.get("session_refreshes") or []),
                lane=lane,
                lane_count=lane_count,
            )
            due = next_lane_due(
                state_path,
                browser_ids,
                lane=lane,
                lane_count=lane_count,
                now=utc_now(),
                session_refreshes=lane_refreshes,
            )
            outcome_status = str(outcome.get("status") or "")
            if outcome_status.startswith("session_refresh_"):
                delay = idle_seconds
            elif outcome_status == "idle":
                delay = bounded_daemon_delay(
                    float(idle_seconds),
                    due,
                    now=utc_now(),
                    idle_seconds=idle_seconds,
                )
            else:
                delay = bounded_daemon_delay(
                    max(0.0, pace - (time.monotonic() - started)),
                    due,
                    now=utc_now(),
                    idle_seconds=idle_seconds,
                )
        except Exception as exc:
            print(
                f"[profile-patrol] lane={lane} error={type(exc).__name__}",
                flush=True,
            )
            delay = idle_seconds
        time.sleep(delay)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--merged-state", type=Path, required=True)
    parser.add_argument("--lane", type=int, required=True)
    parser.add_argument("--lanes", type=int, default=2)
    parser.add_argument("--once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.lanes < 1 or args.lane < 1 or args.lane > args.lanes:
        raise ProfilePatrolError("lane must be between 1 and lanes")
    config = validate_config(load_json(args.config, {}))
    if int(config["lane_count"]) != args.lanes:
        raise ProfilePatrolError("configured lane_count must match --lanes")
    if config.get("enabled", True) is False:
        return 0
    if args.once:
        print(
            json.dumps(
                run_lane_once(
                    args.state,
                    args.merged_state,
                    config,
                    lane=args.lane,
                    lane_count=args.lanes,
                ),
                ensure_ascii=False,
            )
        )
        return 0
    run_daemon(
        args.state,
        args.merged_state,
        config,
        lane=args.lane,
        lane_count=args.lanes,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
