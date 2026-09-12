from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from typing import Any

from .host import (
    LiteratureMaintenanceError,
    browser_id_to_service,
    docker_container,
    fleet_mutation_lock,
    pool_request,
    pool_status,
    utc_now,
    wait_service_healthy,
    write_state,
)


STATE_SCHEMA = "scientist-literature-profile-repair/v1"
DEFAULT_RUNTIME_ROOT = Path(
    os.environ.get(
        "SCIENTIST_LARK_RUNTIME_ROOT",
        "/srv/hd-scientist/repos/Scientist/runtime/lark",
    )
)


def sanitized_error_detail(exc: Exception) -> str:
    raw = str(getattr(exc, "stderr", "") or getattr(exc, "stdout", "") or exc)
    raw = re.sub(
        r"(?i)(authorization|token|api[_-]?key|secret)(\s*[:=]\s*)\S+",
        r"\1\2<redacted>",
        raw,
    )
    return " ".join(raw.split())[-1000:]


def restart_browser_service(service: str) -> None:
    container = docker_container(service, include_stopped=True)
    if not container:
        raise LiteratureMaintenanceError(
            f"browser service container is missing: {service}"
        )
    subprocess.run(
        ["docker", "restart", "--time", "15", container],
        check=True,
        text=True,
        capture_output=True,
        timeout=90,
    )


def request_pool_repair(
    repo: Path,
    browser_id: str,
    *,
    restart_worker: bool = False,
) -> dict[str, Any]:
    value = pool_request(
        repo,
        "/v1/admin/browser-repair",
        {
            "browser_id": browser_id,
            "backend_only": True,
            "restart_worker": restart_worker,
        },
    )
    if value.get("success") is not True:
        raise LiteratureMaintenanceError(
            "literature browser pool rejected repair request"
        )
    return value


def wait_pool_profile_recovered(
    repo: Path,
    browser_id: str,
    *,
    timeout_seconds: int = 90,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(1, timeout_seconds)
    while time.monotonic() < deadline:
        pool = pool_status(repo).get("pool")
        browsers = pool.get("browsers") if isinstance(pool, dict) else None
        if isinstance(browsers, list):
            profile = next(
                (
                    item
                    for item in browsers
                    if isinstance(item, dict)
                    and str(item.get("browser_id") or "") == browser_id
                ),
                None,
            )
            if isinstance(profile, dict) and profile.get("state") == "available":
                return profile
        time.sleep(1)
    raise LiteratureMaintenanceError(
        f"profile did not pass backend re-admission: {browser_id}"
    )


def repair_candidates(
    pool: dict[str, Any],
    *,
    minimum_failures: int = 1,
) -> list[str]:
    candidates: list[str] = []
    for item in pool.get("browsers", []):
        if not isinstance(item, dict) or item.get("state") != "quarantined":
            continue
        if int(item.get("repair_failures") or 0) < minimum_failures:
            continue
        browser_id = str(item.get("browser_id") or "")
        if browser_id:
            candidates.append(browser_id)
    return candidates


def repair_browser_services(
    repo: Path,
    pool: dict[str, Any],
    *,
    limit: int = 10,
    verification_slots: int = 2,
    mutation_slots: int = 2,
    repair_mode: str | None = None,
) -> list[dict[str, Any]]:
    selected = repair_candidates(pool)[: max(0, limit)]
    if not selected:
        return []

    mode = str(
        repair_mode
        or os.environ.get("LITERATURE_PROFILE_REPAIR_MODE", "containers")
    ).strip().lower()
    if mode not in {"containers", "engine"}:
        raise LiteratureMaintenanceError(
            "LITERATURE_PROFILE_REPAIR_MODE must be containers or engine"
        )

    with fleet_mutation_lock():
        verification_gate = threading.BoundedSemaphore(
            max(1, verification_slots)
        )
        mutation_gate = threading.BoundedSemaphore(max(1, mutation_slots))

        def repair_one(browser_id: str) -> dict[str, Any]:
            service = (
                "literature-browser-engine"
                if mode == "engine"
                else browser_id_to_service(browser_id)
            )
            with mutation_gate:
                if mode == "engine":
                    request_pool_repair(
                        repo,
                        browser_id,
                        restart_worker=True,
                    )
                else:
                    restart_browser_service(service)
                    wait_service_healthy(repo, service)
            with verification_gate:
                if mode != "engine":
                    request_pool_repair(repo, browser_id)
                wait_pool_profile_recovered(repo, browser_id)
            return {
                "browser_id": browser_id,
                "service": service,
                "status": "recovered",
                "verification": (
                    "engine_worker_recycled_and_backend_verified"
                    if mode == "engine"
                    else "backend_verified"
                ),
            }

        by_browser: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=len(selected)) as executor:
            futures = {
                executor.submit(repair_one, browser_id): browser_id
                for browser_id in selected
            }
            for future in as_completed(futures):
                browser_id = futures[future]
                try:
                    by_browser[browser_id] = future.result()
                except Exception as exc:
                    by_browser[browser_id] = {
                        "browser_id": browser_id,
                        "service": (
                            "literature-browser-engine"
                            if mode == "engine"
                            else browser_id_to_service(browser_id)
                        ),
                        "status": "failed",
                        "error_type": type(exc).__name__,
                        "error_detail": sanitized_error_detail(exc),
                    }
    return [by_browser[browser_id] for browser_id in selected]


def profile_repair_cycle(
    repo: Path,
    state_path: Path,
    *,
    repair_workers: int = 10,
    verification_slots: int = 2,
    mutation_slots: int = 2,
) -> dict[str, Any]:
    status = pool_status(repo)
    pool = status.get("pool")
    if not isinstance(pool, dict):
        raise LiteratureMaintenanceError(
            "literature browser pool state is missing"
        )
    repairs = repair_browser_services(
        repo,
        pool,
        limit=max(1, repair_workers),
        verification_slots=max(1, verification_slots),
        mutation_slots=max(1, mutation_slots),
    )
    refreshed = pool_status(repo).get("pool") if repairs else pool
    if not isinstance(refreshed, dict):
        refreshed = pool
    result = {
        "schema": STATE_SCHEMA,
        "observed_at": utc_now(),
        "repair_workers": max(1, repair_workers),
        "verification_slots": max(1, verification_slots),
        "mutation_slots": max(1, mutation_slots),
        "candidate_count": len(repair_candidates(pool)),
        "repairs": repairs,
        "pool": refreshed,
    }
    write_state(state_path, result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Repair quarantined Scientist literature browser profiles"
    )
    parser.add_argument("--repo", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument(
        "--state",
        type=Path,
        default=Path("/var/lib/scientist-literature-profile-repair/state.json"),
    )
    parser.add_argument(
        "--repair-workers",
        type=int,
        default=int(os.environ.get("LITERATURE_PROFILE_REPAIR_WORKERS", "10")),
    )
    parser.add_argument(
        "--verification-slots",
        type=int,
        default=int(
            os.environ.get("LITERATURE_PROFILE_VERIFICATION_SLOTS", "2")
        ),
    )
    parser.add_argument(
        "--mutation-slots",
        type=int,
        default=int(os.environ.get("LITERATURE_PROFILE_MUTATION_SLOTS", "2")),
    )
    args = parser.parse_args(argv)
    result = profile_repair_cycle(
        args.repo.resolve(),
        args.state.resolve(),
        repair_workers=max(1, args.repair_workers),
        verification_slots=max(1, args.verification_slots),
        mutation_slots=max(1, args.mutation_slots),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
