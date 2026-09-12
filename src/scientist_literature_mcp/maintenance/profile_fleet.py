from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import sqlite3
import statistics
import threading
import time
from typing import Any, Iterator
import uuid

from .profile_patrol import (
    configured_browser_ids,
    configured_pool_browser_ids,
    load_json,
    pool_request,
    probe_passed,
    run_probe,
    sanitized_probe_result,
    validate_config as validate_patrol_config,
)


SCHEMA = "scientist_literature_profile_fleet/v1"
ACTIVE_REPLACEMENT_STATES = {"claimed", "warming", "committing", "rolling_back"}


class ProfileFleetError(RuntimeError):
    pass


def utc_epoch() -> float:
    return time.time()


def utc_iso(value: float | None = None) -> str:
    return datetime.fromtimestamp(
        utc_epoch() if value is None else value,
        timezone.utc,
    ).isoformat(timespec="seconds")


@dataclass(frozen=True)
class FleetConfig:
    browser_ids: tuple[str, ...]
    probes: tuple[dict[str, Any], ...]
    active_slots: int
    replacement_enabled: bool
    replacement_min_slots: int
    replacement_max_slots: int
    idle_check_seconds: int
    poll_seconds: int
    timeout_seconds: int
    wait_ms: int
    recoverable_failure_threshold: int
    improvement_margin: int
    publisher_limits: dict[str, int]


def load_fleet_config(path: Path) -> FleetConfig:
    raw = load_json(path, {})
    if not isinstance(raw, dict) or raw.get("schema") != (
        "scientist_literature_profile_fleet_config/v1"
    ):
        raise ProfileFleetError("invalid profile fleet config schema")
    probes_path = Path(str(raw.get("probes_path") or ""))
    if not probes_path.is_absolute():
        probes_path = (path.parent / probes_path).resolve()
    probe_document = load_json(probes_path, {})
    if not isinstance(probe_document, dict):
        raise ProfileFleetError("profile fleet probe document is invalid")
    patrol_shape = validate_patrol_config(
        {
            "probes": probe_document.get("probes"),
            "lease_hours": 0.5,
            "retry_seconds": raw.get("retry_seconds", 300),
        }
    )
    probes = []
    for probe in patrol_shape["probes"]:
        normalized = dict(probe)
        normalized["browser_ids"] = []
        probes.append(normalized)
    browser_ids = configured_browser_ids(raw.get("browser_ids"))
    if not browser_ids:
        raise ProfileFleetError("profile fleet requires managed browser_ids")
    active_slots = max(1, min(int(raw.get("active_slots") or 80), 150))
    replacement_min = max(
        1,
        min(int(raw.get("replacement_min_slots") or 2), active_slots),
    )
    replacement_max = max(
        replacement_min,
        min(int(raw.get("replacement_max_slots") or 8), active_slots),
    )
    publisher_limits = {
        str(key).strip().lower(): max(1, min(int(value), active_slots))
        for key, value in dict(raw.get("publisher_limits") or {}).items()
        if str(key).strip()
    }
    return FleetConfig(
        browser_ids=tuple(browser_ids),
        probes=tuple(probes),
        active_slots=active_slots,
        replacement_enabled=raw.get("replacement_enabled") is True,
        replacement_min_slots=replacement_min,
        replacement_max_slots=replacement_max,
        idle_check_seconds=max(300, int(raw.get("idle_check_seconds") or 1800)),
        poll_seconds=max(1, int(raw.get("poll_seconds") or 5)),
        timeout_seconds=max(45, min(int(raw.get("timeout_seconds") or 240), 900)),
        wait_ms=max(0, min(int(raw.get("wait_ms") or 5000), 20000)),
        recoverable_failure_threshold=max(
            2,
            int(raw.get("recoverable_failure_threshold") or 4),
        ),
        improvement_margin=max(1, int(raw.get("improvement_margin") or 1)),
        publisher_limits=publisher_limits,
    )


class FleetStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS profiles (
                    browser_id TEXT PRIMARY KEY,
                    generation_id TEXT NOT NULL,
                    initial_complete INTEGER NOT NULL DEFAULT 0,
                    next_probe_index INTEGER NOT NULL DEFAULT 0,
                    next_due_at REAL NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS evidence (
                    browser_id TEXT NOT NULL,
                    generation_id TEXT NOT NULL,
                    probe_id TEXT NOT NULL,
                    publisher_id TEXT NOT NULL,
                    resource_kind TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    observed_at REAL NOT NULL,
                    duration_seconds REAL,
                    summary_json TEXT NOT NULL,
                    PRIMARY KEY(browser_id, generation_id, probe_id)
                );
                CREATE TABLE IF NOT EXISTS replacements (
                    replacement_id TEXT PRIMARY KEY,
                    target_browser_id TEXT NOT NULL,
                    old_generation_id TEXT NOT NULL,
                    candidate_generation_id TEXT,
                    state TEXT NOT NULL,
                    old_score INTEGER,
                    candidate_score INTEGER,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    error_type TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_fleet_replacement
                ON replacements(target_browser_id)
                WHERE state IN ('claimed', 'warming', 'committing', 'rolling_back');
                """
            )

    def ensure_profiles(self, browser_ids: tuple[str, ...]) -> None:
        now = utc_epoch()
        with self.lock, self.connect() as connection:
            for browser_id in browser_ids:
                connection.execute(
                    "INSERT OR IGNORE INTO profiles(browser_id, generation_id, updated_at) "
                    "VALUES (?, ?, ?)",
                    (browser_id, f"current-{browser_id}", now),
                )

    def profile(self, browser_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM profiles WHERE browser_id=?",
                (browser_id,),
            ).fetchone()
        if row is None:
            raise ProfileFleetError(f"unknown fleet profile {browser_id}")
        return dict(row)

    def record(
        self,
        browser_id: str,
        generation_id: str,
        probe: dict[str, Any],
        result: dict[str, Any],
        *,
        source: str,
        duration_seconds: float,
    ) -> bool:
        passed = probe_passed(probe, result)
        now = utc_epoch()
        summary = sanitized_probe_result(result)
        with self.lock, self.connect() as connection:
            connection.execute(
                """
                INSERT INTO evidence(
                    browser_id, generation_id, probe_id, publisher_id,
                    resource_kind, success, source, observed_at,
                    duration_seconds, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(browser_id, generation_id, probe_id) DO UPDATE SET
                    publisher_id=excluded.publisher_id,
                    resource_kind=excluded.resource_kind,
                    success=excluded.success,
                    source=excluded.source,
                    observed_at=excluded.observed_at,
                    duration_seconds=excluded.duration_seconds,
                    summary_json=excluded.summary_json
                """,
                (
                    browser_id,
                    generation_id,
                    probe["id"],
                    str(probe.get("publisher_id") or ""),
                    probe["resource_kind"],
                    int(passed),
                    source,
                    now,
                    round(duration_seconds, 3),
                    json.dumps(summary, ensure_ascii=False, sort_keys=True),
                ),
            )
            connection.execute(
                "UPDATE profiles SET updated_at=? WHERE browser_id=?",
                (now, browser_id),
            )
        return passed

    def record_external_success(
        self,
        browser_id: str,
        generation_id: str,
        probe: dict[str, Any],
        observed_at: float,
    ) -> None:
        summary = {
            "http_status": 200,
            "probe": {
                "success": True,
                "evidence": "foreground publisher/resource success",
            },
        }
        with self.lock, self.connect() as connection:
            current = connection.execute(
                "SELECT observed_at FROM evidence WHERE browser_id=? AND "
                "generation_id=? AND probe_id=?",
                (browser_id, generation_id, probe["id"]),
            ).fetchone()
            if current is not None and float(current["observed_at"] or 0) >= observed_at:
                return
            connection.execute(
                """
                INSERT INTO evidence(
                    browser_id, generation_id, probe_id, publisher_id,
                    resource_kind, success, source, observed_at,
                    duration_seconds, summary_json
                ) VALUES (?, ?, ?, ?, ?, 1, 'external_service', ?, NULL, ?)
                ON CONFLICT(browser_id, generation_id, probe_id) DO UPDATE SET
                    success=1, source='external_service',
                    observed_at=excluded.observed_at,
                    duration_seconds=NULL, summary_json=excluded.summary_json
                """,
                (
                    browser_id,
                    generation_id,
                    probe["id"],
                    str(probe.get("publisher_id") or ""),
                    probe["resource_kind"],
                    observed_at,
                    json.dumps(summary, ensure_ascii=False, sort_keys=True),
                ),
            )

    def complete_initial(self, browser_id: str, generation_id: str) -> None:
        with self.lock, self.connect() as connection:
            connection.execute(
                "UPDATE profiles SET initial_complete=1, next_due_at=?, updated_at=? "
                "WHERE browser_id=? AND generation_id=?",
                (utc_epoch(), utc_epoch(), browser_id, generation_id),
            )

    def advance_idle(
        self,
        browser_id: str,
        generation_id: str,
        probe_count: int,
        interval_seconds: int,
    ) -> None:
        with self.lock, self.connect() as connection:
            row = connection.execute(
                "SELECT next_probe_index FROM profiles WHERE browser_id=?",
                (browser_id,),
            ).fetchone()
            next_index = (int(row["next_probe_index"] or 0) + 1) % probe_count
            now = utc_epoch()
            connection.execute(
                "UPDATE profiles SET next_probe_index=?, next_due_at=?, updated_at=? "
                "WHERE browser_id=? AND generation_id=?",
                (
                    next_index,
                    now + interval_seconds,
                    now,
                    browser_id,
                    generation_id,
                ),
            )

    def evidence(self, browser_id: str, generation_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM evidence WHERE browser_id=? AND generation_id=?",
                (browser_id, generation_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def census_complete(self, browser_ids: tuple[str, ...]) -> bool:
        placeholders = ",".join("?" for _ in browser_ids)
        with self.connect() as connection:
            value = connection.execute(
                f"SELECT COUNT(*) FROM profiles WHERE browser_id IN ({placeholders}) "
                "AND initial_complete=1",
                browser_ids,
            ).fetchone()[0]
        return int(value) == len(browser_ids)

    def claim_replacement(self, browser_id: str, old_score: int) -> str | None:
        replacement_id = f"replace-{browser_id}-{uuid.uuid4().hex}"
        profile = self.profile(browser_id)
        now = utc_epoch()
        with self.lock, self.connect() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO replacements(
                        replacement_id, target_browser_id, old_generation_id,
                        state, old_score, created_at, updated_at
                    ) VALUES (?, ?, ?, 'claimed', ?, ?, ?)
                    """,
                    (
                        replacement_id,
                        browser_id,
                        profile["generation_id"],
                        old_score,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                return None
        return replacement_id

    def update_replacement(
        self,
        replacement_id: str,
        state: str,
        *,
        old_generation_id: str | None = None,
        candidate_generation_id: str | None = None,
        candidate_score: int | None = None,
        error_type: str | None = None,
    ) -> None:
        with self.lock, self.connect() as connection:
            connection.execute(
                """
                UPDATE replacements SET state=?,
                    old_generation_id=COALESCE(?, old_generation_id),
                    candidate_generation_id=COALESCE(?, candidate_generation_id),
                    candidate_score=COALESCE(?, candidate_score),
                    error_type=?, updated_at=?
                WHERE replacement_id=?
                """,
                (
                    state,
                    old_generation_id,
                    candidate_generation_id,
                    candidate_score,
                    error_type,
                    utc_epoch(),
                    replacement_id,
                ),
            )

    def adopt_generation(self, browser_id: str, generation_id: str) -> None:
        with self.lock, self.connect() as connection:
            now = utc_epoch()
            connection.execute(
                "UPDATE profiles SET generation_id=?, initial_complete=1, "
                "next_probe_index=0, next_due_at=?, updated_at=? WHERE browser_id=?",
                (generation_id, now, now, browser_id),
            )

    def active_replacement_targets(self) -> set[str]:
        placeholders = ",".join("?" for _ in ACTIVE_REPLACEMENT_STATES)
        with self.connect() as connection:
            rows = connection.execute(
                f"SELECT target_browser_id FROM replacements WHERE state IN ({placeholders})",
                tuple(sorted(ACTIVE_REPLACEMENT_STATES)),
            ).fetchall()
        return {str(row[0]) for row in rows}

    def active_replacements(self) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in ACTIVE_REPLACEMENT_STATES)
        with self.connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM replacements WHERE state IN ({placeholders}) "
                "ORDER BY created_at",
                tuple(sorted(ACTIVE_REPLACEMENT_STATES)),
            ).fetchall()
        return [dict(row) for row in rows]

    def summary(self, browser_ids: tuple[str, ...], probe_count: int) -> dict[str, Any]:
        with self.connect() as connection:
            initial = connection.execute(
                "SELECT COUNT(*) FROM profiles WHERE initial_complete=1"
            ).fetchone()[0]
            evidence = connection.execute("SELECT COUNT(*) FROM evidence").fetchone()[0]
            replacements = {
                str(row[0]): int(row[1])
                for row in connection.execute(
                    "SELECT state, COUNT(*) FROM replacements GROUP BY state"
                ).fetchall()
            }
        return {
            "schema": SCHEMA,
            "updated_at": utc_iso(),
            "managed_profiles": len(browser_ids),
            "probe_count": probe_count,
            "initial_qualified": min(int(initial), len(browser_ids)),
            "evidence_records": int(evidence),
            "replacements": replacements,
        }


class ProfileFleet:
    def __init__(self, config: FleetConfig, store: FleetStore):
        self.config = config
        self.store = store
        self.store.ensure_profiles(config.browser_ids)
        self.executor = ThreadPoolExecutor(
            max_workers=config.active_slots,
            thread_name_prefix="literature-profile",
        )
        self.publisher_gates = {
            publisher: threading.BoundedSemaphore(limit)
            for publisher, limit in config.publisher_limits.items()
        }
        self.default_publisher_gate = threading.BoundedSemaphore(
            max(1, min(12, config.active_slots))
        )
        self.futures: dict[Future[Any], tuple[str, str]] = {}
        self.active_profiles: set[str] = set()
        self.lock = threading.RLock()

    def recover_replacements(self) -> None:
        """Resolve transactions interrupted by a previous fleet process."""
        for item in self.store.active_replacements():
            replacement_id = str(item["replacement_id"])
            browser_id = str(item["target_browser_id"])
            try:
                body = self.replacement_request(
                    {
                        "browser_id": browser_id,
                        "action": "rollback",
                        "replacement_id": replacement_id,
                    }
                )
            except Exception as exc:
                if str(item["state"]) == "claimed":
                    self.store.update_replacement(
                        replacement_id,
                        "cancelled",
                        error_type="InterruptedBeforeBegin",
                    )
                    continue
                self.store.update_replacement(
                    replacement_id,
                    "rolling_back",
                    error_type=type(exc).__name__,
                )
                continue
            replacement = body.get("replacement") if isinstance(body, dict) else None
            if not isinstance(replacement, dict):
                self.store.update_replacement(
                    replacement_id,
                    "rolling_back",
                    error_type="MissingRecoveryTransaction",
                )
                continue
            state = str(replacement.get("state") or "")
            if state == "committed":
                generation = str(
                    replacement.get("candidate_generation_id")
                    or item.get("candidate_generation_id")
                    or ""
                )
                if generation:
                    self.store.adopt_generation(browser_id, generation)
                self.store.update_replacement(replacement_id, "committed")
            elif state == "rolled_back":
                self.store.adopt_generation(
                    browser_id,
                    str(item["old_generation_id"]),
                )
                self.store.update_replacement(replacement_id, "rolled_back")
            else:
                self.store.update_replacement(
                    replacement_id,
                    "rolling_back",
                    error_type="IncompleteRecovery",
                )

    @contextmanager
    def publisher_slot(self, publisher_id: str) -> Iterator[None]:
        gate = self.publisher_gates.get(
            publisher_id.strip().lower(),
            self.default_publisher_gate,
        )
        gate.acquire()
        try:
            yield
        finally:
            gate.release()

    @staticmethod
    def replacement_request(payload: dict[str, Any]) -> dict[str, Any]:
        response = pool_request(
            "/v1/admin/browser-replacement",
            payload,
            timeout=180,
        )
        if "body" in response:
            status = int(response.get("http_status") or 0)
            body = response.get("body")
        else:
            status = 200
            body = response
        if status != 200 or not isinstance(body, dict) or body.get("success") is not True:
            raise ProfileFleetError(
                f"replacement request failed with HTTP {status or 'unknown'}"
            )
        return body

    def ordered_probes(self, browser_id: str, generation_id: str) -> list[dict[str, Any]]:
        probes = [dict(probe) for probe in self.config.probes]
        seed = int.from_bytes(
            hashlib.sha256(f"{browser_id}:{generation_id}".encode()).digest()[:8],
            "big",
        )
        random.Random(seed).shuffle(probes)
        return probes

    def probe(
        self,
        browser_id: str,
        generation_id: str,
        probe: dict[str, Any],
        *,
        operation_kind: str,
        source: str,
    ) -> bool | None:
        started = time.monotonic()
        with self.publisher_slot(str(probe.get("publisher_id") or "")):
            result = run_probe(
                browser_id,
                probe,
                timeout_seconds=self.config.timeout_seconds,
                wait_ms=self.config.wait_ms,
                operation_kind=operation_kind,
            )
        if int(result.get("http_status") or 0) == 429:
            return None
        return self.store.record(
            browser_id,
            generation_id,
            probe,
            result,
            source=source,
            duration_seconds=time.monotonic() - started,
        )

    def qualify_initial(self, browser_id: str) -> None:
        profile = self.store.profile(browser_id)
        generation = str(profile["generation_id"])
        existing = {
            row["probe_id"]
            for row in self.store.evidence(browser_id, generation)
        }
        complete = True
        for probe in self.ordered_probes(browser_id, generation):
            if probe["id"] in existing:
                continue
            outcome = self.probe(
                browser_id,
                generation,
                probe,
                operation_kind="checking",
                source="initial_qualification",
            )
            if outcome is None:
                complete = False
                break
        if complete and len(self.store.evidence(browser_id, generation)) >= len(
            self.config.probes
        ):
            self.store.complete_initial(browser_id, generation)

    def idle_check(self, browser_id: str) -> None:
        profile = self.store.profile(browser_id)
        generation = str(profile["generation_id"])
        index = int(profile["next_probe_index"] or 0) % len(self.config.probes)
        probe = self.ordered_probes(browser_id, generation)[index]
        recent = next(
            (
                row
                for row in self.store.evidence(browser_id, generation)
                if row["probe_id"] == probe["id"]
                and int(row["success"]) == 1
                and row["source"] == "external_service"
                and float(row["observed_at"] or 0)
                >= utc_epoch() - self.config.idle_check_seconds
            ),
            None,
        )
        if recent is not None:
            self.store.advance_idle(
                browser_id,
                generation,
                len(self.config.probes),
                self.config.idle_check_seconds,
            )
            return
        outcome = self.probe(
            browser_id,
            generation,
            probe,
            operation_kind="checking",
            source="idle_keep_warm",
        )
        if outcome is not None:
            self.store.advance_idle(
                browser_id,
                generation,
                len(self.config.probes),
                self.config.idle_check_seconds,
            )

    def replacement_scores(self) -> list[tuple[int, int, str]]:
        profiles = [self.store.profile(browser_id) for browser_id in self.config.browser_ids]
        success_by_probe: dict[str, int] = {}
        evidence_by_profile: dict[str, list[dict[str, Any]]] = {}
        for profile in profiles:
            rows = self.store.evidence(profile["browser_id"], profile["generation_id"])
            evidence_by_profile[profile["browser_id"]] = rows
            for row in rows:
                if int(row["success"]):
                    success_by_probe[row["probe_id"]] = success_by_probe.get(
                        row["probe_id"], 0
                    ) + 1
        scores = []
        for profile in profiles:
            rows = evidence_by_profile[profile["browser_id"]]
            passed = sum(int(row["success"]) for row in rows)
            recoverable_failures = sum(
                int(row["success"]) == 0
                and success_by_probe.get(row["probe_id"], 0) > 0
                for row in rows
            )
            scores.append((passed, recoverable_failures, profile["browser_id"]))
        return sorted(scores)

    @staticmethod
    def parse_observed_at(value: Any) -> float:
        text = str(value or "").strip()
        if not text:
            return 0.0
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return 0.0
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()

    def sync_external_evidence(self, snapshot: dict[str, Any]) -> None:
        pool = snapshot.get("pool") if isinstance(snapshot, dict) else None
        browsers = pool.get("browsers") if isinstance(pool, dict) else None
        if not isinstance(browsers, list):
            return
        probes_by_capability: dict[str, list[dict[str, Any]]] = {}
        for probe in self.config.probes:
            publisher = str(probe.get("publisher_id") or "")
            capability = (
                f"{publisher}-si"
                if probe["resource_kind"] == "supplementary_pdf"
                else publisher
            )
            probes_by_capability.setdefault(capability, []).append(probe)
        for browser in browsers:
            if not isinstance(browser, dict):
                continue
            browser_id = str(browser.get("browser_id") or "")
            if browser_id not in self.config.browser_ids:
                continue
            profile = self.store.profile(browser_id)
            if not int(profile["initial_complete"]):
                continue
            capabilities = browser.get("publisher_capabilities")
            if not isinstance(capabilities, dict):
                continue
            for capability, probes in probes_by_capability.items():
                evidence = capabilities.get(capability)
                if not isinstance(evidence, dict):
                    continue
                observed = self.parse_observed_at(evidence.get("last_success_at"))
                if observed <= 0:
                    continue
                for probe in probes:
                    self.store.record_external_success(
                        browser_id,
                        str(profile["generation_id"]),
                        probe,
                        observed,
                    )

    def replacement_candidates(self, limit: int) -> list[tuple[str, int]]:
        if not self.store.census_complete(self.config.browser_ids):
            return []
        scores = self.replacement_scores()
        if not scores:
            return []
        median = statistics.median(item[0] for item in scores)
        active = self.store.active_replacement_targets()
        with self.lock:
            active.update(self.active_profiles)
        candidates = []
        for passed, recoverable, browser_id in scores:
            if browser_id in active:
                continue
            if recoverable < self.config.recoverable_failure_threshold:
                continue
            if passed + self.config.improvement_margin > median:
                continue
            candidates.append((browser_id, passed))
            if len(candidates) >= limit:
                break
        return candidates

    def replacement(self, browser_id: str, replacement_id: str, old_score: int) -> None:
        profile = self.store.profile(browser_id)
        old_generation = str(profile["generation_id"])
        engine_old_generation = old_generation
        try:
            body = self.replacement_request(
                {
                    "browser_id": browser_id,
                    "action": "begin",
                    "replacement_id": replacement_id,
                    "expected_generation_id": "",
                }
            )
            replacement = body.get("replacement") if isinstance(body, dict) else None
            if not isinstance(replacement, dict):
                raise ProfileFleetError("replacement begin returned no transaction")
            candidate = str(replacement.get("candidate_generation_id") or "")
            engine_old_generation = str(
                replacement.get("expected_generation_id") or old_generation
            )
            if not candidate:
                raise ProfileFleetError("replacement begin returned no candidate")
            self.store.update_replacement(
                replacement_id,
                "warming",
                old_generation_id=engine_old_generation,
                candidate_generation_id=candidate,
            )
            passed = 0
            completed = 0
            for probe in self.ordered_probes(browser_id, candidate):
                outcome = self.probe(
                    browser_id,
                    candidate,
                    probe,
                    operation_kind="warming",
                    source="replacement_qualification",
                )
                if outcome is None:
                    raise ProfileFleetError("replacement qualification was deferred")
                completed += 1
                passed += int(outcome)
            commit = (
                completed == len(self.config.probes)
                and passed >= old_score + self.config.improvement_margin
            )
            self.store.update_replacement(
                replacement_id,
                "committing" if commit else "rolling_back",
                candidate_score=passed,
            )
            self.replacement_request(
                {
                    "browser_id": browser_id,
                    "action": "commit" if commit else "rollback",
                    "replacement_id": replacement_id,
                }
            )
            if commit:
                self.store.adopt_generation(browser_id, candidate)
            else:
                self.store.adopt_generation(browser_id, engine_old_generation)
            self.store.update_replacement(
                replacement_id,
                "committed" if commit else "rolled_back",
                candidate_score=passed,
            )
        except Exception as exc:
            self.store.update_replacement(
                replacement_id,
                "rolling_back",
                error_type=type(exc).__name__,
            )
            try:
                self.replacement_request(
                    {
                        "browser_id": browser_id,
                        "action": "rollback",
                        "replacement_id": replacement_id,
                    }
                )
                self.store.adopt_generation(browser_id, engine_old_generation)
                self.store.update_replacement(
                    replacement_id,
                    "rolled_back",
                    error_type=type(exc).__name__,
                )
            except Exception as rollback_exc:
                self.store.update_replacement(
                    replacement_id,
                    "rolling_back",
                    error_type=type(rollback_exc).__name__,
                )
            raise

    def reap(self) -> None:
        with self.lock:
            completed = [future for future in self.futures if future.done()]
            for future in completed:
                browser_id, _kind = self.futures.pop(future)
                self.active_profiles.discard(browser_id)
                try:
                    future.result()
                except Exception as exc:
                    print(
                        f"[profile-fleet] browser={browser_id} "
                        f"error={type(exc).__name__}",
                        flush=True,
                    )

    def submit(self, browser_id: str, kind: str, function: Any, *args: Any) -> bool:
        with self.lock:
            if browser_id in self.active_profiles or len(self.futures) >= self.config.active_slots:
                return False
            future = self.executor.submit(function, browser_id, *args)
            self.futures[future] = (browser_id, kind)
            self.active_profiles.add(browser_id)
            return True

    def tick(self) -> None:
        self.reap()
        snapshot = pool_request("/ready")
        self.sync_external_evidence(snapshot)
        runtime_ids = set(configured_pool_browser_ids(snapshot, {"browser_ids": list(self.config.browser_ids)}))
        managed = [browser_id for browser_id in self.config.browser_ids if browser_id in runtime_ids]
        if len(managed) != len(self.config.browser_ids):
            raise ProfileFleetError("managed profile set is not fully present in browser pool")
        pool = snapshot.get("pool") if isinstance(snapshot, dict) else {}
        foreground_active = int(
            dict(pool.get("active_by_operation") or {}).get("serving_external") or 0
        )
        capacity = max(0, self.config.active_slots - foreground_active)
        if capacity <= len(self.futures):
            return

        unqualified = []
        for browser_id in managed:
            profile = self.store.profile(browser_id)
            if not int(profile["initial_complete"]):
                unqualified.append(browser_id)
        if unqualified:
            for browser_id in unqualified:
                if len(self.futures) >= capacity:
                    break
                self.submit(browser_id, "initial", self.qualify_initial)
            return

        replacement_active = sum(
            kind == "replacement" for _browser_id, kind in self.futures.values()
        )
        replacement_capacity = (
            min(
                max(0, self.config.replacement_max_slots - replacement_active),
                max(0, capacity - len(self.futures)),
            )
            if self.config.replacement_enabled
            else 0
        )
        candidates = self.replacement_candidates(replacement_capacity)
        for browser_id, old_score in candidates:
            replacement_id = self.store.claim_replacement(browser_id, old_score)
            if replacement_id:
                submitted = self.submit(
                    browser_id,
                    "replacement",
                    self.replacement,
                    replacement_id,
                    old_score,
                )
                if not submitted:
                    self.store.update_replacement(replacement_id, "cancelled")

        replacement_backlog = bool(candidates)
        replacement_active = sum(
            kind == "replacement" for _browser_id, kind in self.futures.values()
        )
        reserved = (
            max(0, self.config.replacement_min_slots - replacement_active)
            if replacement_backlog
            else 0
        )
        check_limit = max(0, capacity - reserved)
        now = utc_epoch()
        for browser_id in managed:
            if len(self.futures) >= check_limit:
                break
            profile = self.store.profile(browser_id)
            if float(profile["next_due_at"] or 0) > now:
                continue
            self.submit(browser_id, "idle", self.idle_check)

    def close(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=False)


def write_summary(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the shared Scientist literature profile fleet"
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    config = load_fleet_config(args.config.resolve())
    store = FleetStore(args.database.resolve())
    fleet = ProfileFleet(config, store)
    try:
        fleet.recover_replacements()
        while True:
            fleet.tick()
            write_summary(
                args.summary.resolve(),
                store.summary(config.browser_ids, len(config.probes)),
            )
            if args.once:
                break
            time.sleep(config.poll_seconds)
    finally:
        fleet.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
