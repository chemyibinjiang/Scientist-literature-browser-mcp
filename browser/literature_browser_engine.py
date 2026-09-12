#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse


HOST = os.environ.get("LITERATURE_ENGINE_HOST", "0.0.0.0")
PORT = int(os.environ.get("LITERATURE_ENGINE_PORT", "9020"))
PROFILE_OFFSET = max(
    0,
    min(int(os.environ.get("LITERATURE_ENGINE_PROFILE_OFFSET", "0")), 149),
)
PROFILE_COUNT = max(
    1,
    min(
        int(os.environ.get("LITERATURE_ENGINE_PROFILE_COUNT", "80")),
        150 - PROFILE_OFFSET,
    ),
)
PROFILE_START = PROFILE_OFFSET + 1
PROFILE_END = PROFILE_OFFSET + PROFILE_COUNT
MAX_RESIDENT = max(
    1,
    min(
        int(os.environ.get("LITERATURE_ENGINE_MAX_RESIDENT", "80")),
        PROFILE_COUNT,
    ),
)
IDLE_SECONDS = max(
    60,
    int(os.environ.get("LITERATURE_ENGINE_IDLE_SECONDS", "1800")),
)
CHILD_PORT_BASE = int(os.environ.get("LITERATURE_ENGINE_CHILD_PORT_BASE", "10000"))
CHILD_START_TIMEOUT_SECONDS = max(
    5,
    int(os.environ.get("LITERATURE_ENGINE_CHILD_START_TIMEOUT_SECONDS", "30")),
)
PROFILE_ROOT = Path(
    os.environ.get("LITERATURE_ENGINE_PROFILE_ROOT", "/browser-profiles")
)
FLARESOLVERR_URLS = tuple(
    item.strip().rstrip("/")
    for item in os.environ.get(
        "LITERATURE_FLARESOLVERR_URLS",
        os.environ.get("LITERATURE_FLARESOLVERR_URL", ""),
    ).replace("\n", ",").split(",")
    if item.strip()
)
LOG_ROOT = Path(os.environ.get("LITERATURE_ENGINE_LOG_ROOT", "/tmp/literature-engine"))
BROWSER_SCRIPT = Path(
    os.environ.get("LITERATURE_ENGINE_BROWSER_SCRIPT", "/app/literature_browser.py")
)
ADMIN_TOKEN_FILE = Path(
    os.environ.get(
        "LITERATURE_ENGINE_ADMIN_TOKEN_FILE",
        "/run/secrets/literature-engine/admin-token",
    )
)
MAX_BODY_BYTES = 65536
ALLOWED_DOMAIN_COUNT = len(
    {
        item.strip().lower()
        for item in os.environ.get("LITERATURE_BROWSER_ALLOWED_DOMAINS", "").split(",")
        if item.strip()
    }
)
PROFILE_ROUTE_RE = re.compile(
    r"^/profiles/(?P<index>[0-9]{3})(?P<suffix>/.*)?$"
)
REPLACEMENT_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
ACTIVE_REPLACEMENT_STATES = ("preparing", "replacing", "warming")


class LiteratureEngineError(RuntimeError):
    pass


class LiteratureEngineBusy(LiteratureEngineError):
    pass


def utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def allowed_domain_count() -> int:
    return ALLOWED_DOMAIN_COUNT


def parse_profile_route(path: str) -> tuple[int, str] | None:
    match = PROFILE_ROUTE_RE.fullmatch(urlparse(path).path)
    if not match:
        return None
    index = int(match.group("index"))
    if index < PROFILE_START or index > PROFILE_END:
        return None
    return index, match.group("suffix") or "/"


def load_admin_token() -> str:
    try:
        return ADMIN_TOKEN_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class ProfileGenerationStore:
    """Crash-safe, target-bound profile replacement registry."""

    def __init__(self, profile_root: Path):
        self.profile_root = profile_root
        self.fleet_root = profile_root / ".fleet"
        self.transaction_root = self.fleet_root / "transactions"
        self.database_path = self.fleet_root / "replacements.sqlite3"
        self.lock = threading.RLock()
        self.fleet_root.mkdir(parents=True, exist_ok=True)
        self.transaction_root.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS replacements (
                    replacement_id TEXT PRIMARY KEY,
                    target_browser_id TEXT NOT NULL,
                    expected_generation_id TEXT NOT NULL,
                    candidate_generation_id TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    error_type TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_replacement_per_target
                ON replacements(target_browser_id)
                WHERE state IN ('preparing', 'replacing', 'warming');
                """
            )

    @staticmethod
    def _generation_path(profile_root: Path) -> Path:
        return profile_root / ".scientist-generation.json"

    def ensure_generation(self, slot: "ProfileSlot") -> str:
        path = self._generation_path(slot.profile_root)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        generation_id = str(payload.get("generation_id") or "").strip()
        if generation_id:
            return generation_id
        slot.profile_root.mkdir(parents=True, exist_ok=True)
        generation_id = f"legacy-{slot.browser_id}-{uuid.uuid4().hex}"
        atomic_json(
            path,
            {
                "schema": "scientist_literature_profile_generation/v1",
                "browser_id": slot.browser_id,
                "generation_id": generation_id,
                "created_at": utc_now(),
            },
        )
        return generation_id

    def _row(self, replacement_id: str) -> sqlite3.Row | None:
        with self._connect() as connection:
            return connection.execute(
                "SELECT * FROM replacements WHERE replacement_id = ?",
                (replacement_id,),
            ).fetchone()

    @staticmethod
    def _public(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "replacement_id": row["replacement_id"],
            "target_browser_id": row["target_browser_id"],
            "expected_generation_id": row["expected_generation_id"],
            "candidate_generation_id": row["candidate_generation_id"],
            "state": row["state"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "error_type": row["error_type"],
        }

    def begin(
        self,
        slot: "ProfileSlot",
        replacement_id: str,
        expected_generation_id: str = "",
    ) -> dict[str, Any]:
        if not REPLACEMENT_ID_RE.fullmatch(replacement_id):
            raise LiteratureEngineError("invalid replacement_id")
        with self.lock, slot.lock:
            if slot.active:
                raise LiteratureEngineBusy(
                    f"{slot.browser_id} is active and cannot be replaced"
                )
            slot.stop(force=True)
            existing = self._row(replacement_id)
            if existing is not None:
                if existing["target_browser_id"] != slot.browser_id:
                    raise LiteratureEngineError(
                        "replacement_id is already bound to another target"
                    )
                return self._public(existing)
            current_generation = self.ensure_generation(slot)
            if expected_generation_id and current_generation != expected_generation_id:
                raise LiteratureEngineBusy(
                    f"{slot.browser_id} generation changed before replacement"
                )
            candidate_generation = (
                f"candidate-{slot.browser_id}-{replacement_id}-{uuid.uuid4().hex}"
            )
            now = utc_now()
            with self._connect() as connection:
                try:
                    connection.execute(
                        """
                        INSERT INTO replacements(
                            replacement_id, target_browser_id,
                            expected_generation_id, candidate_generation_id,
                            state, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, 'preparing', ?, ?)
                        """,
                        (
                            replacement_id,
                            slot.browser_id,
                            current_generation,
                            candidate_generation,
                            now,
                            now,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise LiteratureEngineBusy(
                        f"{slot.browser_id} already has an active replacement"
                    ) from exc
            transaction = self.transaction_root / replacement_id
            old_root = transaction / "old"
            try:
                transaction.mkdir(parents=True, exist_ok=False)
                os.replace(slot.profile_root, old_root)
                slot.profile_root.mkdir(parents=True, exist_ok=False)
                atomic_json(
                    self._generation_path(slot.profile_root),
                    {
                        "schema": "scientist_literature_profile_generation/v1",
                        "browser_id": slot.browser_id,
                        "generation_id": candidate_generation,
                        "replacement_id": replacement_id,
                        "created_at": utc_now(),
                    },
                )
                with self._connect() as connection:
                    connection.execute(
                        "UPDATE replacements SET state='warming', updated_at=? "
                        "WHERE replacement_id=? AND state='preparing'",
                        (utc_now(), replacement_id),
                    )
            except Exception as exc:
                if not slot.profile_root.exists() and old_root.exists():
                    os.replace(old_root, slot.profile_root)
                with self._connect() as connection:
                    connection.execute(
                        "UPDATE replacements SET state='failed', updated_at=?, "
                        "error_type=? WHERE replacement_id=?",
                        (utc_now(), type(exc).__name__, replacement_id),
                    )
                raise
            row = self._row(replacement_id)
            if row is None:
                raise LiteratureEngineError("replacement registry lost its transaction")
            return self._public(row)

    def finish(
        self,
        slot: "ProfileSlot",
        replacement_id: str,
        *,
        commit: bool,
    ) -> dict[str, Any]:
        with self.lock, slot.lock:
            row = self._row(replacement_id)
            if row is None or row["target_browser_id"] != slot.browser_id:
                raise LiteratureEngineError("replacement transaction not found")
            if row["state"] not in ACTIVE_REPLACEMENT_STATES:
                return self._public(row)
            if slot.active:
                raise LiteratureEngineBusy(
                    f"{slot.browser_id} is active and cannot finish replacement"
                )
            slot.stop(force=True)
            live_generation = self.ensure_generation(slot)
            if live_generation != row["candidate_generation_id"]:
                raise LiteratureEngineBusy(
                    f"{slot.browser_id} candidate generation changed"
                )
            transaction = self.transaction_root / replacement_id
            old_root = transaction / "old"
            if commit:
                shutil.rmtree(transaction)
                state = "committed"
            else:
                aborted = transaction / "aborted-candidate"
                os.replace(slot.profile_root, aborted)
                if not old_root.is_dir():
                    os.replace(aborted, slot.profile_root)
                    raise LiteratureEngineError("replacement rollback source is missing")
                os.replace(old_root, slot.profile_root)
                shutil.rmtree(transaction)
                state = "rolled_back"
            with self._connect() as connection:
                connection.execute(
                    "UPDATE replacements SET state=?, updated_at=?, error_type=NULL "
                    "WHERE replacement_id=?",
                    (state, utc_now(), replacement_id),
                )
            result = self._row(replacement_id)
            if result is None:
                raise LiteratureEngineError("replacement registry lost its transaction")
            return self._public(result)

    def reconcile(self, slots: dict[int, "ProfileSlot"]) -> None:
        with self.lock, self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM replacements WHERE state IN "
                "('preparing', 'replacing', 'warming')"
            ).fetchall()
            for row in rows:
                index = int(str(row["target_browser_id"]).rsplit("-", 1)[1])
                slot = slots.get(index)
                if slot is None:
                    connection.execute(
                        "UPDATE replacements SET state='failed', updated_at=?, "
                        "error_type='UnknownProfile' WHERE replacement_id=?",
                        (utc_now(), row["replacement_id"]),
                    )
                    continue
                old_root = self.transaction_root / row["replacement_id"] / "old"
                live_path = self._generation_path(slot.profile_root)
                try:
                    live = json.loads(live_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    live = {}
                if live.get("generation_id") == row["candidate_generation_id"] and old_root.is_dir():
                    connection.execute(
                        "UPDATE replacements SET state='warming', updated_at=? "
                        "WHERE replacement_id=?",
                        (utc_now(), row["replacement_id"]),
                    )
                    continue
                if not slot.profile_root.exists() and old_root.is_dir():
                    os.replace(old_root, slot.profile_root)
                    shutil.rmtree(self.transaction_root / row["replacement_id"])
                    connection.execute(
                        "UPDATE replacements SET state='rolled_back', updated_at=?, "
                        "error_type='CrashReconciled' WHERE replacement_id=?",
                        (utc_now(), row["replacement_id"]),
                    )
                    continue
                connection.execute(
                    "UPDATE replacements SET state='failed', updated_at=?, "
                    "error_type='ReconciliationRequired' WHERE replacement_id=?",
                    (utc_now(), row["replacement_id"]),
                )

    def summary(self) -> dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT state, COUNT(*) AS count FROM replacements GROUP BY state"
            ).fetchall()
        return {str(row["state"]): int(row["count"]) for row in rows}


class ProfileSlot:
    def __init__(self, index: int):
        self.index = index
        self.browser_id = f"browser-{index:03d}"
        self.port = CHILD_PORT_BASE + index
        self.profile_root = PROFILE_ROOT / f"profile-{index:03d}"
        self.profile_dir = self.profile_root / "chromium"
        self.home_dir = self.profile_root / "home"
        self.runtime_dir = LOG_ROOT / "runtime" / self.browser_id
        self.log_path = LOG_ROOT / "workers" / f"{self.browser_id}.log"
        self.process: subprocess.Popen[bytes] | None = None
        self.log_handle: Any = None
        self.active = 0
        self.starts = 0
        self.recycles = 0
        self.failures = 0
        self.last_started_at: str | None = None
        self.last_used_at: str | None = None
        self.last_used_monotonic = 0.0
        self.lock = threading.RLock()
        self.solver_slot = (
            ((index - PROFILE_START) % len(FLARESOLVERR_URLS)) + 1
            if FLARESOLVERR_URLS
            else None
        )

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def _prepare_directories(self) -> None:
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self.home_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
            try:
                (self.profile_dir / name).unlink()
            except FileNotFoundError:
                pass

    def start(self) -> None:
        with self.lock:
            if self.is_running():
                return
            self.stop(force=True)
            self._prepare_directories()
            environment = os.environ.copy()
            if self.solver_slot is not None:
                environment["LITERATURE_FLARESOLVERR_URL"] = (
                    FLARESOLVERR_URLS[self.solver_slot - 1]
                )
            environment.update(
                {
                    "HOME": str(self.home_dir),
                    "XDG_RUNTIME_DIR": str(self.runtime_dir),
                    "LITERATURE_BROWSER_HOST": "127.0.0.1",
                    "LITERATURE_BROWSER_PORT": str(self.port),
                    "LITERATURE_BROWSER_PROFILE": str(self.profile_dir),
                    "LITERATURE_BROWSER_PROFILE_ID": self.browser_id,
                    "LITERATURE_BROWSER_CDP_URL": "",
                    "LITERATURE_BROWSER_HEADLESS": os.environ.get(
                        "LITERATURE_BROWSER_HEADLESS",
                        "false",
                    ),
                    "LITERATURE_BROWSER_DISABLE_DEV_SHM_USAGE": "false",
                }
            )
            self.log_handle = self.log_path.open("ab", buffering=0)
            self.process = subprocess.Popen(
                [sys.executable, str(BROWSER_SCRIPT)],
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=self.log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            self.starts += 1
            self.last_started_at = utc_now()
            self.last_used_monotonic = time.monotonic()
        self.wait_ready(CHILD_START_TIMEOUT_SECONDS)

    def wait_ready(self, timeout_seconds: int) -> None:
        deadline = time.monotonic() + max(1, timeout_seconds)
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            with self.lock:
                process = self.process
                if process is None or process.poll() is not None:
                    raise LiteratureEngineError(
                        f"{self.browser_id} worker exited during startup"
                    )
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{self.port}/ready",
                    timeout=1,
                ) as response:
                    if response.status == 200:
                        return
            except Exception as exc:
                last_error = exc
            time.sleep(0.1)
        self.stop(force=True)
        raise LiteratureEngineError(
            f"{self.browser_id} worker did not become ready: "
            f"{type(last_error).__name__ if last_error else 'timeout'}"
        )

    def stop(self, *, force: bool = False) -> bool:
        with self.lock:
            process = self.process
            if process is None:
                if self.log_handle is not None:
                    self.log_handle.close()
                    self.log_handle = None
                return False
            if self.active and not force:
                return False
            self.process = None
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except OSError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
        with self.lock:
            if self.log_handle is not None:
                self.log_handle.close()
                self.log_handle = None
        return True

    def status(self) -> dict[str, Any]:
        with self.lock:
            if self.is_running():
                state = "active" if self.active else "resident"
            else:
                state = "cold"
            return {
                "browser_id": self.browser_id,
                "state": state,
                "active": self.active,
                "starts": self.starts,
                "recycles": self.recycles,
                "failures": self.failures,
                "last_started_at": self.last_started_at,
                "last_used_at": self.last_used_at,
                "solver_slot": self.solver_slot,
            }


class LiteratureBrowserEngine:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.slots = {
            index: ProfileSlot(index)
            for index in range(PROFILE_START, PROFILE_END + 1)
        }
        self.generations = ProfileGenerationStore(PROFILE_ROOT)
        self.generations.reconcile(self.slots)
        self._closed = threading.Event()
        self.sweeper = threading.Thread(
            target=self._sweep_idle,
            name="literature-engine-idle-sweeper",
            daemon=True,
        )
        self.sweeper.start()

    def resident_slots(self) -> list[ProfileSlot]:
        return [slot for slot in self.slots.values() if slot.is_running()]

    def _evict_one(self, excluded_index: int) -> bool:
        candidates = [
            slot
            for slot in self.resident_slots()
            if slot.index != excluded_index and slot.active == 0
        ]
        if not candidates:
            return False
        victim = min(candidates, key=lambda slot: slot.last_used_monotonic)
        return victim.stop()

    def ensure_started(self, index: int) -> ProfileSlot:
        slot = self.slots[index]
        with self.lock:
            if slot.is_running():
                return slot
            while len(self.resident_slots()) >= MAX_RESIDENT:
                if not self._evict_one(index):
                    raise LiteratureEngineBusy(
                        "all resident browser slots are actively serving requests"
                    )
            slot.start()
            return slot

    def begin(self, index: int) -> ProfileSlot:
        slot = self.ensure_started(index)
        with slot.lock:
            slot.active += 1
            slot.last_used_at = utc_now()
            slot.last_used_monotonic = time.monotonic()
        return slot

    def finish(self, slot: ProfileSlot, *, failed: bool = False) -> None:
        with slot.lock:
            slot.active = max(0, slot.active - 1)
            slot.last_used_at = utc_now()
            slot.last_used_monotonic = time.monotonic()
            if failed:
                slot.failures += 1

    def recycle(self, index: int) -> dict[str, Any]:
        slot = self.slots[index]
        with self.lock, slot.lock:
            if slot.active:
                raise LiteratureEngineBusy(
                    f"{slot.browser_id} is active and cannot be recycled"
                )
            slot.stop(force=True)
            slot.recycles += 1
        return slot.status()

    def begin_replacement(
        self,
        index: int,
        replacement_id: str,
        expected_generation_id: str = "",
    ) -> dict[str, Any]:
        return self.generations.begin(
            self.slots[index],
            replacement_id,
            expected_generation_id,
        )

    def finish_replacement(
        self,
        index: int,
        replacement_id: str,
        *,
        commit: bool,
    ) -> dict[str, Any]:
        return self.generations.finish(
            self.slots[index],
            replacement_id,
            commit=commit,
        )

    def slot_ready(self, index: int) -> tuple[int, dict[str, Any]]:
        slot = self.slots[index]
        if not slot.is_running():
            return 200, {
                "ready": True,
                "profile": "cold",
                "allowed_domain_count": allowed_domain_count(),
                "lifecycle": {"worker_alive": True, "stalled": False},
            }
        request = urllib.request.Request(
            f"http://127.0.0.1:{slot.port}/ready",
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                return exc.code, json.loads(raw)
            except json.JSONDecodeError:
                return exc.code, {"ready": False, "message": "worker unhealthy"}
        except Exception as exc:
            slot.stop(force=True)
            return 503, {
                "ready": False,
                "message": f"worker transport failed: {type(exc).__name__}",
            }

    def status(self) -> dict[str, Any]:
        slots = [slot.status() for slot in self.slots.values()]
        resident = sum(item["state"] in {"resident", "active"} for item in slots)
        active = sum(item["active"] for item in slots)
        return {
            "ready": True,
            "service": "literature-browser-engine",
            "configured_profiles": PROFILE_COUNT,
            "profile_offset": PROFILE_OFFSET,
            "profile_start": PROFILE_START,
            "profile_end": PROFILE_END,
            "solver_service_count": len(FLARESOLVERR_URLS),
            "maximum_resident_profiles": MAX_RESIDENT,
            "resident_profiles": resident,
            "active_requests": active,
            "cold_profiles": PROFILE_COUNT - resident,
            "idle_reap_seconds": IDLE_SECONDS,
            "allowed_domain_count": allowed_domain_count(),
            "replacement_transactions": self.generations.summary(),
            "profiles": slots,
        }

    def _sweep_idle(self) -> None:
        while not self._closed.wait(min(30, max(5, IDLE_SECONDS // 4))):
            now = time.monotonic()
            for slot in self.resident_slots():
                with slot.lock:
                    should_stop = bool(
                        slot.active == 0
                        and slot.last_used_monotonic
                        and now - slot.last_used_monotonic >= IDLE_SECONDS
                    )
                if should_stop:
                    slot.stop()

    def close(self) -> None:
        self._closed.set()
        for slot in self.slots.values():
            slot.stop(force=True)


ENGINE = LiteratureBrowserEngine()


def send_json(
    handler: BaseHTTPRequestHandler,
    status: int,
    value: dict[str, Any],
) -> None:
    raw = json_bytes(value)
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    try:
        handler.wfile.write(raw)
    except (BrokenPipeError, ConnectionResetError):
        pass


def admin_authorized(handler: BaseHTTPRequestHandler) -> bool:
    expected = load_admin_token()
    if expected:
        supplied = handler.headers.get("X-Scientist-Engine-Admin", "")
        return secrets.compare_digest(supplied, expected)
    return handler.client_address[0] in {"127.0.0.1", "::1"}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        if urlparse(self.path).path == "/ready":
            send_json(self, 200, ENGINE.status())
            return
        route = parse_profile_route(self.path)
        if route and route[1] == "/ready":
            status, payload = ENGINE.slot_ready(route[0])
            send_json(self, status, payload)
            return
        send_json(self, 404, {"success": False, "message": "not found"})

    def do_POST(self) -> None:
        route = parse_profile_route(self.path)
        if route is None:
            send_json(self, 404, {"success": False, "message": "not found"})
            return
        index, suffix = route
        if suffix in {
            "/v1/admin/replacement/begin",
            "/v1/admin/replacement/commit",
            "/v1/admin/replacement/rollback",
        }:
            if not admin_authorized(self):
                send_json(self, 403, {"success": False, "message": "forbidden"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise LiteratureEngineError("invalid request body size")
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise LiteratureEngineError("request body must be an object")
                replacement_id = str(
                    payload.get("replacement_id") or ""
                ).strip()
                if suffix.endswith("/begin"):
                    transaction = ENGINE.begin_replacement(
                        index,
                        replacement_id,
                        str(payload.get("expected_generation_id") or "").strip(),
                    )
                else:
                    transaction = ENGINE.finish_replacement(
                        index,
                        replacement_id,
                        commit=suffix.endswith("/commit"),
                    )
                send_json(
                    self,
                    200,
                    {"success": True, "replacement": transaction},
                )
            except LiteratureEngineBusy as exc:
                send_json(self, 409, {"success": False, "message": str(exc)})
            except (
                LiteratureEngineError,
                OSError,
                sqlite3.Error,
                TypeError,
                ValueError,
                json.JSONDecodeError,
            ) as exc:
                send_json(
                    self,
                    400,
                    {
                        "success": False,
                        "message": f"replacement failed: {type(exc).__name__}",
                    },
                )
            return
        if suffix == "/v1/admin/recycle":
            if not admin_authorized(self):
                send_json(self, 403, {"success": False, "message": "forbidden"})
                return
            try:
                send_json(
                    self,
                    200,
                    {"success": True, "profile": ENGINE.recycle(index)},
                )
            except LiteratureEngineBusy as exc:
                send_json(self, 409, {"success": False, "message": str(exc)})
            return
        if suffix != "/v1/literature/read":
            send_json(self, 404, {"success": False, "message": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0 or length > MAX_BODY_BYTES:
                raise LiteratureEngineError("invalid request body size")
            raw = self.rfile.read(length)
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise LiteratureEngineError("request body must be an object")
            timeout_seconds = max(
                45,
                min(int(payload.get("timeout_seconds") or 600), 600),
            )
            slot = ENGINE.begin(index)
        except LiteratureEngineBusy as exc:
            send_json(self, 503, {"success": False, "message": str(exc)})
            return
        except (LiteratureEngineError, TypeError, ValueError, json.JSONDecodeError) as exc:
            send_json(self, 400, {"success": False, "message": str(exc)})
            return

        failed = False
        try:
            request = urllib.request.Request(
                f"http://127.0.0.1:{slot.port}/v1/literature/read",
                data=raw,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(
                    request,
                    timeout=timeout_seconds + 10,
                ) as response:
                    response_raw = response.read()
                    response_status = response.status
            except urllib.error.HTTPError as exc:
                response_raw = exc.read()
                response_status = exc.code
            self.send_response(response_status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(response_raw)))
            self.end_headers()
            try:
                self.wfile.write(response_raw)
            except (BrokenPipeError, ConnectionResetError):
                pass
        except Exception as exc:
            failed = True
            slot.stop(force=True)
            send_json(
                self,
                502,
                {
                    "success": False,
                    "message": f"profile worker transport failed: {type(exc).__name__}",
                },
            )
        finally:
            ENGINE.finish(slot, failed=failed)


class EngineServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256


def main() -> None:
    if not BROWSER_SCRIPT.is_file():
        raise SystemExit(f"browser worker script is missing: {BROWSER_SCRIPT}")
    server = EngineServer((HOST, PORT), Handler)
    print(
        "[literature-browser-engine] "
        f"listen={HOST}:{PORT} profiles={PROFILE_COUNT} "
        f"max_resident={MAX_RESIDENT}",
        flush=True,
    )
    try:
        server.serve_forever()
    finally:
        ENGINE.close()
        server.server_close()


if __name__ == "__main__":
    main()
