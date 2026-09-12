from __future__ import annotations

from datetime import datetime, timezone
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import time
from typing import Any
import urllib.error
import urllib.request

try:
    import fcntl
except ImportError:  # pragma: no cover - Linux production only
    fcntl = None


MAX_BROWSER_COUNT = 150
BROWSER_SERVICE_RE = re.compile(
    r"^campus-literature-browser(?:-([0-9]{2,3}))?$"
)


class LiteratureMaintenanceError(RuntimeError):
    pass


@contextmanager
def fleet_mutation_lock():
    path = Path(
        os.environ.get(
            "SCIENTIST_LITERATURE_FLEET_LOCK",
            "/run/lock/scientist-literature-browser-fleet.lock",
        )
    )
    if fcntl is None:
        yield
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def browser_service(index: int) -> str:
    if index < 1 or index > MAX_BROWSER_COUNT:
        raise LiteratureMaintenanceError(
            f"browser index must be between 1 and {MAX_BROWSER_COUNT}"
        )
    return (
        "campus-literature-browser"
        if index == 1
        else f"campus-literature-browser-{index:02d}"
    )


def browser_id_to_service(browser_id: str) -> str:
    prefix = "browser-"
    suffix = browser_id[len(prefix) :] if browser_id.startswith(prefix) else ""
    if not suffix.isdigit():
        raise LiteratureMaintenanceError("invalid browser_id")
    return browser_service(int(suffix))


def service_index(service: str) -> int | None:
    match = BROWSER_SERVICE_RE.fullmatch(service)
    if not match:
        return None
    return int(match.group(1) or "1")


def compose_prefix() -> list[str]:
    configured = os.environ.get("SCIENTIST_LARK_COMPOSE_COMMAND", "").strip()
    return shlex.split(configured) if configured else ["docker", "compose"]


def compose(
    repo: Path,
    *args: str,
    timeout: int = 300,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*compose_prefix(), *args],
        cwd=repo,
        check=True,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def docker_container(service: str, *, include_stopped: bool = False) -> str:
    command = ["docker", "ps"]
    if include_stopped:
        command.append("-a")
    command.extend(
        [
            "-q",
            "--filter",
            "label=com.docker.compose.project=lark-codex",
            "--filter",
            f"label=com.docker.compose.service={service}",
        ]
    )
    result = subprocess.run(
        command,
        check=True,
        text=True,
        capture_output=True,
        timeout=15,
    )
    return next((line for line in result.stdout.splitlines() if line), "")


def pool_container(_repo: Path) -> str:
    container = docker_container("literature-browser-pool")
    if not container:
        raise LiteratureMaintenanceError(
            "literature browser pool container is not running"
        )
    return container


def pool_request(
    repo: Path,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    direct_url = os.environ.get("SCIENTIST_LITERATURE_POOL_URL", "").strip()
    if direct_url:
        headers: dict[str, str] = {}
        data = None
        timeout = 10
        if payload is not None:
            data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
            timeout = max(30, int(payload.get("timeout_seconds") or 0) + 30)
        if path.startswith("/v1/admin/"):
            token_path = Path(
                os.environ.get(
                    "SCIENTIST_LITERATURE_POOL_ADMIN_TOKEN_FILE",
                    "/etc/scientist-literature/engine-admin-token",
                )
            )
            try:
                token = token_path.read_text(encoding="utf-8").strip()
            except OSError as exc:
                raise LiteratureMaintenanceError(
                    "literature pool admin token is unavailable"
                ) from exc
            if not token:
                raise LiteratureMaintenanceError(
                    "literature pool admin token is empty"
                )
            headers["X-Scientist-Pool-Admin"] = token
        request = urllib.request.Request(
            f"{direct_url.rstrip('/')}{path}",
            data=data,
            headers=headers,
            method="POST" if payload is not None else "GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                value = json.loads(response.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise LiteratureMaintenanceError(
                f"literature pool returned HTTP {exc.code}: {detail[:500]}"
            ) from exc
        except (OSError, TimeoutError, json.JSONDecodeError) as exc:
            raise LiteratureMaintenanceError(
                f"literature pool request failed: {type(exc).__name__}"
            ) from exc
        if not isinstance(value, dict):
            raise LiteratureMaintenanceError(
                "literature browser pool returned invalid status"
            )
        return value

    container = pool_container(repo)
    if payload is None:
        script = (
            "import json,urllib.request;"
            "print(json.dumps(json.load(urllib.request.urlopen("
            f"'http://127.0.0.1:9030{path}',timeout=5))))"
        )
    else:
        encoded = json.dumps(payload, separators=(",", ":"))
        script = (
            "import json,urllib.request;"
            f"data={encoded!r}.encode('utf-8');"
            "request=urllib.request.Request("
            f"'http://127.0.0.1:9030{path}',data=data,"
            "headers={'Content-Type':'application/json'},method='POST');"
            "print(json.dumps(json.load(urllib.request.urlopen(request,timeout=10))))"
        )
    result = subprocess.run(
        ["docker", "exec", container, "python3", "-c", script],
        check=True,
        text=True,
        capture_output=True,
        timeout=20,
    )
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        raise LiteratureMaintenanceError(
            "literature browser pool returned invalid status"
        )
    return value


def pool_status(repo: Path) -> dict[str, Any]:
    return pool_request(repo, "/ready")


def wait_service_healthy(
    _repo: Path,
    service: str,
    timeout_seconds: int = 180,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            container = docker_container(service)
            if container:
                inspected = json.loads(
                    subprocess.run(
                        ["docker", "inspect", container],
                        check=True,
                        text=True,
                        capture_output=True,
                        timeout=15,
                    ).stdout
                )[0]
                health = inspected.get("State", {}).get("Health", {}).get("Status")
                if health == "healthy":
                    return
        except (
            subprocess.SubprocessError,
            OSError,
            json.JSONDecodeError,
            IndexError,
            TypeError,
        ):
            # Docker can briefly stall while another bounded fleet mutation finishes.
            # Treat one slow inspection as transient and keep the overall deadline.
            pass
        time.sleep(2)
    raise LiteratureMaintenanceError(
        f"service did not become healthy: {service}"
    )


def write_state(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.chmod(temporary, 0o640)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
