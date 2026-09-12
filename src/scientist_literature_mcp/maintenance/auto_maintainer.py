#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import contextlib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

try:
    import fcntl
except ImportError:  # pragma: no cover - the service runs on Linux.
    fcntl = None


FULL_TEXT_STATES = {
    "institutional_full_text",
    "publisher_full_text",
    "open_access_full_text",
    "full_text_visible",
}
DEFAULT_MINIMUM_DOWNLOAD_BYTES = 4096
SUPPLEMENTARY_PATH_MARKERS = (
    "/doi/suppl/",
    "/suppdata/",
    "/supplement/",
    "/supplementary/",
    "/supporting-information/",
    "/suppl_file/",
    "/esm/",
)
SUPPLEMENTARY_FILENAME_RE = re.compile(
    r"(?:-mmc\d+|_moesm\d+_esm|[-_.](?:si|esi|supinfo|supp(?:lement(?:ary)?)?|"
    r"supporting[-_]?information))\.pdf$",
    flags=re.IGNORECASE,
)
COPY_EXCLUDES = {
    ".git",
    ".env",
    "__pycache__",
    "imports",
    "workspaces",
}
UNTRACKED_SOURCE_ROOTS = {
    ".codex",
    ".github",
    "deploy",
    "docs",
    "examples",
    "packages",
    "runtime",
    "schemas",
    "tests",
    "tools",
}


class MaintenanceError(RuntimeError):
    pass


class MaintenanceAgentBlocked(MaintenanceError):
    pass


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise MaintenanceError(f"{path} must contain a JSON object")
    return value


def save_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def sha256(path: Path) -> str:
    content = path.read_bytes()
    if path.suffix.lower() in {
        ".json",
        ".md",
        ".py",
        ".sh",
        ".toml",
        ".yaml",
        ".yml",
    }:
        content = content.replace(b"\r\n", b"\n")
    return hashlib.sha256(content).hexdigest()


def source_hashes(root: Path) -> dict[str, str]:
    found: dict[str, str] = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if any(part in COPY_EXCLUDES for part in relative.parts):
            continue
        found[relative.as_posix()] = sha256(path)
    return found


def changed_files(
    before: dict[str, str],
    candidate: Path,
) -> list[str]:
    after = source_hashes(candidate)
    names = set(before) | set(after)
    return sorted(name for name in names if before.get(name) != after.get(name))


def run(
    command: list[str],
    *,
    cwd: Path | None = None,
    timeout: int = 3600,
    capture: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=str(cwd) if cwd else None,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
            timeout=timeout,
            check=True,
            env=env,
        )
    except subprocess.CalledProcessError as exc:
        detail = "\n".join(
            part[-4000:]
            for part in (str(exc.stdout or ""), str(exc.stderr or ""))
            if part
        )
        raise MaintenanceError(
            f"command failed ({exc.returncode}): {' '.join(command)}\n{detail}"
        ) from exc


def configured_root(name: str, fallback: Path) -> Path:
    configured = os.environ.get(name, "").strip()
    return (
        Path(configured).expanduser().resolve()
        if configured
        else fallback.resolve()
    )


def workspace_root(repo: Path) -> Path:
    return configured_root("SCIENTIST_LARK_WORKSPACE_ROOT", repo / "workspaces")


def config_root(repo: Path) -> Path:
    return configured_root("SCIENTIST_LARK_CONFIG_ROOT", repo / "config")


def compose(repo: Path, *args: str, timeout: int = 3600) -> subprocess.CompletedProcess[str]:
    command = [
        "docker",
        "compose",
        "--project-name",
        os.environ.get("SCIENTIST_LARK_PROJECT_NAME", "lark-codex"),
    ]
    env_file = os.environ.get("SCIENTIST_LARK_ENV_FILE", "").strip()
    if env_file:
        command.extend(["--env-file", env_file])
    command.extend(["-f", str(repo / "docker-compose.yml")])
    remote_compose = repo / "docker-compose.remote.yml"
    if remote_compose.is_file():
        command.extend(["-f", str(remote_compose)])
    fleet_compose = config_root(repo) / "literature-browser-fleet.generated.json"
    if fleet_compose.is_file():
        command.extend(["-f", str(fleet_compose)])
    command.extend(args)
    return run(
        command,
        cwd=repo,
        timeout=timeout,
    )


def parse_scheduler_result(output: str) -> dict[str, Any]:
    start = output.find("{")
    if start < 0:
        raise MaintenanceError("scheduler returned no JSON result")
    return json.loads(output[start:])


def run_access_check(
    repo: Path,
    label: str,
    *,
    dry_run: bool = True,
) -> dict[str, Any]:
    command = [
        "exec",
        "-T",
        "lark-codex-scheduler",
        "/usr/local/bin/lark-codex-scheduler",
        "literature-access-check-once",
        label,
    ]
    if dry_run:
        command.append("--dry-run")
    result = compose(
        repo,
        *command,
        timeout=1800,
    )
    return parse_scheduler_result(result.stdout)


def current_report(repo: Path) -> dict[str, Any]:
    return load_json(
        workspace_root(repo)
        / "outputs"
        / "literature-access-health"
        / "latest.json"
    )


def report_is_fresh(
    report: dict[str, Any],
    *,
    now: datetime,
    maximum_age_hours: int,
) -> bool:
    if str(report.get("overall_status") or "") != "healthy":
        return False
    checked_at = str(report.get("checked_at") or "").strip()
    if not checked_at:
        return False
    try:
        observed = datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=now.tzinfo)
    age = now.astimezone(timezone.utc) - observed.astimezone(timezone.utc)
    return timedelta(0) <= age <= timedelta(hours=max(1, maximum_age_hours))


def snapshot_is_recent(
    snapshot: dict[str, Any],
    *,
    now: datetime,
    maximum_age_minutes: int,
) -> bool:
    updated_at = str(snapshot.get("updated_at") or "").strip()
    if not updated_at:
        return False
    try:
        observed = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=now.tzinfo)
    age = now.astimezone(timezone.utc) - observed.astimezone(timezone.utc)
    return timedelta(0) <= age <= timedelta(
        minutes=max(1, maximum_age_minutes)
    )


def event_is_recent(
    event: dict[str, Any],
    *,
    now: datetime,
    maximum_age_hours: int,
) -> bool:
    finished_at = str(event.get("finished_at") or "").strip()
    if not finished_at:
        return False
    try:
        observed = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=now.tzinfo)
    age = now.astimezone(timezone.utc) - observed.astimezone(timezone.utc)
    return timedelta(0) <= age <= timedelta(hours=max(1, maximum_age_hours))


def initial_access_state(
    repo: Path,
    run_id: str,
    config: dict[str, Any],
    *,
    now: datetime,
) -> tuple[dict[str, Any], str]:
    patrol_path = str(config.get("profile_patrol_state_path") or "").strip()
    if patrol_path:
        patrol = load_json(Path(patrol_path))
        profile_patrol = patrol.get("profile_patrol")
        aggregate = (
            profile_patrol.get("aggregate")
            if isinstance(profile_patrol, dict)
            else None
        )
        publisher_access = patrol.get("publisher_access")
        publisher_status = (
            str(publisher_access.get("status") or "")
            if isinstance(publisher_access, dict)
            else ""
        )
        if isinstance(aggregate, dict):
            needs_debug = int(aggregate.get("needs_debug") or 0)
            degraded = int(aggregate.get("degraded") or 0)
            failure_fingerprint = str(
                aggregate.get("failure_fingerprint") or ""
            ).strip()
            if needs_debug > 0:
                failure_source = "profile_runtime_failure"
            elif degraded > 0 or publisher_status in {
                "degraded",
                "unavailable",
            }:
                failure_source = "patrol_failure_event"
            else:
                failure_source = ""
            if failure_source:
                previous_path = str(
                    config.get("maintenance_report_path") or ""
                ).strip()
                previous = load_json(Path(previous_path)) if previous_path else {}
                if (
                    failure_fingerprint
                    and previous.get("initial_failure_fingerprint")
                    == failure_fingerprint
                    and event_is_recent(
                        previous,
                        now=now,
                        maximum_age_hours=int(
                            config.get("failure_event_repeat_hours") or 3
                        ),
                    )
                ):
                    return (
                        {
                            "status": "healthy",
                            "failure_fingerprint": failure_fingerprint,
                        },
                        "patrol_failure_already_seen",
                    )
                result = run_access_check(
                    repo, f"auto-runtime-failure-{run_id}"
                )
                return (
                    {**result, "failure_fingerprint": failure_fingerprint},
                    failure_source,
                )
            if snapshot_is_recent(
                patrol,
                now=now,
                maximum_age_minutes=int(
                    config.get("patrol_snapshot_max_age_minutes") or 15
                ),
            ):
                return (
                    {
                        "status": "healthy",
                        "healthy_count": int(
                            publisher_access.get("healthy_count") or 0
                        )
                        if isinstance(publisher_access, dict)
                        else 0,
                        "probe_count": int(
                            publisher_access.get("probe_count") or 0
                        )
                        if isinstance(publisher_access, dict)
                        else 0,
                        "cycle_status": aggregate.get("cycle_status"),
                    },
                    "patrol_event_idle",
                )
    try:
        report = current_report(repo)
    except (OSError, ValueError, json.JSONDecodeError):
        report = {}
    if report_is_fresh(
        report,
        now=now,
        maximum_age_hours=int(config.get("fresh_report_hours") or 8),
    ):
        return (
            {
                "status": "healthy",
                "healthy_count": int(report.get("healthy_count") or 0),
                "probe_count": int(report.get("probe_count") or 0),
                "report": str(
                    workspace_root(repo)
                    / "outputs"
                    / "literature-access-health"
                    / "latest.json"
                ),
            },
            "fresh_report",
        )
    return run_access_check(repo, f"auto-{run_id}"), "live_full_check"


def persistent_failure_urls(report: dict[str, Any]) -> list[str]:
    candidates = report.get("production_verification")
    if not isinstance(candidates, list):
        candidates = report.get("probes")
    urls: list[str] = []
    for item in candidates if isinstance(candidates, list) else []:
        if not isinstance(item, dict) or item.get("status") == "healthy":
            continue
        url = str(item.get("url") or "").strip()
        if url.startswith(("https://", "http://")) and url not in urls:
            urls.append(url)
    return urls


def publisher_family_for_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "unknown").lower().removeprefix("www.")
    path = unquote(parsed.path).lower()
    if host == "doi.org":
        if path.startswith("/10.1021/"):
            return "acs"
        if path.startswith("/10.1039/"):
            return "rsc"
        if path.startswith("/10.1016/"):
            return "elsevier"
        if path.startswith("/10.1002/"):
            return "wiley"
        if path.startswith(("/10.1038/", "/10.1007/")):
            return "nature-springer"
        if path.startswith("/10.1073/"):
            return "pnas"
        if path.startswith("/10.1126/"):
            return "science"
    families = {
        "acs": ("acs.org", "figshare.com"),
        "rsc": ("rsc.org",),
        "elsevier": ("elsevier.com", "sciencedirect.com", "els-cdn.com"),
        "wiley": ("wiley.com",),
        "nature-springer": ("nature.com", "springer.com"),
        "pnas": ("pnas.org",),
        "science": ("science.org",),
    }
    for family, suffixes in families.items():
        if any(host == suffix or host.endswith(f".{suffix}") for suffix in suffixes):
            return family
    return host


def group_failure_urls(urls: list[str]) -> list[list[str]]:
    grouped: dict[tuple[str, str], list[str]] = {}
    for url in urls:
        key = (publisher_family_for_url(url), resource_kind_for_url(url))
        bucket = grouped.setdefault(key, [])
        if url not in bucket:
            bucket.append(url)
    return [grouped[key] for key in sorted(grouped)]


def repair_fingerprint(urls: list[str]) -> str:
    descriptors = []
    for url in sorted(set(urls)):
        descriptors.append(
            f"{publisher_family_for_url(url)}|{resource_kind_for_url(url)}"
        )
    material = "\n".join(descriptors).encode("utf-8")
    return hashlib.sha256(material).hexdigest()[:16]


def create_repair_record(
    runtime_root: Path,
    run_id: str,
    urls: list[str],
) -> tuple[Path, str]:
    fingerprint = repair_fingerprint(urls)
    record_root = runtime_root / "repairs" / fingerprint / run_id
    save_json(
        record_root / "incident.json",
        {
            "schema": "literature_repair_incident/v1",
            "run_id": run_id,
            "fingerprint": fingerprint,
            "failures": [
                {
                    "host": urlparse(url).hostname or "unknown",
                    "resource_kind": resource_kind_for_url(url),
                    "url_sha256": hashlib.sha256(url.encode("utf-8")).hexdigest(),
                }
                for url in sorted(set(urls))
            ],
        },
    )
    return record_root, fingerprint


def review_incidents(repo: Path) -> list[tuple[Path, dict[str, Any]]]:
    directory = (
        workspace_root(repo)
        / "outputs"
        / "literature-access-incidents"
        / "review"
    )
    found: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(directory.glob("*.json")):
        try:
            value = load_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        url = str(value.get("url") or "").strip()
        next_patrol = str(value.get("next_patrol_at") or "").strip()
        if next_patrol:
            try:
                if datetime.now(timezone.utc) < datetime.fromisoformat(
                    next_patrol.replace("Z", "+00:00")
                ):
                    continue
            except (TypeError, ValueError):
                pass
        if url.startswith(("https://", "http://")):
            found.append((path, value))
    return found


def production_probe(repo: Path, url: str) -> dict[str, Any]:
    code = (
        "import json,sys;"
        "sys.path.insert(0,'/usr/local/bin');"
        "from literature_worker_client import request_json;"
        "x=request_json('/v1/literature/read',"
        "payload={'url':sys.argv[1],'wait_ms':5000,'max_chars':0,"
        "'timeout_seconds':240},timeout=270,"
        "base_url='http://literature-browser-pool:9030');"
        "print(json.dumps({'success':x.get('success'),"
        "'access_state':x.get('access_state'),"
        "'content_chars':len(x.get('text') or ''),"
        "'text_source':x.get('text_source'),"
        "'page_state':x.get('page_state'),"
        "'text_truncated':bool(x.get('text_truncated')),'pdf_extraction_success':"
        "bool((x.get('pdf_extraction') or {}).get('success')),'pdf_pages':"
        "(x.get('pdf_extraction') or {}).get('pages'),'download_bytes':"
        "int((x.get('pdf_extraction') or {}).get('bytes') or 0),"
        "'download_status':(x.get('pdf_extraction') or {}).get('status'),"
        "'download_verified':bool((x.get('pdf_extraction') or {}).get('success'))"
        " and 200 <= int((x.get('pdf_extraction') or {}).get('status') or 0) < 300"
        " and int((x.get('pdf_extraction') or {}).get('bytes') or 0) >= 4096}))"
    )
    result = compose(
        repo,
        "exec",
        "-T",
        "lark-codex-scheduler",
        "python3",
        "-c",
        code,
        url,
        timeout=330,
    )
    return json.loads(result.stdout)


def resolve_incident(
    repo: Path,
    review_path: Path,
    incident: dict[str, Any],
    probe: dict[str, Any],
) -> None:
    sys.path.insert(0, str(repo / "docker"))
    from literature_strategy import record_patrol_result

    root = workspace_root(repo) / "outputs" / "literature-access-incidents"
    pending = root / "pending" / review_path.name
    if pending.exists():
        try:
            record_patrol_result(
                pending,
                incident,
                {"status": "healthy", **probe},
                root=root,
                strategy_path=config_root(repo) / "literature-access-strategy.json",
            )
        except PermissionError:
            resolve_incident_via_container(repo, root, review_path.name, probe)


def resolve_incident_via_container(
    repo: Path,
    root: Path,
    incident_name: str,
    probe: dict[str, Any],
) -> None:
    if (
        root.name != "literature-access-incidents"
        or Path(incident_name).name != incident_name
        or not incident_name.endswith(".json")
    ):
        raise MaintenanceError("refusing privileged resolution outside incident root")
    owner = root.stat()
    safe_probe = {
        key: probe.get(key)
        for key in (
            "success",
            "access_state",
            "content_chars",
            "text_source",
            "page_state",
            "text_truncated",
            "pdf_extraction_success",
            "pdf_pages",
            "resource_kind",
        )
        if key in probe
    }
    payload = base64.b64encode(
        json.dumps(
            {
                "incident_name": incident_name,
                "probe": safe_probe,
            }
        ).encode("utf-8")
    ).decode("ascii")
    code = """
import base64
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, "/usr/local/bin")
from literature_strategy import record_patrol_result

root = Path("/incidents")
payload = json.loads(base64.b64decode(os.environ["PAYLOAD"]))
name = str(payload["incident_name"])
if Path(name).name != name or not name.endswith(".json"):
    raise RuntimeError("invalid incident name")
pending = root / "pending" / name
incident = json.loads(pending.read_text(encoding="utf-8"))
target = record_patrol_result(
    pending,
    incident,
    {"status": "healthy", **payload["probe"]},
    root=root,
    strategy_path=Path("/strategy.json"),
)
print(json.dumps({"status": "resolved", "target": target.name}))
"""
    result = run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--user",
            f"{owner.st_uid}:{owner.st_gid}",
            "--mount",
            f"type=bind,src={root.resolve()},dst=/incidents",
            "--mount",
            (
                "type=bind,src="
                f"{(config_root(repo) / 'literature-access-strategy.json').resolve()},"
                "dst=/strategy.json,readonly"
            ),
            "--env",
            f"PAYLOAD={payload}",
            "--entrypoint",
            "python3",
            "lark-codex:local",
            "-c",
            code,
        ],
        timeout=120,
    )
    value = json.loads(result.stdout)
    if value.get("status") != "resolved":
        raise MaintenanceError("incident resolution container did not resolve incident")


def defer_incidents(
    repo: Path,
    urls: list[str],
    *,
    hours: int,
) -> int:
    root = workspace_root(repo) / "outputs" / "literature-access-incidents"
    try:
        return defer_incidents_direct(root, urls, hours=hours)
    except PermissionError:
        return defer_incidents_via_container(root, urls, hours=hours)


def defer_incidents_direct(
    root: Path,
    urls: list[str],
    *,
    hours: int,
) -> int:
    deferred = 0
    next_patrol = (
        datetime.now(timezone.utc) + timedelta(hours=max(1, hours))
    ).isoformat(timespec="seconds").replace("+00:00", "Z")
    for bucket in ("pending", "review"):
        for path in (root / bucket).glob("*.json"):
            try:
                value = load_json(path)
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            if str(value.get("url") or "") not in urls:
                continue
            value["next_patrol_at"] = next_patrol
            value["auto_maintenance_deferred_at"] = datetime.now(
                timezone.utc
            ).isoformat(timespec="seconds").replace("+00:00", "Z")
            save_json(path, value)
            deferred += 1
    return deferred


def defer_incidents_via_container(
    root: Path,
    urls: list[str],
    *,
    hours: int,
) -> int:
    if root.name != "literature-access-incidents":
        raise MaintenanceError("refusing privileged write outside incident root")
    owner = root.stat()
    payload = base64.b64encode(
        json.dumps(
            {
                "urls": urls,
                "hours": max(1, hours),
            }
        ).encode("utf-8")
    ).decode("ascii")
    code = """
import base64
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

root = Path("/incidents")
payload = json.loads(base64.b64decode(os.environ["PAYLOAD"]))
urls = set(payload["urls"])
now = datetime.now(timezone.utc)
next_patrol = (now + timedelta(hours=payload["hours"])).isoformat(
    timespec="seconds"
).replace("+00:00", "Z")
deferred = 0
for bucket in ("pending", "review"):
    for path in (root / bucket).glob("*.json"):
        value = json.loads(path.read_text(encoding="utf-8"))
        if str(value.get("url") or "") not in urls:
            continue
        value["next_patrol_at"] = next_patrol
        value["auto_maintenance_deferred_at"] = now.isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\\n",
            encoding="utf-8",
        )
        temporary.replace(path)
        deferred += 1
print(json.dumps({"deferred": deferred}))
"""
    result = run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--user",
            f"{owner.st_uid}:{owner.st_gid}",
            "--mount",
            f"type=bind,src={root.resolve()},dst=/incidents",
            "--env",
            f"PAYLOAD={payload}",
            "--entrypoint",
            "python3",
            "lark-codex:local",
            "-c",
            code,
        ],
        timeout=120,
    )
    value = json.loads(result.stdout)
    return int(value.get("deferred") or 0)


def sync_candidate(repo: Path, candidate: Path) -> None:
    if candidate.exists():
        try:
            shutil.rmtree(candidate)
        except PermissionError:
            if candidate.name != "candidate":
                raise MaintenanceError("refusing privileged cleanup outside candidate")
            run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--user",
                    "0:0",
                    "--entrypoint",
                    "rm",
                    "-v",
                    f"{candidate.parent}:/maintenance",
                    "lark-codex:local",
                    "-rf",
                    "--",
                    "/maintenance/candidate",
                ],
                timeout=120,
            )
    shutil.copytree(
        repo,
        candidate,
        ignore=shutil.ignore_patterns(*COPY_EXCLUDES),
    )
    tools = workspace_root(repo) / "default" / "tools"
    if tools.exists():
        shutil.copytree(
            tools,
            candidate / "workspaces" / "default" / "tools",
            ignore=shutil.ignore_patterns("__pycache__"),
        )


def create_isolated_candidate(
    repo: Path,
    runtime_root: Path,
    fingerprint: str,
    run_id: str,
) -> dict[str, Any]:
    worktree_root = runtime_root / "worktrees" / fingerprint / run_id
    try:
        git_root = Path(
            run(
                ["git", "-C", str(repo), "rev-parse", "--show-toplevel"],
                timeout=30,
            ).stdout.strip()
        ).resolve()
        relative_repo = repo.resolve().relative_to(git_root)
    except (MaintenanceError, OSError, ValueError):
        candidate = worktree_root / "candidate"
        sync_candidate(repo, candidate)
        return {
            "mode": "snapshot_copy",
            "root": worktree_root,
            "candidate": candidate,
            "git_root": None,
        }

    snapshot = run(
        ["git", "-C", str(git_root), "rev-parse", "HEAD"],
        timeout=30,
    ).stdout.strip()
    if not re.fullmatch(r"[a-f0-9]{40,64}", snapshot):
        raise MaintenanceError("git did not produce a valid maintenance snapshot")
    worktree_root.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            "git",
            "-C",
            str(git_root),
            "worktree",
            "add",
            "--detach",
            str(worktree_root),
            snapshot,
        ],
        timeout=180,
    )
    overlay_git_working_tree(git_root, worktree_root)
    candidate = worktree_root / relative_repo
    tools = workspace_root(repo) / "default" / "tools"
    if tools.exists():
        shutil.copytree(
            tools,
            candidate / "workspaces" / "default" / "tools",
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
    return {
        "mode": "git_worktree",
        "root": worktree_root,
        "candidate": candidate,
        "git_root": git_root,
        "snapshot": snapshot,
    }


def git_path_list(git_root: Path, *args: str) -> list[Path]:
    output = run(
        ["git", "-C", str(git_root), *args, "-z"],
        timeout=60,
    ).stdout
    return [Path(value) for value in output.split("\0") if value]


def candidate_path_allowed(relative: Path, *, untracked: bool) -> bool:
    if relative.is_absolute() or ".." in relative.parts:
        return False
    if any(part in COPY_EXCLUDES for part in relative.parts):
        return False
    return not untracked or (
        bool(relative.parts) and relative.parts[0] in UNTRACKED_SOURCE_ROOTS
    )


def overlay_git_working_tree(git_root: Path, worktree_root: Path) -> None:
    changed = git_path_list(
        git_root,
        "diff",
        "--no-renames",
        "--name-only",
        "--diff-filter=ACMRTUXB",
        "HEAD",
    )
    untracked = git_path_list(
        git_root,
        "ls-files",
        "--others",
        "--exclude-standard",
    )
    deleted = git_path_list(
        git_root,
        "diff",
        "--no-renames",
        "--name-only",
        "--diff-filter=D",
        "HEAD",
    )

    for relative, is_untracked in [
        *((path, False) for path in changed),
        *((path, True) for path in untracked),
    ]:
        if not candidate_path_allowed(relative, untracked=is_untracked):
            continue
        source = git_root / relative
        target = worktree_root / relative
        if source.is_symlink():
            raise MaintenanceError("candidate overlay may not copy symlinks")
        if not source.is_file():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    for relative in deleted:
        if not candidate_path_allowed(relative, untracked=False):
            continue
        target = worktree_root / relative
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink(missing_ok=True)


def container_workdir(candidate: Path, runtime_root: Path) -> str:
    try:
        relative = candidate.resolve().relative_to(runtime_root.resolve())
    except ValueError as exc:
        raise MaintenanceError("candidate is outside the maintenance runtime root") from exc
    return "/maintenance/" + relative.as_posix()


def restore_tree_owner(path: Path, owner_source: Path) -> None:
    if not path.exists():
        return
    owner = owner_source.stat()
    run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--user",
            "0:0",
            "--mount",
            f"type=bind,src={path.resolve()},dst=/worktree",
            "--entrypoint",
            "/bin/sh",
            "lark-codex:local",
            "-c",
            f"chown -R {owner.st_uid}:{owner.st_gid} /worktree",
        ],
        timeout=120,
    )


def cleanup_isolated_candidate(isolation: dict[str, Any], runtime_root: Path) -> None:
    root = Path(isolation["root"])
    if not root.exists():
        return
    restore_tree_owner(root, runtime_root)
    git_root = isolation.get("git_root")
    if isolation.get("mode") == "git_worktree" and git_root:
        run(
            [
                "git",
                "-C",
                str(git_root),
                "worktree",
                "remove",
                "--force",
                str(root),
            ],
            timeout=180,
        )
    else:
        shutil.rmtree(root, ignore_errors=True)


def reject_candidate_symlinks(candidate: Path) -> None:
    for directory, names, files in os.walk(candidate, followlinks=False):
        root = Path(directory)
        for name in [*names, *files]:
            if (root / name).is_symlink():
                raise MaintenanceError("candidate workspace may not contain symlinks")


def normalize_candidate_permissions(candidate: Path) -> None:
    reject_candidate_symlinks(candidate)
    run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--user",
            "0:0",
            "--mount",
            f"type=bind,src={candidate.resolve()},dst=/candidate",
            "--entrypoint",
            "/bin/sh",
            "lark-codex:local",
            "-c",
            (
                "chown -R codex:codex /candidate && "
                "find -P /candidate -type d -exec chmod 0755 {} + && "
                "find -P /candidate -type f -exec chmod 0644 {} +"
            ),
        ],
        timeout=120,
    )


def replace_protected_file(source: Path, target: Path, repo: Path) -> None:
    resolved_repo = repo.resolve()
    resolved_target = target.resolve()
    if resolved_target == resolved_repo or resolved_repo not in resolved_target.parents:
        raise MaintenanceError("refusing protected write outside repository")
    if source.is_symlink() or target.is_symlink():
        raise MaintenanceError("refusing protected write through symlink")
    run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--user",
            "0:0",
            "--mount",
            f"type=bind,src={source.resolve()},dst=/source,readonly",
            "--mount",
            f"type=bind,src={resolved_target},dst=/target",
            "--entrypoint",
            "/bin/sh",
            "lark-codex:local",
            "-c",
            "cat /source > /target && chmod 0644 /target",
        ],
        timeout=120,
    )


def ast_named_nodes(source: str) -> dict[str, ast.AST]:
    tree = ast.parse(source)
    found: dict[str, ast.AST] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            found[f"function:{node.name}"] = node
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    found[f"assignment:{target.id}"] = node
    return found


def validate_security_invariants(repo: Path, candidate: Path) -> None:
    relative = Path("docker/literature_browser.py")
    baseline = ast_named_nodes((repo / relative).read_text(encoding="utf-8"))
    proposed = ast_named_nodes((candidate / relative).read_text(encoding="utf-8"))
    protected = {
        "assignment:DEFAULT_ALLOWED_DOMAINS",
        "assignment:DEFAULT_BROWSER_PDF_FETCH_DOMAINS",
        "assignment:FLARESOLVERR_ALLOWED_DOMAINS",
        "assignment:SUPPLEMENTARY_PDF_PATH_MARKERS",
        "assignment:SUPPLEMENTARY_PDF_FILENAME_RE",
        "function:validate_url",
        "function:host_in_domains",
        "function:host_allowed",
        "function:flaresolverr_host_allowed",
        "function:browser_pdf_fetch_host_allowed",
        "function:validate_browser_pdf_result_url",
        "function:fetch_pdf_in_browser",
        "function:fetch_public_figshare_pdf",
        "function:is_supplementary_pdf_url",
        "function:supplementary_pdf_landing_url",
        "function:fetch_supplementary_pdf_from_origin",
    }
    for name in protected:
        if name not in baseline or name not in proposed:
            raise MaintenanceError(f"protected browser policy node is missing: {name}")
        if ast.dump(baseline[name], include_attributes=False) != ast.dump(
            proposed[name],
            include_attributes=False,
        ):
            raise MaintenanceError(f"agent changed protected browser policy: {name}")

    extractor = proposed.get("function:extract_pdf_urls_from_viewer")
    if extractor is None:
        raise MaintenanceError("extract_pdf_urls_from_viewer is missing")
    calls_pdf_allowlist = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "browser_pdf_fetch_host_allowed"
        for node in ast.walk(extractor)
    )
    if not calls_pdf_allowlist:
        raise MaintenanceError(
            "extract_pdf_urls_from_viewer bypasses the dedicated PDF fetch allowlist"
        )


def call_agent(
    endpoint: str,
    *,
    session_id: str,
    prompt: str,
    timeout: int,
    workdir: str,
) -> dict[str, Any]:
    payload = {
        "session_id": session_id,
        "session_mode": "ephemeral",
        "dialogue": [{"role": "user", "content": prompt}],
        "kwargs": {
            "timeout": timeout,
            "session_mode": "ephemeral",
            "sandbox": "workspace-write",
            "workdir": workdir,
        },
    }
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout + 30) as response:
        value = json.load(response)
    if not isinstance(value, dict) or not value.get("success"):
        raise MaintenanceError(
            str(value.get("message") if isinstance(value, dict) else value)
        )
    return value


def repair_prompt(
    failure_urls: list[str],
    allowed_files: list[str],
    workdir: str = "/maintenance/candidate",
) -> str:
    return f"""You are the unattended Scientist literature-browser maintainer.

The production patrol and confirmation check found persistent full-text failures for:
{json.dumps(failure_urls, ensure_ascii=False, indent=2)}

Your current working directory is `{workdir}`. First verify that it
is writable. Work only inside that candidate workspace. Diagnose the failure
using the existing source, tests, and governed literature browser endpoints. You
may edit only these files:
{json.dumps(allowed_files, ensure_ascii=False, indent=2)}

Do not edit Docker Compose, Dockerfiles, dependencies, credentials, domain
allowlists, publisher subscriptions, NAS/Lark data, or deployment scripts. Do
not operate Docker, send messages, weaken evidence thresholds, or merely relabel
the failure. Make the smallest general fix supported by evidence, add a focused
regression test, and run the focused tests. If no bounded code fix is justified,
make no changes.

Always write `.maintenance/repair-manifest.json`, including when no bounded code
fix is justified. It must contain:
`status` (`repaired` or `blocked`), `summary`, `changed_files`, `tests_run`, and
`residual_risks`. Do not include article bodies, credentials, or conversation
identifiers. Your final response should be a concise maintenance summary.
"""


def validate_manifest(candidate: Path, changed: list[str]) -> dict[str, Any]:
    manifest = load_json(candidate / ".maintenance" / "repair-manifest.json")
    if manifest.get("status") != "repaired":
        raise MaintenanceError(
            f"maintenance agent did not produce a repair: {manifest.get('summary')}"
        )
    declared = sorted(str(value) for value in manifest.get("changed_files", []))
    if declared != sorted(changed):
        raise MaintenanceError("repair manifest changed_files does not match filesystem")
    return manifest


def runtime_test_modules(candidate: Path) -> list[str]:
    return [
        f"tests.{path.stem}"
        for path in sorted((candidate / "tests").glob("test_*.py"))
        if path.stem != "test_literature_browser"
    ]


def validate_candidate(candidate: Path) -> None:
    run(
        [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "python3",
            "-v",
            f"{candidate}:/src:ro",
            "-w",
            "/src",
            "lark-codex-literature:local",
            "-m",
            "unittest",
            "tests.test_literature_browser",
        ],
        timeout=600,
    )
    modules = runtime_test_modules(candidate)
    if not modules:
        raise MaintenanceError("candidate runtime test suite is empty")
    test_env = os.environ.copy()
    for name in (
        "SCIENTIST_LARK_RUNTIME_ROOT",
        "SCIENTIST_LARK_WORKSPACE_ROOT",
        "SCIENTIST_LARK_CONFIG_ROOT",
        "SCIENTIST_LARK_ENV_FILE",
        "SCIENTIST_LARK_PROJECT_NAME",
        "SCIENTIST_LARK_COMPOSE_COMMAND",
    ):
        test_env.pop(name, None)
    run(
        [sys.executable, "-m", "unittest", *modules],
        cwd=candidate,
        timeout=900,
        env=test_env,
    )
    run(["docker", "compose", "config", "--quiet"], cwd=candidate, timeout=120)


def network_name(repo: Path) -> str:
    result = compose(repo, "ps", "-q", "literature-browser-pool")
    container = result.stdout.strip()
    if not container:
        raise MaintenanceError("production literature pool is not running")
    value = json.loads(run(["docker", "inspect", container]).stdout)[0]
    return next(iter(value["NetworkSettings"]["Networks"]))


def wait_ready(container: str, timeout: int = 90) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            [
                "docker",
                "exec",
                container,
                "python3",
                "-c",
                (
                    "import urllib.request;"
                    "urllib.request.urlopen('http://127.0.0.1:9020/ready',"
                    "timeout=3).read()"
                ),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            return
        time.sleep(2)
    raise MaintenanceError(f"candidate browser {container} did not become ready")


def browser_read(container: str, url: str, timeout: int = 300) -> dict[str, Any]:
    code = (
        "import json,sys,urllib.request;"
        "url=sys.argv[1];"
        "body=json.dumps({'url':url,'wait_ms':5000,'max_chars':0,"
        "'timeout_seconds':240}).encode();"
        "req=urllib.request.Request('http://127.0.0.1:9020/v1/literature/read',"
        "data=body,headers={'Content-Type':'application/json'},method='POST');"
        "print(urllib.request.urlopen(req,timeout=270).read().decode())"
    )
    result = run(
        ["docker", "exec", container, "python3", "-c", code, url],
        timeout=timeout,
    )
    return json.loads(result.stdout)


def resource_kind_for_url(url: str, explicit: object = None) -> str:
    if str(explicit or "").strip() == "supplementary_pdf":
        return "supplementary_pdf"
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = unquote(parsed.path).lower()
    query = unquote(parsed.query).lower()
    if (
        (
            host in {"acs.figshare.com", "ndownloader.figshare.com"}
            or host.endswith(".acs.figshare.com")
        )
        and re.fullmatch(r"/(?:ndownloader/)?files/\d+/?", path)
    ):
        return "supplementary_pdf"
    if (
        (host == "wiley.com" or host.endswith(".wiley.com"))
        and path.rstrip("/") == "/action/downloadsupplement"
    ):
        return "supplementary_pdf"
    if (
        (host == "pubs.acs.org" or host.endswith(".pubs.acs.org"))
        and re.fullmatch(
            r"/[^/]+/article-supplement/\d+/pdf/[^/]+/?",
            path,
        )
    ):
        return "supplementary_pdf"
    if path.endswith(".pdf") and any(
        marker in path for marker in SUPPLEMENTARY_PATH_MARKERS
    ):
        return "supplementary_pdf"
    filename = path.rsplit("/", 1)[-1]
    if path.endswith(".pdf") and SUPPLEMENTARY_FILENAME_RE.search(filename):
        return "supplementary_pdf"
    if path.rstrip("/").endswith("/downloadsupplement") and re.search(
        r"(?:^|&)file=[^&]*\.pdf(?:&|$)",
        query,
    ):
        return "supplementary_pdf"
    return "article"


def result_content_chars(result: dict[str, Any]) -> int:
    recorded = result.get("content_chars")
    if isinstance(recorded, int) and not isinstance(recorded, bool):
        return recorded
    return len(str(result.get("text") or ""))


def result_is_full_text(result: dict[str, Any], minimum: int) -> bool:
    return (
        result.get("success") is not False
        and str(result.get("access_state") or "") in FULL_TEXT_STATES
        and result_content_chars(result) >= minimum
        and not bool(result.get("text_truncated"))
    )


def result_is_supplementary_pdf(
    result: dict[str, Any],
    minimum: int,
) -> bool:
    extraction = (
        result.get("pdf_extraction")
        if isinstance(result.get("pdf_extraction"), dict)
        else {}
    )
    extraction_success = bool(
        result.get("pdf_extraction_success") or extraction.get("success")
    )
    if "download_verified" in result:
        download_verified = bool(result.get("download_verified"))
    else:
        download_bytes = int(extraction.get("bytes") or 0)
        download_status = int(extraction.get("status") or 0)
        download_verified = (
            extraction_success
            and 200 <= download_status < 300
            and download_bytes >= DEFAULT_MINIMUM_DOWNLOAD_BYTES
        )
    return (
        result.get("success") is not False
        and str(result.get("page_state") or "") == "content"
        and str(result.get("text_source") or "") == "pdf"
        and extraction_success
        and download_verified
        and result_content_chars(result) >= minimum
        and not bool(result.get("text_truncated"))
    )


def result_matches_resource(
    result: dict[str, Any],
    minimum: int,
    resource_kind: str,
) -> bool:
    if resource_kind == "supplementary_pdf":
        return result_is_supplementary_pdf(result, minimum)
    return result_is_full_text(result, minimum)


def minimum_for_resource(config: dict[str, Any], resource_kind: str) -> int:
    if resource_kind == "supplementary_pdf":
        return int(config.get("supplementary_minimum_content_chars") or 5000)
    return int(config.get("minimum_content_chars") or 12000)


@contextlib.contextmanager
def candidate_browser(
    repo: Path,
    image: str,
    run_id: str,
):
    name = f"literature-maintainer-{run_id}"
    network = network_name(repo)
    run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--network",
            network,
            "--dns",
            "223.5.5.5",
            "--dns",
            "119.29.29.29",
            "-e",
            "LITERATURE_FLARESOLVERR_URL=http://flaresolverr:8191/v1",
            "-e",
            (
                "LITERATURE_FLARESOLVERR_ALLOWED_DOMAINS="
                "rsc.org,sciencedirect.com,elsevier.com,wiley.com,science.org"
            ),
            "-e",
            "LITERATURE_BROWSER_MAX_CHARS=80000",
            "--tmpfs",
            "/browser-profile:rw,uid=10001,gid=10001,mode=0700",
            image,
        ],
        timeout=120,
    )
    try:
        wait_ready(name)
        yield name
    finally:
        subprocess.run(
            ["docker", "rm", "-f", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def wait_service_healthy(repo: Path, service: str, timeout: int = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        container = compose(repo, "ps", "-q", service).stdout.strip()
        if container:
            value = json.loads(run(["docker", "inspect", container]).stdout)[0]
            if value["State"].get("Health", {}).get("Status") == "healthy":
                return
        time.sleep(2)
    raise MaintenanceError(f"{service} did not become healthy")


def roll_browsers(repo: Path, services: list[str]) -> None:
    for service in services:
        compose(
            repo,
            "up",
            "-d",
            "--no-deps",
            "--force-recreate",
            service,
            timeout=300,
        )
        wait_service_healthy(repo, service)


def running_browser_services(repo: Path) -> list[str]:
    result = compose(repo, "ps", "--services", "--status", "running")
    services = []
    for raw in result.stdout.splitlines():
        service = raw.strip()
        if service == "campus-literature-browser" or re.fullmatch(
            r"campus-literature-browser-\d{2,3}", service
        ):
            services.append(service)
    return services


def restore_files(repo: Path, backup: Path, files: list[str]) -> None:
    for relative in files:
        source = backup / relative
        target = repo / relative
        replace_protected_file(source, target, repo)


def log_event(log_root: Path, event: dict[str, Any]) -> None:
    log_root.mkdir(parents=True, exist_ok=True)
    with (log_root / "events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")
    save_json(log_root / "latest.json", event)


def prepare_repair_job(
    repo: Path,
    runtime_root: Path,
    config: dict[str, Any],
    run_id: str,
    failure_urls: list[str],
) -> dict[str, Any]:
    record, fingerprint = create_repair_record(
        runtime_root,
        run_id,
        failure_urls,
    )
    job: dict[str, Any] = {
        "fingerprint": fingerprint,
        "failure_urls": list(failure_urls),
        "repair_record": str(record),
        "status": "preparing",
    }
    isolation: dict[str, Any] | None = None
    candidate_image = f"lark-codex-literature:auto-{run_id}-{fingerprint[:8]}"
    try:
        isolation = create_isolated_candidate(
            repo,
            runtime_root,
            fingerprint,
            run_id,
        )
        candidate = Path(isolation["candidate"])
        normalize_candidate_permissions(candidate)
        before = source_hashes(candidate)
        allowed = [str(value) for value in config["allowed_files"]]
        agent_workdir = container_workdir(candidate, runtime_root)
        response = call_agent(
            str(config["agent_endpoint"]),
            session_id=f"{str(config['agent_session_id'])}-{fingerprint}",
            prompt=repair_prompt(failure_urls, allowed, agent_workdir),
            timeout=int(config.get("agent_timeout_seconds") or 3600),
            workdir=agent_workdir,
        )
        job["agent_response_chars"] = len(str(response.get("text") or ""))
        normalize_candidate_permissions(candidate)
        changed = changed_files(before, candidate)
        disallowed = sorted(
            set(changed) - set(allowed) - {".maintenance/repair-manifest.json"}
        )
        if disallowed:
            raise MaintenanceError(f"agent changed disallowed files: {disallowed}")
        changed = [
            name for name in changed if name != ".maintenance/repair-manifest.json"
        ]
        if not changed:
            manifest_path = candidate / ".maintenance" / "repair-manifest.json"
            if not manifest_path.is_file():
                raise MaintenanceError(
                    "maintenance agent made no code change and wrote no repair "
                    "manifest; verify candidate workspace write access"
                )
            manifest = load_json(manifest_path)
            job["agent_manifest_status"] = str(manifest.get("status") or "unknown")
            summary = str(manifest.get("summary") or "no summary provided").strip()
            if job["agent_manifest_status"] == "blocked":
                job["agent_blocked_summary"] = summary
                raise MaintenanceAgentBlocked(
                    f"maintenance agent reported no bounded code fix: {summary}"
                )
            raise MaintenanceError(
                "maintenance agent manifest claimed a repair but no allowed "
                "source file changed"
            )
        manifest = validate_manifest(candidate, changed)
        validate_security_invariants(repo, candidate)
        validate_candidate(candidate)
        run(
            [
                "docker",
                "build",
                "--no-cache",
                "-f",
                "Dockerfile.literature",
                "-t",
                candidate_image,
                ".",
            ],
            cwd=candidate,
            timeout=1800,
        )
        with candidate_browser(
            repo,
            candidate_image,
            f"{run_id}-{fingerprint[:8]}",
        ) as browser:
            for url in [*failure_urls, str(config["control_url"])]:
                result = browser_read(browser, url)
                resource_kind = resource_kind_for_url(url)
                minimum = minimum_for_resource(config, resource_kind)
                if not result_matches_resource(result, minimum, resource_kind):
                    raise MaintenanceError(
                        "candidate failed content validation for a required probe"
                    )
        baseline_hashes = {
            relative: sha256(repo / relative)
            for relative in changed
            if (repo / relative).is_file()
        }
        job.update(
            {
                "status": "prepared",
                "candidate": str(candidate),
                "isolation": isolation,
                "isolation_mode": str(isolation["mode"]),
                "changed_files": changed,
                "baseline_hashes": baseline_hashes,
                "manifest": manifest,
            }
        )
        save_json(record / "result.json", {key: value for key, value in job.items() if key != "isolation"})
        return job
    except Exception as exc:
        job.update(
            {
                "status": "prepare_failed",
                "error_type": type(exc).__name__,
                "error": str(exc)[:1000],
                "_exception": exc,
            }
        )
        save_json(
            record / "result.json",
            {key: value for key, value in job.items() if not key.startswith("_")},
        )
        if isolation is not None:
            try:
                cleanup_isolated_candidate(isolation, runtime_root)
            except Exception:
                pass
        return job
    finally:
        subprocess.run(
            ["docker", "image", "rm", candidate_image],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def prepare_repair_jobs(
    repo: Path,
    runtime_root: Path,
    config: dict[str, Any],
    run_id: str,
    groups: list[list[str]],
) -> list[dict[str, Any]]:
    maximum = max(
        1,
        min(
            int(config.get("max_parallel_maintainers") or 2),
            len(groups),
        ),
    )
    results: dict[str, dict[str, Any]] = {}
    order = [repair_fingerprint(group) for group in groups]
    with ThreadPoolExecutor(max_workers=maximum) as executor:
        futures = {
            executor.submit(
                prepare_repair_job,
                repo,
                runtime_root,
                config,
                run_id,
                group,
            ): repair_fingerprint(group)
            for group in groups
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return [results[fingerprint] for fingerprint in order]


def production_urls_healthy(
    repo: Path,
    config: dict[str, Any],
    urls: list[str],
) -> bool:
    for url in urls:
        result = production_probe(repo, url)
        kind = resource_kind_for_url(url)
        if not result_matches_resource(
            result,
            minimum_for_resource(config, kind),
            kind,
        ):
            return False
    return True


def deploy_prepared_repair(
    repo: Path,
    runtime_root: Path,
    config: dict[str, Any],
    run_id: str,
    job: dict[str, Any],
) -> dict[str, Any]:
    changed = [str(value) for value in job["changed_files"]]
    conflicts = [
        relative
        for relative in changed
        if not (repo / relative).is_file()
        or sha256(repo / relative) != job["baseline_hashes"].get(relative)
    ]
    if conflicts:
        if production_urls_healthy(repo, config, list(job["failure_urls"])):
            return {**job, "status": "resolved_by_prior_repair", "conflicts": conflicts}
        return {**job, "status": "conflict_deferred", "conflicts": conflicts}

    fingerprint = str(job["fingerprint"])
    candidate = Path(job["candidate"])
    backup = runtime_root / "backups" / run_id / fingerprint
    for relative in changed:
        target = backup / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repo / relative, target)

    rollback_image = f"lark-codex-literature:rollback-{run_id}-{fingerprint[:8]}"
    run(["docker", "tag", "lark-codex-literature:local", rollback_image])
    production_services = running_browser_services(repo) or [
        str(value) for value in config["production_services"]
    ]
    for relative in changed:
        replace_protected_file(candidate / relative, repo / relative, repo)
    try:
        compose(
            repo,
            "build",
            "--no-cache",
            "campus-literature-browser",
            timeout=1800,
        )
        roll_browsers(repo, production_services)
        if not production_urls_healthy(
            repo,
            config,
            [*list(job["failure_urls"]), str(config["control_url"])],
        ):
            raise MaintenanceError("post-deployment targeted verification failed")
    except Exception:
        restore_files(repo, backup, changed)
        run(["docker", "tag", rollback_image, "lark-codex-literature:local"])
        roll_browsers(repo, production_services)
        raise
    return {
        **job,
        "status": "deployed",
        "backup": str(backup),
    }


@contextlib.contextmanager
def serialized_deployment(runtime_root: Path):
    lock_path = runtime_root / "deployment.lock"
    with lock_path.open("w") as lock:
        if fcntl is None:
            raise MaintenanceError("serialized deployment requires Linux fcntl")
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def maintenance_cycle(repo: Path, runtime_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now().astimezone()
    run_id = now.strftime("%Y%m%d-%H%M%S")
    event: dict[str, Any] = {
        "schema": "literature_auto_maintenance_event/v1",
        "run_id": run_id,
        "started_at": now.isoformat(timespec="seconds"),
        "status": "running",
    }
    log_root = runtime_root / "logs"
    production_touched = False
    try:
        initial, initial_source = initial_access_state(
            repo,
            run_id,
            config,
            now=now,
        )
        event["initial_source"] = initial_source
        if initial.get("failure_fingerprint"):
            event["initial_failure_fingerprint"] = initial["failure_fingerprint"]
        monitor_status = str(initial.get("status") or "unknown")
        event["initial_status"] = monitor_status
        if monitor_status == "canary_degraded":
            compose(
                repo,
                "up",
                "-d",
                "--no-deps",
                "--force-recreate",
                "campus-literature-browser-probe",
            )
            wait_service_healthy(repo, "campus-literature-browser-probe")
            retry = run_access_check(repo, f"auto-canary-{run_id}")
            monitor_status = str(retry.get("status") or "unknown")
            event["canary_retry_status"] = monitor_status

        failure_urls: list[str] = []
        if monitor_status in {"degraded", "unavailable"}:
            failure_urls.extend(persistent_failure_urls(current_report(repo)))

        checked_reviews = 0
        recovered_reviews = 0
        review_probe_errors: list[str] = []
        for path, incident in review_incidents(repo)[:2]:
            checked_reviews += 1
            incident_url = str(incident["url"])
            resource_kind = resource_kind_for_url(
                incident_url,
                incident.get("resource_kind"),
            )
            try:
                probe = {
                    **production_probe(repo, incident_url),
                    "resource_kind": resource_kind,
                }
            except Exception as exc:
                review_probe_errors.append(type(exc).__name__)
                continue
            minimum = minimum_for_resource(config, resource_kind)
            if result_matches_resource(probe, minimum, resource_kind):
                resolve_incident(repo, path, incident, probe)
                recovered_reviews += 1
            elif incident_url not in failure_urls:
                failure_urls.append(incident_url)
        event["review_incidents_checked"] = checked_reviews
        event["review_incidents_recovered"] = recovered_reviews
        event["review_incident_probe_error_count"] = len(review_probe_errors)
        if review_probe_errors:
            event["review_incident_probe_error_types"] = sorted(
                set(review_probe_errors)
            )[:5]
        event["persistent_failure_count"] = len(failure_urls)

        if not failure_urls:
            event["status"] = (
                "canary_recovered"
                if event.get("initial_status") == "canary_degraded"
                else "healthy_no_change"
            )
            return event
        if monitor_status == "canary_degraded":
            event["status"] = "canary_degraded"
            return event

        groups = group_failure_urls(failure_urls)
        event["repair_group_count"] = len(groups)
        event["repair_fingerprints"] = [
            repair_fingerprint(group) for group in groups
        ]
        jobs = prepare_repair_jobs(
            repo,
            runtime_root,
            config,
            run_id,
            groups,
        )
        prepared = [job for job in jobs if job.get("status") == "prepared"]
        if not prepared:
            first_error = next(
                (job.get("_exception") for job in jobs if job.get("_exception")),
                MaintenanceError("all isolated maintainer jobs failed"),
            )
            raise first_error

        outcomes: list[dict[str, Any]] = []
        with serialized_deployment(runtime_root):
            for job in jobs:
                if job.get("status") != "prepared":
                    outcomes.append(job)
                    continue
                try:
                    production_touched = True
                    outcome = deploy_prepared_repair(
                        repo,
                        runtime_root,
                        config,
                        run_id,
                        job,
                    )
                except Exception as exc:
                    outcome = {
                        **job,
                        "status": "deployment_failed",
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:1000],
                    }
                outcomes.append(outcome)

        deferred_urls: list[str] = []
        for outcome in outcomes:
            if outcome.get("status") in {
                "prepare_failed",
                "conflict_deferred",
                "deployment_failed",
            }:
                deferred_urls.extend(str(url) for url in outcome["failure_urls"])
            record = Path(str(outcome["repair_record"]))
            save_json(
                record / "result.json",
                {
                    key: value
                    for key, value in outcome.items()
                    if key not in {"isolation", "_exception"}
                },
            )
            isolation = outcome.get("isolation")
            if isinstance(isolation, dict):
                try:
                    cleanup_isolated_candidate(isolation, runtime_root)
                except Exception as cleanup_exc:
                    outcome["cleanup_error_type"] = type(cleanup_exc).__name__

        if deferred_urls:
            event["incidents_deferred"] = defer_incidents(
                repo,
                sorted(set(deferred_urls)),
                hours=int(config.get("blocked_retry_hours") or 24),
            )
        final = run_access_check(repo, f"auto-post-{run_id}")
        outcome_statuses = [str(outcome.get("status")) for outcome in outcomes]
        event["repair_jobs"] = [
            {
                "fingerprint": outcome.get("fingerprint"),
                "status": outcome.get("status"),
                "isolation_mode": outcome.get("isolation_mode"),
                "changed_files": outcome.get("changed_files", []),
                "error_type": outcome.get("error_type"),
            }
            for outcome in outcomes
        ]
        event["post_status"] = final.get("status")
        event["status"] = (
            "repaired"
            if final.get("status") == "healthy" and not deferred_urls
            else "partial_repair"
        )
        event["changed_files"] = sorted(
            {
                relative
                for outcome in outcomes
                if outcome.get("status") == "deployed"
                for relative in outcome.get("changed_files", [])
            }
        )
        event["repair_job_statuses"] = outcome_statuses
        return event
    except Exception as exc:
        failure_type = type(exc).__name__
        failure_detail = str(exc)[:1000]
        if not production_touched:
            try:
                recovered = run_access_check(repo, f"auto-recovery-{run_id}")
                event["recovery_status"] = str(
                    recovered.get("status") or "unknown"
                )
                if event["recovery_status"] == "healthy":
                    event["status"] = "transient_recovered"
                    event["recovered_after_error_type"] = failure_type
                    return event
            except Exception as recovery_exc:
                event["recovery_check_error_type"] = type(recovery_exc).__name__
        event["status"] = "repair_failed"
        event["error_type"] = failure_type
        event["error"] = failure_detail
        failure_urls = locals().get("failure_urls", [])
        if failure_urls:
            try:
                event["incidents_deferred"] = defer_incidents(
                    repo,
                    list(failure_urls),
                    hours=int(config.get("blocked_retry_hours") or 24),
                )
            except Exception as defer_exc:
                event["incident_defer_error"] = type(defer_exc).__name__
        if (
            event.get("initial_status") in {"degraded", "unavailable"}
            or int(event.get("persistent_failure_count") or 0) > 0
        ) and os.environ.get("LITERATURE_MAINTAINER_NO_NOTIFY") != "1":
            try:
                notified = run_access_check(
                    repo,
                    f"auto-failed-{run_id}",
                    dry_run=False,
                )
                event["failure_notification"] = notified.get("notification")
            except Exception as notify_exc:
                event["failure_notification_error"] = type(notify_exc).__name__
        return event
    finally:
        event["finished_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
        log_event(log_root, event)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path(
            os.environ.get(
                "SCIENTIST_LARK_RUNTIME_ROOT",
                "/srv/hd-scientist/repos/Scientist/runtime/lark",
            )
        ),
    )
    parser.add_argument(
        "--runtime-root",
        type=Path,
        default=Path("/srv/hd-scientist/runtime/literature-maintainer"),
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/literature-auto-maintainer.json"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo = args.repo.resolve()
    config_path = args.config
    if not config_path.is_absolute():
        config_path = repo / config_path
    config = load_json(config_path)
    if not config.get("enabled", False):
        return 0
    args.runtime_root.mkdir(parents=True, exist_ok=True)
    lock_path = args.runtime_root / "maintenance.lock"
    with lock_path.open("w") as lock:
        if fcntl is None:
            raise MaintenanceError("the unattended maintainer requires Linux fcntl")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        event = maintenance_cycle(repo, args.runtime_root, config)
    print(json.dumps(event, ensure_ascii=False, indent=2))
    return 0 if event["status"] not in {"repair_failed"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
