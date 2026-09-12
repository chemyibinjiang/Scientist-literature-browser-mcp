from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import signal
import time
from typing import Any
from urllib.parse import urlparse

from .browser_client import BrowserClient
from .publisher_policy import load_policy, publisher_by_id


PROBE_SCHEMA = "scientist-literature-probes/v1"
REPORT_SCHEMA = "scientist-literature-monitor-report/v1"
FULL_TEXT_STATES = {
    "institutional_full_text",
    "publisher_full_text",
    "open_access_full_text",
    "full_text_visible",
}
DEFAULT_MINIMUM_DOWNLOAD_BYTES = 4096


class LiteratureMonitorError(ValueError):
    pass


def _host_in_domains(host: str, domains: list[str]) -> bool:
    normalized = host.lower().rstrip(".").lstrip(".")
    return any(
        normalized == domain or normalized.endswith(f".{domain}")
        for domain in domains
    )


def load_probes(path: Path, policy: dict[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LiteratureMonitorError(f"cannot read probe config: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema") != PROBE_SCHEMA:
        raise LiteratureMonitorError(f"probe schema must be {PROBE_SCHEMA}")
    probes = value.get("probes")
    if not isinstance(probes, list) or not probes:
        raise LiteratureMonitorError("probes must be a non-empty array")
    config_download_minimum = value.get("minimum_download_bytes")
    if config_download_minimum is not None and (
        isinstance(config_download_minimum, bool)
        or not isinstance(config_download_minimum, int)
        or config_download_minimum < 1024
    ):
        raise LiteratureMonitorError(
            "minimum_download_bytes must be an integer >= 1024"
        )
    all_domains = [
        str(domain).lower().lstrip(".")
        for publisher in policy["publishers"]
        if publisher["enabled"]
        for domain in publisher["domains"]
    ]
    seen: set[str] = set()
    for index, probe in enumerate(probes):
        if not isinstance(probe, dict):
            raise LiteratureMonitorError(f"probes[{index}] must be an object")
        probe_id = str(probe.get("id") or "")
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,63}", probe_id):
            raise LiteratureMonitorError(f"probes[{index}].id is invalid")
        if probe_id in seen:
            raise LiteratureMonitorError(f"duplicate probe id: {probe_id}")
        seen.add(probe_id)
        publisher_by_id(policy, str(probe.get("publisher_id") or ""))
        url = str(probe.get("url") or "")
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise LiteratureMonitorError(f"probes[{index}].url is invalid")
        if not _host_in_domains(parsed.hostname, all_domains):
            raise LiteratureMonitorError(
                f"probes[{index}].url is outside the approved publisher policy"
            )
        resource_kind = str(probe.get("resource_kind") or "article")
        if resource_kind not in {"article", "supplementary_pdf"}:
            raise LiteratureMonitorError(
                f"probes[{index}].resource_kind is invalid"
            )
        minimum = probe.get("minimum_content_chars", 12000)
        if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum < 1000:
            raise LiteratureMonitorError(
                f"probes[{index}].minimum_content_chars must be >= 1000"
            )
        download_minimum = probe.get("minimum_download_bytes")
        if download_minimum is not None and (
            isinstance(download_minimum, bool)
            or not isinstance(download_minimum, int)
            or download_minimum < 1024
        ):
            raise LiteratureMonitorError(
                f"probes[{index}].minimum_download_bytes must be >= 1024"
            )
    return value


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _safe_error(exc: Exception) -> str:
    return re.sub(r"\s+", " ", str(exc)).strip()[:500]


def run_probe(
    client: BrowserClient,
    probe: dict[str, Any],
    *,
    wait_ms: int,
    timeout_seconds: int,
    minimum_download_bytes: int = DEFAULT_MINIMUM_DOWNLOAD_BYTES,
) -> dict[str, Any]:
    started = time.monotonic()
    result: dict[str, Any] = {
        "id": probe["id"],
        "publisher_id": probe["publisher_id"],
        "resource_kind": probe.get("resource_kind", "article"),
    }
    try:
        read = client.read(
            str(probe["url"]),
            wait_ms=wait_ms,
            max_chars=0,
            timeout_seconds=timeout_seconds,
            affinity_key=str(probe.get("publisher_id") or ""),
            resource_kind=str(probe.get("resource_kind") or "article"),
            landing_url_hint=str(probe.get("landing_url_hint") or ""),
        )
        text_chars = len(str(read.get("text") or ""))
        minimum = int(probe.get("minimum_content_chars") or 12000)
        if result["resource_kind"] == "supplementary_pdf":
            pdf = read.get("pdf_extraction")
            pdf = pdf if isinstance(pdf, dict) else {}
            download_bytes = int(pdf.get("bytes") or 0)
            download_status = int(pdf.get("status") or 0)
            effective_download_minimum = max(
                1024,
                int(
                    probe.get("minimum_download_bytes")
                    or minimum_download_bytes
                ),
            )
            download_verified = (
                bool(pdf.get("success"))
                and 200 <= download_status < 300
                and download_bytes >= effective_download_minimum
            )
            healthy = (
                read.get("page_state") == "content"
                and read.get("text_source") == "pdf"
                and download_verified
                and text_chars >= minimum
                and not read.get("text_truncated")
            )
        else:
            pdf = {}
            download_bytes = 0
            download_status = 0
            download_verified = False
            healthy = (
                read.get("access_state") in FULL_TEXT_STATES
                and text_chars >= minimum
                and not read.get("text_truncated")
            )
        challenge = read.get("challenge_bypass")
        challenge = challenge if isinstance(challenge, dict) else {}
        result.update(
            {
                "status": "healthy" if healthy else "degraded",
                "access_state": read.get("access_state") or "missing",
                "page_state": read.get("page_state") or "missing",
                "text_source": read.get("text_source") or "",
                "content_chars": text_chars,
                "reference_count": len(read.get("references") or []),
                "challenge_attempted": bool(challenge.get("attempted")),
                "challenge_succeeded": bool(challenge.get("success")),
                "pdf_extraction_success": bool(pdf.get("success")),
                "download_verified": download_verified,
                "download_bytes": download_bytes,
                "download_status": download_status or None,
                "pdf_pages": int(pdf.get("pages") or 0),
            }
        )
    except Exception as exc:
        result.update(
            {
                "status": "degraded",
                "access_state": "error",
                "page_state": "error",
                "text_source": "",
                "content_chars": 0,
                "reference_count": 0,
                "challenge_attempted": False,
                "challenge_succeeded": False,
                "pdf_extraction_success": False,
                "download_verified": False,
                "download_bytes": 0,
                "download_status": None,
                "pdf_pages": 0,
                "error_type": type(exc).__name__,
                "message": _safe_error(exc),
            }
        )
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return result


def run_report(
    *,
    client: BrowserClient,
    policy_path: Path,
    probes_path: Path,
    output_path: Path,
    publisher_ids: set[str] | None = None,
    include_supplementary: bool = True,
) -> dict[str, Any]:
    policy = load_policy(policy_path)
    config = load_probes(probes_path, policy)
    probes = [
        item
        for item in config["probes"]
        if (not publisher_ids or item["publisher_id"] in publisher_ids)
        and (include_supplementary or item.get("resource_kind", "article") == "article")
    ]
    if not probes:
        raise LiteratureMonitorError("no probes matched the requested publishers")
    wait_ms = max(0, min(int(config.get("wait_ms") or 5000), 20000))
    timeout_seconds = max(45, min(int(config.get("timeout_seconds") or 600), 900))
    minimum_download_bytes = max(
        1024,
        int(
            config.get("minimum_download_bytes")
            or DEFAULT_MINIMUM_DOWNLOAD_BYTES
        ),
    )
    try:
        browser = client.ready()
    except Exception as exc:
        browser = {"ready": False, "message": _safe_error(exc)}
    results = [
        run_probe(
            client,
            probe,
            wait_ms=wait_ms,
            timeout_seconds=timeout_seconds,
            minimum_download_bytes=minimum_download_bytes,
        )
        for probe in probes
    ]
    healthy_count = sum(item["status"] == "healthy" for item in results)
    if not browser.get("ready") or healthy_count == 0:
        overall = "unavailable"
    elif healthy_count == len(results):
        overall = "healthy"
    else:
        overall = "degraded"
    report = {
        "schema": REPORT_SCHEMA,
        "checked_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "overall_status": overall,
        "healthy_count": healthy_count,
        "probe_count": len(results),
        "browser": browser,
        "probes": results,
    }
    _atomic_json(output_path, report)
    return report


def read_latest_report(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or value.get("schema") != REPORT_SCHEMA:
        return None
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Monitor literature browser access")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    policy_path = Path(
        os.environ.get("LITERATURE_PUBLISHER_POLICY", "/config/publishers.json")
    )
    probes_path = Path(
        os.environ.get("LITERATURE_PROBES_CONFIG", "/config/probes.json")
    )
    output_path = Path(
        os.environ.get("LITERATURE_HEALTH_REPORT", "/state/reports/latest.json")
    )
    interval = max(
        300, int(os.environ.get("LITERATURE_MONITOR_INTERVAL_SECONDS", "21600"))
    )
    startup_delay = max(
        0, int(os.environ.get("LITERATURE_MONITOR_STARTUP_DELAY_SECONDS", "60"))
    )
    client = BrowserClient()
    if args.once:
        report = run_report(
            client=client,
            policy_path=policy_path,
            probes_path=probes_path,
            output_path=output_path,
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["overall_status"] != "unavailable" else 1

    stopping = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    if startup_delay:
        time.sleep(startup_delay)
    while not stopping:
        try:
            report = run_report(
                client=client,
                policy_path=policy_path,
                probes_path=probes_path,
                output_path=output_path,
            )
            print(
                json.dumps(
                    {
                        "checked_at": report["checked_at"],
                        "overall_status": report["overall_status"],
                        "healthy_count": report["healthy_count"],
                        "probe_count": report["probe_count"],
                    }
                ),
                flush=True,
            )
        except Exception as exc:
            print(
                json.dumps(
                    {"overall_status": "error", "message": _safe_error(exc)}
                ),
                flush=True,
            )
        for _ in range(interval):
            if stopping:
                break
            time.sleep(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
