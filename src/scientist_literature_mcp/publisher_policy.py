from __future__ import annotations

import argparse
from datetime import date
import hashlib
import ipaddress
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse


POLICY_SCHEMA = "scientist-literature-publisher-policy/v1"
DOMAIN_RE = re.compile(
    r"(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
)
PUBLISHER_ID_RE = re.compile(r"[a-z0-9][a-z0-9-]{1,63}")


class PublisherPolicyError(ValueError):
    pass


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PublisherPolicyError(f"{field} must be a non-empty string")
    return value.strip()


def normalize_domain(value: Any, field: str) -> str:
    domain = _required_string(value, field).lower().rstrip(".").lstrip(".")
    if "*" in domain or "://" in domain or "/" in domain:
        raise PublisherPolicyError(f"{field} must be a literal DNS suffix")
    try:
        ipaddress.ip_address(domain)
    except ValueError:
        pass
    else:
        raise PublisherPolicyError(f"{field} must not be an IP address")
    if domain in {"localhost", "local"} or not DOMAIN_RE.fullmatch(domain):
        raise PublisherPolicyError(f"{field} is not a valid public DNS suffix")
    return domain


def domain_matches(host: str, domain: str) -> bool:
    normalized_host = host.lower().rstrip(".")
    normalized_domain = normalize_domain(domain, "publisher domain")
    return (
        normalized_host == normalized_domain
        or normalized_host.endswith(f".{normalized_domain}")
    )


def publisher_owns_url(publisher: dict[str, Any], target: str) -> bool:
    if not isinstance(target, str) or len(target) > 2048:
        return False
    parsed = urlparse(target)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False
    domains = publisher.get("domains")
    if not isinstance(domains, list):
        return False
    return any(domain_matches(parsed.hostname, domain) for domain in domains)


def load_policy(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PublisherPolicyError(f"cannot read publisher policy: {exc}") from exc
    validate_policy(value)
    return value


def policy_digest(value: dict[str, Any]) -> str:
    governed = {
        "challenge_solver": value.get("challenge_solver"),
        "session_handoff": value.get("session_handoff"),
        "publishers": value.get("publishers"),
    }
    rendered = json.dumps(
        governed,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def validate_policy(value: Any) -> None:
    if not isinstance(value, dict) or value.get("schema") != POLICY_SCHEMA:
        raise PublisherPolicyError(f"policy schema must be {POLICY_SCHEMA}")

    review = value.get("review")
    if not isinstance(review, dict) or review.get("status") != "approved":
        raise PublisherPolicyError("publisher policy must have an approved review")
    _required_string(review.get("reviewed_by"), "review.reviewed_by")
    reviewed_at = _required_string(review.get("reviewed_at"), "review.reviewed_at")
    try:
        date.fromisoformat(reviewed_at)
    except ValueError as exc:
        raise PublisherPolicyError("review.reviewed_at must be an ISO date") from exc
    _required_string(review.get("scope"), "review.scope")

    solver = value.get("challenge_solver")
    if not isinstance(solver, dict) or not isinstance(solver.get("enabled"), bool):
        raise PublisherPolicyError("challenge_solver.enabled must be boolean")
    if solver.get("provider") != "flaresolverr":
        raise PublisherPolicyError("challenge_solver.provider must be flaresolverr")
    if solver.get("result_use") != "same_profile_handoff_only":
        raise PublisherPolicyError(
            "challenge_solver.result_use must be same_profile_handoff_only"
        )

    handoff = value.get("session_handoff")
    if not isinstance(handoff, dict):
        raise PublisherPolicyError("session_handoff must be an object")
    if handoff.get("source") != "flaresolverr":
        raise PublisherPolicyError("session_handoff.source must be flaresolverr")
    if handoff.get("destination") != "same_profile":
        raise PublisherPolicyError(
            "session_handoff.destination must be same_profile"
        )
    if handoff.get("scope") != "same_publisher":
        raise PublisherPolicyError("session_handoff.scope must be same_publisher")
    if handoff.get("browser_version_match") != "required":
        raise PublisherPolicyError(
            "session_handoff.browser_version_match must be required"
        )
    if handoff.get("persistence") != "memory_only":
        raise PublisherPolicyError(
            "session_handoff.persistence must be memory_only"
        )
    if handoff.get("cookie_export") != "deny":
        raise PublisherPolicyError("session_handoff.cookie_export must be deny")
    if not isinstance(handoff.get("enabled"), bool):
        raise PublisherPolicyError("session_handoff.enabled must be boolean")
    if not handoff["enabled"]:
        raise PublisherPolicyError(
            "same-profile solver session handoff must remain enabled"
        )

    publishers = value.get("publishers")
    if not isinstance(publishers, list) or not publishers:
        raise PublisherPolicyError("publishers must be a non-empty array")
    seen_ids: set[str] = set()
    seen_domains: set[str] = set()
    enabled_count = 0
    for index, publisher in enumerate(publishers):
        if not isinstance(publisher, dict):
            raise PublisherPolicyError(f"publishers[{index}] must be an object")
        publisher_id = _required_string(
            publisher.get("id"), f"publishers[{index}].id"
        )
        if not PUBLISHER_ID_RE.fullmatch(publisher_id):
            raise PublisherPolicyError(f"publishers[{index}].id is invalid")
        if publisher_id in seen_ids:
            raise PublisherPolicyError(f"duplicate publisher id: {publisher_id}")
        seen_ids.add(publisher_id)
        _required_string(
            publisher.get("display_name"), f"publishers[{index}].display_name"
        )
        if not isinstance(publisher.get("enabled"), bool):
            raise PublisherPolicyError(
                f"publishers[{index}].enabled must be boolean"
            )
        if publisher["enabled"]:
            enabled_count += 1
        for field in ("challenge_solver", "pdf_fetch"):
            if not isinstance(publisher.get(field), bool):
                raise PublisherPolicyError(
                    f"publishers[{index}].{field} must be boolean"
                )
        domains = publisher.get("domains")
        if not isinstance(domains, list) or not domains:
            raise PublisherPolicyError(
                f"publishers[{index}].domains must be a non-empty array"
            )
        for domain_index, raw_domain in enumerate(domains):
            domain = normalize_domain(
                raw_domain,
                f"publishers[{index}].domains[{domain_index}]",
            )
            if domain in seen_domains:
                raise PublisherPolicyError(f"duplicate publisher domain: {domain}")
            seen_domains.add(domain)
    if enabled_count == 0:
        raise PublisherPolicyError("at least one publisher must be enabled")
    approved_digest = _required_string(
        review.get("approved_policy_sha256"),
        "review.approved_policy_sha256",
    )
    if not re.fullmatch(r"[0-9a-f]{64}", approved_digest):
        raise PublisherPolicyError(
            "review.approved_policy_sha256 must be a lowercase SHA-256"
        )
    if approved_digest != policy_digest(value):
        raise PublisherPolicyError(
            "publisher policy changed after review; run the explicit approval command"
        )


def _unique_domains(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            result.append(value)
            seen.add(value)
    return result


def compile_browser_environment(policy: dict[str, Any]) -> dict[str, str]:
    validate_policy(policy)
    enabled = [item for item in policy["publishers"] if item["enabled"]]
    allowed = _unique_domains(
        [normalize_domain(domain, "publisher domain") for item in enabled for domain in item["domains"]]
    )
    solver_domains = _unique_domains(
        [
            normalize_domain(domain, "challenge solver domain")
            for item in enabled
            if item["challenge_solver"]
            for domain in item["domains"]
        ]
    )
    pdf_domains = _unique_domains(
        [
            normalize_domain(domain, "PDF fetch domain")
            for item in enabled
            if item["pdf_fetch"]
            for domain in item["domains"]
        ]
    )
    return {
        "LITERATURE_BROWSER_ALLOWED_DOMAINS": ",".join(allowed),
        "LITERATURE_BROWSER_PDF_FETCH_DOMAINS": ",".join(pdf_domains),
        "LITERATURE_FLARESOLVERR_ALLOWED_DOMAINS": ",".join(solver_domains),
        "LITERATURE_SESSION_HANDOFF_ENABLED": (
            "true" if policy["session_handoff"]["enabled"] else "false"
        ),
        "LITERATURE_SESSION_HANDOFF_DESTINATION": policy["session_handoff"][
            "destination"
        ],
        "LITERATURE_SESSION_HANDOFF_SCOPE": policy["session_handoff"]["scope"],
        "LITERATURE_SESSION_HANDOFF_BROWSER_VERSION_MATCH": policy[
            "session_handoff"
        ]["browser_version_match"],
    }


def publisher_by_id(policy: dict[str, Any], publisher_id: str) -> dict[str, Any]:
    for publisher in policy["publishers"]:
        if publisher["id"] == publisher_id and publisher["enabled"]:
            return publisher
    raise PublisherPolicyError(f"publisher is not enabled: {publisher_id}")


def public_policy(policy: dict[str, Any]) -> dict[str, Any]:
    validate_policy(policy)
    return {
        "schema": policy["schema"],
        "review": dict(policy["review"]),
        "challenge_solver": {
            "enabled": policy["challenge_solver"]["enabled"],
            "provider": policy["challenge_solver"]["provider"],
        },
        "session_handoff": dict(policy["session_handoff"]),
        "publishers": [dict(item) for item in policy["publishers"]],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a literature publisher policy")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--approve", action="store_true")
    parser.add_argument("--reviewed-by")
    args = parser.parse_args(argv)
    if args.approve:
        reviewer = _required_string(args.reviewed_by, "--reviewed-by")
        try:
            draft = json.loads(args.config.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PublisherPolicyError(f"cannot read publisher policy: {exc}") from exc
        if not isinstance(draft, dict) or not isinstance(draft.get("review"), dict):
            raise PublisherPolicyError("policy requires a review object")
        draft["review"].update(
            {
                "status": "approved",
                "reviewed_by": reviewer,
                "reviewed_at": date.today().isoformat(),
                "approved_policy_sha256": policy_digest(draft),
            }
        )
        validate_policy(draft)
        temporary = args.config.with_name(f".{args.config.name}.tmp")
        temporary.write_text(
            json.dumps(draft, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(args.config)
    policy = load_policy(args.config)
    compiled = compile_browser_environment(policy)
    print(
        json.dumps(
            {
                "ok": True,
                "schema": policy["schema"],
                "review": policy["review"],
                "enabled_publishers": sum(
                    bool(item["enabled"]) for item in policy["publishers"]
                ),
                "allowed_domains": len(
                    compiled["LITERATURE_BROWSER_ALLOWED_DOMAINS"].split(",")
                ),
                "challenge_solver_domains": len(
                    compiled["LITERATURE_FLARESOLVERR_ALLOWED_DOMAINS"].split(",")
                ),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
