from __future__ import annotations

import json
from pathlib import Path
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from scientist_literature_mcp import __version__  # noqa: E402
from scientist_literature_mcp.api_keys import new_token, token_hash  # noqa: E402
from scientist_literature_mcp.publisher_policy import (  # noqa: E402
    validate_policy,
)


PRIVATE_TOPOLOGY_MARKERS = (
    ".".join(("10", "24", "11", "82")),
    ".".join(("59", "77", "33", "211")),
    ".".join(("192", "168", "1", "56")),
    "Scientist-RSC-" + "Aliya",
)


def test_version_matches_project_metadata() -> None:
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert __version__ == metadata["project"]["version"]


def test_publisher_policy_is_safe_or_requires_fresh_review() -> None:
    policy = json.loads(
        (ROOT / "config" / "publishers.json").read_text(encoding="utf-8")
    )
    assert policy["session_handoff"]["cookie_export"] == "deny"
    assert policy["session_handoff"]["persistence"] == "memory_only"
    if policy["review"]["status"] == "approved":
        validate_policy(policy)
    else:
        assert policy["review"]["status"] == "review_required"
        assert policy["review"]["reviewed_by"] == ""
        assert policy["review"]["approved_policy_sha256"] == ""


def test_documented_gateway_defaults_are_generic() -> None:
    paths = (
        ROOT / ".env.example",
        ROOT / "AGENT_USAGE.md",
        ROOT / "README.md",
        ROOT / "codex.remote.config.example.toml",
        ROOT / "docker-compose.yml",
        ROOT / "src" / "scientist_literature_mcp" / "http_gateway.py",
    )
    payload = "\n".join(path.read_text(encoding="utf-8") for path in paths)
    assert "scientist.example.edu" in payload
    for marker in PRIVATE_TOPOLOGY_MARKERS:
        assert marker not in payload


def test_browser_and_solver_share_one_immutable_chromium_base() -> None:
    browser = (ROOT / "Dockerfile.browser").read_text(encoding="utf-8")
    engine = (ROOT / "Dockerfile.engine").read_text(encoding="utf-8")
    solver = (ROOT / "Dockerfile.flaresolverr").read_text(encoding="utf-8")
    digest = (
        "ghcr.io/flaresolverr/flaresolverr@sha256:"
        "139dfee1c6f89249c8d665d1333a42e8ec74ec0a86bc6bb1c8461e10d3a66a47"
    )
    assert f"ARG CHROMIUM_IMAGE={digest}" in browser
    assert f"ARG CHROMIUM_IMAGE={digest}" in engine
    assert f"FROM {digest}" in solver


def test_api_key_helpers_store_only_a_digest() -> None:
    token = new_token("student-smoke")
    digest = token_hash(token)
    assert token.startswith("srg_student-smoke_")
    assert digest.startswith("sha256$")
    assert token not in digest


def test_runtime_state_is_not_part_of_the_checkout() -> None:
    assert not (ROOT / ".env").exists()
    state_files = [
        path
        for path in (ROOT / "state").rglob("*")
        if path.is_file() and path.name != ".gitkeep"
    ]
    assert state_files == []
