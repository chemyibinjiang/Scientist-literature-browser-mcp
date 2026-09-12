from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import stat
import tempfile
import threading
import time
from datetime import datetime, timezone
from typing import Any


KEY_SCHEMA = "scientist-research-gateway-keys/v1"
KEY_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
TOKEN_RE = re.compile(r"^srg_[A-Za-z0-9._-]+_[A-Za-z0-9_-]{32,}$")
HASH_RE = re.compile(r"^sha256\$[a-f0-9]{64}$")
SCOPES = {
    "literature.publishers",
    "literature.read",
    "literature.health",
    "literature.probe",
    "literature.session_refresh",
}
DEFAULT_STUDENT_SCOPES = (
    "literature.publishers",
    "literature.read",
    "literature.health",
)
DEFAULT_CONCURRENT_REQUESTS = 8
MAX_CONCURRENT_REQUESTS = 24


class ApiKeyConfigError(ValueError):
    pass


class ApiKeyAuthenticationError(ValueError):
    pass


class ApiKeyAuthorizationError(ValueError):
    pass


class ApiKeyRateLimitError(ValueError):
    pass


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiKeyConfigError(f"{field} must be an ISO 8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ApiKeyConfigError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def empty_config() -> dict[str, Any]:
    return {
        "schema": KEY_SCHEMA,
        "keys": [],
        "audit": {"enabled": True, "retain_days": 365},
    }


def validate_config(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("schema") != KEY_SCHEMA:
        raise ApiKeyConfigError(f"API key configuration schema must be {KEY_SCHEMA}")
    if set(value) != {"schema", "keys", "audit"}:
        raise ApiKeyConfigError("API key configuration contains unsupported fields")
    keys = value.get("keys")
    if not isinstance(keys, list):
        raise ApiKeyConfigError("keys must be an array")
    seen: set[str] = set()
    for index, item in enumerate(keys):
        field = f"keys[{index}]"
        if not isinstance(item, dict):
            raise ApiKeyConfigError(f"{field} must be an object")
        required = {
            "id",
            "display_name",
            "secret_hash",
            "status",
            "scopes",
            "limits",
            "created_at",
        }
        optional = {"expires_at"}
        if required - set(item) or set(item) - required - optional:
            raise ApiKeyConfigError(f"{field} has missing or unsupported fields")
        key_id = item["id"]
        if not isinstance(key_id, str) or not KEY_ID_RE.fullmatch(key_id):
            raise ApiKeyConfigError(f"{field}.id is invalid")
        if key_id in seen:
            raise ApiKeyConfigError(f"duplicate key id: {key_id}")
        seen.add(key_id)
        if not isinstance(item["display_name"], str) or not item["display_name"].strip():
            raise ApiKeyConfigError(f"{field}.display_name must be non-empty")
        if not isinstance(item["secret_hash"], str) or not HASH_RE.fullmatch(item["secret_hash"]):
            raise ApiKeyConfigError(f"{field}.secret_hash is invalid")
        if item["status"] not in {"active", "disabled"}:
            raise ApiKeyConfigError(f"{field}.status must be active or disabled")
        scopes = item["scopes"]
        if not isinstance(scopes, list) or not scopes or len(scopes) != len(set(scopes)):
            raise ApiKeyConfigError(f"{field}.scopes must be a non-empty unique array")
        if any(scope not in SCOPES for scope in scopes):
            raise ApiKeyConfigError(f"{field}.scopes contains an unsupported scope")
        limits = item["limits"]
        if not isinstance(limits, dict) or set(limits) != {
            "requests_per_minute",
            "concurrent_requests",
        }:
            raise ApiKeyConfigError(f"{field}.limits is invalid")
        rpm = limits["requests_per_minute"]
        concurrent = limits["concurrent_requests"]
        if not isinstance(rpm, int) or not 1 <= rpm <= 600:
            raise ApiKeyConfigError(f"{field}.limits.requests_per_minute is invalid")
        if (
            not isinstance(concurrent, int)
            or not 1 <= concurrent <= MAX_CONCURRENT_REQUESTS
        ):
            raise ApiKeyConfigError(f"{field}.limits.concurrent_requests is invalid")
        created = _timestamp(item["created_at"], f"{field}.created_at")
        if item.get("expires_at") and _timestamp(
            item["expires_at"], f"{field}.expires_at"
        ) <= created:
            raise ApiKeyConfigError(f"{field}.expires_at must be after created_at")
    audit = value.get("audit")
    if not isinstance(audit, dict) or set(audit) != {"enabled", "retain_days"}:
        raise ApiKeyConfigError("audit is invalid")
    if not isinstance(audit["enabled"], bool):
        raise ApiKeyConfigError("audit.enabled must be a boolean")
    if not isinstance(audit["retain_days"], int) or not 30 <= audit["retain_days"] <= 3650:
        raise ApiKeyConfigError("audit.retain_days must be between 30 and 3650")
    return value


def load_config(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ApiKeyConfigError(f"cannot read API key configuration: {path}") from exc
    return validate_config(value)


def write_config(path: Path, value: dict[str, Any]) -> None:
    validate_config(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = path.stat()
    except FileNotFoundError:
        existing = None
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if os.name != "nt":
            os.chmod(temporary, stat.S_IMODE(existing.st_mode) if existing else 0o600)
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def token_hash(token: str) -> str:
    return "sha256$" + hashlib.sha256(token.encode("ascii")).hexdigest()


def new_token(key_id: str) -> str:
    if not KEY_ID_RE.fullmatch(key_id):
        raise ApiKeyConfigError("key id is invalid")
    return f"srg_{key_id}_{secrets.token_urlsafe(32)}"


def sanitized_keys(value: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {key: item[key] for key in item if key != "secret_hash"}
        for item in value["keys"]
    ]


class ApiKeyStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self._stamp: tuple[int, int] | None = None
        self._config: dict[str, Any] | None = None

    def config(self) -> dict[str, Any]:
        stat_result = self.path.stat()
        stamp = (stat_result.st_mtime_ns, stat_result.st_size)
        with self._lock:
            if self._config is None or self._stamp != stamp:
                self._config = load_config(self.path)
                self._stamp = stamp
            return self._config

    def authenticate(
        self,
        authorization: str | None,
        *,
        required_scope: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if not authorization or not authorization.lower().startswith("bearer "):
            raise ApiKeyAuthenticationError("Bearer API key required")
        token = authorization[7:].strip()
        if not TOKEN_RE.fullmatch(token):
            raise ApiKeyAuthenticationError("invalid API key")
        candidate = token_hash(token)
        item = None
        for entry in self.config()["keys"]:
            prefix_matches = token.startswith(f"srg_{entry['id']}_")
            hash_matches = hmac.compare_digest(candidate, entry["secret_hash"])
            if prefix_matches and hash_matches:
                item = entry
        if item is None:
            raise ApiKeyAuthenticationError("invalid API key")
        current = (now or _utc_now()).astimezone(timezone.utc)
        if item["status"] != "active":
            raise ApiKeyAuthenticationError("API key is disabled")
        if item.get("expires_at") and current >= _timestamp(item["expires_at"], "expires_at"):
            raise ApiKeyAuthenticationError("API key has expired")
        if required_scope and required_scope not in item["scopes"]:
            raise ApiKeyAuthorizationError(f"API key lacks scope: {required_scope}")
        return {key: value for key, value in item.items() if key != "secret_hash"}


class RequestLimiter:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._recent: dict[str, list[float]] = {}
        self._active: dict[str, int] = {}

    def begin(self, principal: dict[str, Any], *, now: float | None = None) -> int:
        key_id = str(principal["id"])
        limits = principal["limits"]
        moment = now if now is not None else time.monotonic()
        with self._lock:
            recent = [value for value in self._recent.get(key_id, []) if moment - value < 60]
            if len(recent) >= int(limits["requests_per_minute"]):
                self._recent[key_id] = recent
                raise ApiKeyRateLimitError("API key request rate exceeded")
            active = self._active.get(key_id, 0)
            if active >= int(limits["concurrent_requests"]):
                raise ApiKeyRateLimitError("API key concurrency exceeded")
            recent.append(moment)
            self._recent[key_id] = recent
            self._active[key_id] = active + 1
            return 1

    def end(self, principal: dict[str, Any]) -> None:
        key_id = str(principal["id"])
        with self._lock:
            active = self._active.get(key_id, 0)
            if active <= 1:
                self._active.pop(key_id, None)
            else:
                self._active[key_id] = active - 1


def _create_or_rotate(
    path: Path,
    *,
    key_id: str,
    display_name: str,
    scopes: list[str],
    requests_per_minute: int,
    concurrent_requests: int,
    expires_at: str | None,
    rotate: bool,
) -> str:
    value = load_config(path) if path.exists() else empty_config()
    existing = next((item for item in value["keys"] if item["id"] == key_id), None)
    if existing is not None and not rotate:
        raise ApiKeyConfigError(f"key already exists: {key_id}")
    token = new_token(key_id)
    record = {
        "id": key_id,
        "display_name": display_name,
        "secret_hash": token_hash(token),
        "status": "active",
        "scopes": sorted(set(scopes)),
        "limits": {
            "requests_per_minute": requests_per_minute,
            "concurrent_requests": concurrent_requests,
        },
        "created_at": _utc_now().isoformat(),
        **({"expires_at": expires_at} if expires_at else {}),
    }
    if existing is None:
        value["keys"].append(record)
    else:
        value["keys"][value["keys"].index(existing)] = record
    write_config(path, value)
    return token


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage Scientist Research Gateway API keys")
    parser.add_argument("--config", type=Path, required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("create", "rotate"):
        command = subparsers.add_parser(name)
        command.add_argument("--id", required=True)
        command.add_argument("--display-name", required=True)
        command.add_argument("--scope", action="append", choices=sorted(SCOPES))
        command.add_argument("--requests-per-minute", type=int, default=30)
        command.add_argument(
            "--concurrent-requests",
            type=int,
            default=DEFAULT_CONCURRENT_REQUESTS,
        )
        command.add_argument("--expires-at")
        command.add_argument("--secret-output", type=Path, required=True)
    revoke = subparsers.add_parser("revoke")
    revoke.add_argument("--id", required=True)
    subparsers.add_parser("list")
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command in {"create", "rotate"}:
        token = _create_or_rotate(
            args.config,
            key_id=args.id,
            display_name=args.display_name,
            scopes=args.scope or list(DEFAULT_STUDENT_SCOPES),
            requests_per_minute=args.requests_per_minute,
            concurrent_requests=args.concurrent_requests,
            expires_at=args.expires_at,
            rotate=args.command == "rotate",
        )
        args.secret_output.parent.mkdir(parents=True, exist_ok=True)
        args.secret_output.write_text(token + "\n", encoding="ascii")
        if os.name != "nt":
            args.secret_output.chmod(0o600)
        print(json.dumps({"created": args.id, "secret_output": str(args.secret_output)}))
        return
    value = load_config(args.config)
    if args.command == "revoke":
        item = next((entry for entry in value["keys"] if entry["id"] == args.id), None)
        if item is None:
            raise SystemExit(f"unknown key: {args.id}")
        item["status"] = "disabled"
        write_config(args.config, value)
        print(json.dumps({"revoked": args.id}))
        return
    print(json.dumps({"keys": sanitized_keys(value)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
