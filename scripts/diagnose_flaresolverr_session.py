#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse


def call(endpoint: str, payload: dict[str, object], timeout: float) -> dict[str, object]:
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            value = json.loads(exc.read().decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            value = {}
        value["http_error"] = exc.code
        return value


def summarize(result: dict[str, object], elapsed: float) -> dict[str, object]:
    solution = result.get("solution")
    solution = solution if isinstance(solution, dict) else {}
    final_url = str(solution.get("url") or "")
    return {
        "elapsed_seconds": round(elapsed, 3),
        "result_status": result.get("status"),
        "http_error": result.get("http_error"),
        "message": str(result.get("message") or "")[:300],
        "publisher_host": urlparse(final_url).hostname or "",
        "publisher_status": solution.get("status"),
        "response_chars": len(str(solution.get("response") or "")),
        "cookie_count": len(solution.get("cookies") or []),
        "user_agent_present": bool(str(solution.get("userAgent") or "").strip()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe one persistent FlareSolverr session without exposing session state"
    )
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--session", required=True)
    parser.add_argument("--attempts", type=int, default=2)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--wait-seconds", type=int, default=3)
    parser.add_argument("--tabs-till-verify", type=int)
    parser.add_argument("--proxy-url")
    parser.add_argument("--preserve-session", action="store_true")
    args = parser.parse_args()

    create_payload: dict[str, object] = {
        "cmd": "sessions.create",
        "session": args.session,
    }
    if args.proxy_url:
        create_payload["proxy"] = {"url": args.proxy_url}
    created = call(
        args.endpoint,
        create_payload,
        30,
    )
    if created.get("status") != "ok" and "already exists" not in str(
        created.get("message") or ""
    ).casefold():
        print(json.dumps({"session_created": False, "message": created.get("message")}))
        return 2

    outcomes: list[dict[str, object]] = []
    try:
        for attempt in range(1, max(1, args.attempts) + 1):
            started = time.monotonic()
            payload: dict[str, object] = {
                "cmd": "request.get",
                "url": args.url,
                "session": args.session,
                "maxTimeout": args.timeout_seconds * 1000,
                "waitInSeconds": args.wait_seconds,
                "disableMedia": False,
            }
            if args.tabs_till_verify is not None:
                payload["tabs_till_verify"] = max(0, args.tabs_till_verify)
            result = call(
                args.endpoint,
                payload,
                args.timeout_seconds + 20,
            )
            outcome = summarize(result, time.monotonic() - started)
            outcome["attempt"] = attempt
            outcomes.append(outcome)
            if (
                outcome["result_status"] == "ok"
                and isinstance(outcome["publisher_status"], int)
                and int(outcome["publisher_status"]) < 400
                and int(outcome["response_chars"]) >= 10_000
            ):
                break
    finally:
        if not args.preserve_session:
            call(
                args.endpoint,
                {"cmd": "sessions.destroy", "session": args.session},
                30,
            )

    print(json.dumps({"session_created": True, "attempts": outcomes}, indent=2))
    return 0 if outcomes and int(outcomes[-1]["response_chars"]) >= 10_000 else 1


if __name__ == "__main__":
    raise SystemExit(main())
