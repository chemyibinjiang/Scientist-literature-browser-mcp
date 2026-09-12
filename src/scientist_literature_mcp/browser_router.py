from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
import urllib.error
import urllib.request
from urllib.parse import urlparse

from .browser_client import BrowserClient


HOST = os.environ.get("LITERATURE_ROUTER_HOST", "127.0.0.1")
PORT = int(os.environ.get("LITERATURE_ROUTER_PORT", "19034"))
MAX_BODY_BYTES = 65536


def json_response(
    handler: BaseHTTPRequestHandler,
    status: int,
    payload: dict[str, Any],
) -> None:
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


class PublisherBrowserRouter:
    def __init__(self, client: BrowserClient | None = None) -> None:
        self.client = client or BrowserClient()

    def route_for(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        return self.client.route_for(
            str(payload.get("url") or ""),
            affinity_key=str(payload.get("affinity_key") or ""),
            landing_url_hint=str(payload.get("landing_url_hint") or ""),
        )

    @staticmethod
    def _request(
        base_url: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        timeout_seconds: int,
    ) -> tuple[int, dict[str, Any]]:
        body = None
        headers: dict[str, str] = {}
        method = "GET"
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
            method = "POST"
        request = urllib.request.Request(
            f"{base_url.rstrip('/')}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
                return response.status, json.loads(raw)
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                value = json.loads(raw)
            except json.JSONDecodeError:
                value = {"success": False, "message": raw[:600]}
            return exc.code, value

    def ready(self) -> tuple[int, dict[str, Any]]:
        try:
            primary_status, primary = self._request(
                self.client.base_url,
                "/ready",
                timeout_seconds=15,
            )
            primary_ready = primary_status == 200 and bool(primary.get("ready"))
        except (OSError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            primary_ready = False
            primary = {
                "ready": False,
                "service": "literature-browser",
                "error": f"{type(exc).__name__}: {str(exc)[:240]}",
            }
        routes: dict[str, dict[str, Any]] = {}
        for route in self.client.routes:
            try:
                status, value = self._request(
                    route["base_url"],
                    "/ready",
                    timeout_seconds=15,
                )
                routes[route["id"]] = {
                    "ready": status == 200 and bool(value.get("ready")),
                    "service": str(value.get("service") or "literature-browser"),
                    "allowed_domain_count": int(
                        value.get("allowed_domain_count") or 0
                    ),
                    "host_suffixes": list(route["host_suffixes"]),
                }
            except (OSError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
                routes[route["id"]] = {
                    "ready": False,
                    "service": "literature-browser-route",
                    "host_suffixes": list(route["host_suffixes"]),
                    "error": f"{type(exc).__name__}: {str(exc)[:240]}",
                }
        result = {
            "ready": primary_ready,
            "service": "literature-browser-router",
            "allowed_domain_count": int(primary.get("allowed_domain_count") or 0),
            "primary": {
                "ready": primary_ready,
                "service": str(primary.get("service") or "literature-browser"),
            },
            "routes": routes,
        }
        return (200 if primary_ready else 503), result

    def read(self, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        url = str(payload.get("url") or "").strip()
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            return 400, {"success": False, "message": "url is required"}
        try:
            timeout_seconds = max(
                45,
                min(int(payload.get("timeout_seconds") or 600), 900),
            )
        except (TypeError, ValueError):
            return 400, {"success": False, "message": "timeout_seconds is invalid"}
        route = self.route_for(payload)
        base_url = route["base_url"] if route else self.client.base_url
        route_id = route["id"] if route else "primary"
        try:
            return self._request(
                base_url,
                "/v1/literature/read",
                payload=payload,
                timeout_seconds=timeout_seconds + 30,
            )
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            label = (
                "literature browser"
                if route_id == "primary"
                else f"{route_id} literature browser route"
            )
            return 502, {
                "success": False,
                "message": f"{label} is unavailable: {type(exc).__name__}",
                "route_id": route_id,
            }


ROUTER = PublisherBrowserRouter()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        if urlparse(self.path).path != "/ready":
            json_response(self, 404, {"success": False, "message": "not found"})
            return
        status, value = ROUTER.ready()
        json_response(self, status, value)

    def do_POST(self) -> None:
        if urlparse(self.path).path != "/v1/literature/read":
            json_response(self, 404, {"success": False, "message": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0 or length > MAX_BODY_BYTES:
                raise ValueError("invalid request body size")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("request body must contain an object")
        except (ValueError, json.JSONDecodeError) as exc:
            json_response(self, 400, {"success": False, "message": str(exc)})
            return
        status, value = ROUTER.read(payload)
        json_response(self, status, value)


def main() -> None:
    try:
        server = ThreadingHTTPServer((HOST, PORT), Handler)
    except OSError as exc:
        raise SystemExit(f"could not bind literature browser router: {exc}") from exc
    print(
        f"[literature-browser-router] listen={HOST}:{PORT} "
        f"routes={len(ROUTER.client.routes)}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
