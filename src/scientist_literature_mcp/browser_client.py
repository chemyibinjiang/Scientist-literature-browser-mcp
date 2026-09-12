from __future__ import annotations

import json
import os
import re
from typing import Any
import urllib.error
import urllib.request
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit


DEFAULT_BROWSER_URL = "http://browser:9020"
ROUTES_ENV = "LITERATURE_BROWSER_ROUTES_JSON"


class LiteratureBrowserClientError(RuntimeError):
    pass


def _validated_base_url(value: Any, *, label: str) -> str:
    base_url = str(value or "").strip().rstrip("/")
    try:
        parsed = urlsplit(base_url)
    except ValueError as exc:
        raise LiteratureBrowserClientError(f"{label} has an invalid base URL") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise LiteratureBrowserClientError(
            f"{label} base URL must use http or https"
        )
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise LiteratureBrowserClientError(
            f"{label} base URL must not contain credentials, query, or fragment"
        )
    return base_url


def _normalized_host_suffix(value: Any) -> str:
    suffix = str(value or "").strip().lower().rstrip(".")
    if suffix.startswith("*."):
        suffix = suffix[2:]
    if not suffix or not re.fullmatch(r"[a-z0-9.-]+", suffix):
        raise LiteratureBrowserClientError("browser route contains an invalid host suffix")
    return suffix


def _host_matches_suffix(host: str, suffix: str) -> bool:
    normalized = host.lower().rstrip(".")
    return normalized == suffix or normalized.endswith(f".{suffix}")


def parse_browser_routes(raw: str | None) -> tuple[dict[str, Any], ...]:
    if not raw or not raw.strip():
        return ()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise LiteratureBrowserClientError(
            f"{ROUTES_ENV} must contain valid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise LiteratureBrowserClientError(f"{ROUTES_ENV} must contain an object")
    routes: list[dict[str, Any]] = []
    for raw_id, raw_config in value.items():
        route_id = str(raw_id or "").strip().lower()
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", route_id):
            raise LiteratureBrowserClientError("browser route has an invalid identifier")
        if not isinstance(raw_config, dict):
            raise LiteratureBrowserClientError(
                f"browser route {route_id} must contain an object"
            )
        raw_suffixes = raw_config.get("host_suffixes")
        if not isinstance(raw_suffixes, list) or not raw_suffixes:
            raise LiteratureBrowserClientError(
                f"browser route {route_id} requires host_suffixes"
            )
        routes.append(
            {
                "id": route_id,
                "base_url": _validated_base_url(
                    raw_config.get("base_url"), label=f"browser route {route_id}"
                ),
                "host_suffixes": tuple(
                    dict.fromkeys(_normalized_host_suffix(item) for item in raw_suffixes)
                ),
            }
        )
    return tuple(routes)


def infer_resource_kind(url: str) -> str:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return "article"
    host = (parsed.hostname or "").lower().rstrip(".")
    target = unquote(f"{parsed.path}?{parsed.query}").lower()
    if host in {"acs.figshare.com", "ndownloader.figshare.com"}:
        return "supplementary_pdf"
    markers = (
        "/action/downloadsupplement",
        "/article-supplement/",
        "/doi/suppl/",
        "/esm/",
        "/suppdata/",
        "/suppl_file/",
        "-mmc",
        "_moesm",
        "_si_",
        "_suppl",
        "supporting-information",
        "supporting_information",
    )
    return (
        "supplementary_pdf"
        if any(marker in target for marker in markers)
        else "article"
    )


def _compact_error(value: Any, limit: int = 600) -> str:
    return re.sub(r"\s+", " ", str(value)).strip()[:limit]


def _safe_metadata(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, list[str]] = {}
    forbidden = ("cookie", "token", "authorization", "user-agent", "user_agent")
    for raw_key, raw_values in value.items():
        key = str(raw_key)[:200]
        if any(marker in key.lower() for marker in forbidden):
            continue
        values = raw_values if isinstance(raw_values, list) else [raw_values]
        result[key] = [str(item)[:4000] for item in values[:20]]
    return result


def _safe_url(value: Any) -> str:
    raw = str(value or "")
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return ""
    sensitive = ("token", "signature", "credential", "authorization", "api_key")
    query = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not any(marker in key.lower() for marker in sensitive)
    ]
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), "")
    )


def _safe_figures(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    figures: list[dict[str, Any]] = []
    for item in value[:30]:
        if not isinstance(item, dict):
            continue
        figure: dict[str, Any] = {
            "src": _safe_url(item.get("src")),
            "alt": str(item.get("alt") or "")[:2000],
            "caption": str(item.get("caption") or "")[:8000],
            "kind": str(item.get("kind") or "figure")[:40],
            "extraction_method": str(item.get("extraction_method") or "")[:80],
            "image_extraction_method": str(
                item.get("image_extraction_method") or ""
            )[:80],
            "image_access_state": str(
                item.get("image_access_state") or ""
            )[:80],
        }
        try:
            figure["index"] = max(0, int(item.get("index") or 0))
        except (TypeError, ValueError):
            figure["index"] = 0
        image_base64 = str(item.get("image_base64") or "")
        if image_base64 and len(image_base64) <= 12 * 1024 * 1024:
            figure["image_base64"] = image_base64
            figure["mime_type"] = str(item.get("mime_type") or "image/png")[:100]
            try:
                figure["image_bytes"] = max(0, int(item.get("image_bytes") or 0))
            except (TypeError, ValueError):
                figure["image_bytes"] = 0
        if item.get("image_error"):
            figure["image_error"] = _compact_error(item["image_error"], 240)
        if item.get("ref"):
            figure["ref"] = str(item.get("ref") or "")[:80]
        if item.get("object_category"):
            figure["object_category"] = str(item.get("object_category") or "")[:80]
        for key in ("object_width", "object_height"):
            try:
                figure[key] = max(0, int(item.get(key) or 0))
            except (TypeError, ValueError):
                figure[key] = 0
        figures.append(figure)
    return figures


def _safe_figure_extraction(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, Any] = {}
    for key in ("total", "offset", "returned"):
        try:
            result[key] = max(0, int(value.get(key) or 0))
        except (TypeError, ValueError):
            result[key] = 0
    result["has_more"] = bool(value.get("has_more"))
    return result


def _safe_pdf_links(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    return [
        {
            "href": _safe_url(item.get("href")),
            "text": str(item.get("text") or "")[:2000],
            "source": str(item.get("source") or "")[:100],
        }
        for item in value[:30]
        if isinstance(item, dict)
    ]


def _safe_content_routes(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    routes: list[dict[str, str]] = []
    for item in value[:20]:
        if not isinstance(item, dict):
            continue
        route = {
            "kind": str(item.get("kind") or "")[:80],
            "route": str(item.get("route") or "")[:120],
            "state": str(item.get("state") or "")[:120],
        }
        if item.get("article_landing_url"):
            route["article_landing_url"] = _safe_url(item.get("article_landing_url"))
        routes.append(route)
    return routes


def _safe_profile_warm(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, Any] = {}
    for key in (
        "attempted",
        "success",
        "reload_attempted",
        "initial_full_text_dom",
        "final_full_text_dom",
    ):
        if key in value:
            result[key] = bool(value.get(key))
    for key in ("publisher", "initial_state", "final_state", "message"):
        if key in value:
            result[key] = str(value.get(key) or "")[:240]
    for key in ("final_text_chars", "elapsed_ms"):
        try:
            result[key] = max(0, int(value.get(key) or 0))
        except (TypeError, ValueError):
            result[key] = 0
    return result


def sanitize_read_result(value: dict[str, Any]) -> dict[str, Any]:
    challenge = value.get("challenge_bypass")
    challenge = challenge if isinstance(challenge, dict) else {}
    html_extraction = value.get("html_extraction")
    html_extraction = html_extraction if isinstance(html_extraction, dict) else {}
    pdf_extraction = value.get("pdf_extraction")
    pdf_extraction = pdf_extraction if isinstance(pdf_extraction, dict) else {}
    return {
        "success": bool(value.get("success")),
        "url": _safe_url(value.get("url")),
        "final_url": _safe_url(value.get("final_url")),
        "status": value.get("status"),
        "page_state": str(value.get("page_state") or ""),
        "title": str(value.get("title") or ""),
        "text": str(value.get("text") or ""),
        "text_truncated": bool(value.get("text_truncated")),
        "text_source": str(value.get("text_source") or ""),
        "html_text_chars": int(value.get("html_text_chars") or 0),
        "full_text_dom_visible": bool(value.get("full_text_dom_visible")),
        "access_state": str(value.get("access_state") or ""),
        "access_signals": list(value.get("access_signals") or [])[:30],
        "institution_markers": list(value.get("institution_markers") or [])[:20],
        "metadata": _safe_metadata(value.get("metadata")),
        "figures": _safe_figures(value.get("figures")),
        "figure_extraction": _safe_figure_extraction(
            value.get("figure_extraction")
        ),
        "pdf_links": _safe_pdf_links(value.get("pdf_links")),
        "content_routes": _safe_content_routes(value.get("content_routes")),
        "references": list(value.get("references") or []),
        "challenge_bypass": {
            key: challenge[key]
            for key in ("attempted", "success", "solver_status", "html_full_text", "message")
            if key in challenge
        },
        "profile_warm": _safe_profile_warm(value.get("profile_warm")),
        "html_extraction": {
            key: html_extraction[key]
            for key in ("attempted", "success", "selector", "text_chars", "message")
            if key in html_extraction
        },
        "pdf_extraction": {
            key: (
                _safe_url(pdf_extraction[key])
                if key == "source_url"
                else pdf_extraction[key]
            )
            for key in (
                "attempted",
                "success",
                "source_url",
                "pages",
                "text_chars",
                "bytes",
                "content_type",
                "status",
                "strategy",
                "message",
                "attempts",
            )
            if key in pdf_extraction
        },
    }


def compact_read_result(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in sanitize_read_result(value).items()
        if key
        not in {
            "text",
            "metadata",
            "figures",
            "figure_extraction",
            "pdf_links",
            "content_routes",
            "references",
            "html_extraction",
            "pdf_extraction",
        }
    }


class BrowserClient:
    def __init__(
        self,
        base_url: str | None = None,
        *,
        routes: tuple[dict[str, Any], ...] | None = None,
    ) -> None:
        self.base_url = _validated_base_url(
            base_url or os.environ.get("LITERATURE_BROWSER_URL") or DEFAULT_BROWSER_URL,
            label="primary literature browser",
        )
        self.routes = (
            routes
            if routes is not None
            else parse_browser_routes(os.environ.get(ROUTES_ENV))
        )

    def route_for(
        self,
        url: str,
        *,
        affinity_key: str = "",
        landing_url_hint: str = "",
    ) -> dict[str, Any] | None:
        hosts: list[str] = []
        for candidate in (url, landing_url_hint):
            try:
                host = (urlsplit(candidate).hostname or "").lower().rstrip(".")
            except ValueError:
                host = ""
            if host:
                hosts.append(host)
        for route in self.routes:
            if affinity_key and affinity_key == route["id"]:
                return route
            if any(
                _host_matches_suffix(host, suffix)
                for host in hosts
                for suffix in route["host_suffixes"]
            ):
                return route
        return None

    def request_json(
        self,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        timeout_seconds: int = 600,
        base_url: str | None = None,
        route_id: str = "primary",
    ) -> dict[str, Any]:
        body = None
        headers: dict[str, str] = {}
        method = "GET"
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
            method = "POST"
        request = urllib.request.Request(
            f"{base_url or self.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise LiteratureBrowserClientError(
                f"literature browser rejected the request ({exc.code}): "
                f"{_compact_error(detail)}"
            ) from exc
        except urllib.error.URLError as exc:
            label = (
                "literature browser"
                if route_id == "primary"
                else f"{route_id} literature browser route"
            )
            raise LiteratureBrowserClientError(
                f"{label} is unavailable: {_compact_error(exc.reason)}"
            ) from exc
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise LiteratureBrowserClientError(
                "literature browser returned invalid JSON"
            ) from exc
        if not isinstance(value, dict):
            raise LiteratureBrowserClientError(
                "literature browser returned a non-object response"
            )
        if value.get("success") is False:
            raise LiteratureBrowserClientError(
                _compact_error(value.get("message") or "literature read failed")
            )
        return value

    def ready(self) -> dict[str, Any]:
        value = self.request_json("/ready", timeout_seconds=15)
        result: dict[str, Any] = {
            "ready": bool(value.get("ready")),
            "service": str(value.get("service") or "literature-browser"),
            "allowed_domain_count": int(value.get("allowed_domain_count") or 0),
        }
        route_status: dict[str, dict[str, Any]] = {}
        upstream_routes = value.get("routes")
        if isinstance(upstream_routes, dict):
            for raw_id, raw_status in upstream_routes.items():
                if not isinstance(raw_status, dict):
                    continue
                route_id = str(raw_id)[:64]
                route_status[route_id] = {
                    "ready": bool(raw_status.get("ready")),
                    "service": str(
                        raw_status.get("service") or "literature-browser-route"
                    )[:120],
                    "allowed_domain_count": int(
                        raw_status.get("allowed_domain_count") or 0
                    ),
                    "host_suffixes": [
                        str(item)[:200]
                        for item in (raw_status.get("host_suffixes") or [])[:20]
                    ],
                }
                if raw_status.get("error"):
                    route_status[route_id]["error"] = _compact_error(
                        raw_status["error"], 240
                    )
        for route in self.routes:
            try:
                route_value = self.request_json(
                    "/ready",
                    timeout_seconds=15,
                    base_url=route["base_url"],
                    route_id=route["id"],
                )
                route_status[route["id"]] = {
                    "ready": bool(route_value.get("ready")),
                    "service": str(
                        route_value.get("service") or "literature-browser"
                    ),
                    "allowed_domain_count": int(
                        route_value.get("allowed_domain_count") or 0
                    ),
                    "host_suffixes": list(route["host_suffixes"]),
                }
            except LiteratureBrowserClientError as exc:
                route_status[route["id"]] = {
                    "ready": False,
                    "service": "literature-browser-route",
                    "host_suffixes": list(route["host_suffixes"]),
                    "error": _compact_error(exc, 240),
                }
        if route_status:
            result["routes"] = route_status
            result["configured_route_count"] = len(route_status)
        return result

    def read(
        self,
        url: str,
        *,
        wait_ms: int = 5000,
        max_chars: int = 0,
        include_figure_images: bool = False,
        max_figures: int = 30,
        figure_offset: int = 0,
        timeout_seconds: int = 600,
        affinity_key: str = "",
        resource_kind: str | None = None,
        landing_url_hint: str = "",
    ) -> dict[str, Any]:
        if not 0 <= wait_ms <= 20000:
            raise LiteratureBrowserClientError("wait_ms must be between 0 and 20000")
        if max_chars != 0 and not 1000 <= max_chars <= 2_000_000:
            raise LiteratureBrowserClientError(
                "max_chars must be 0 for full text or between 1000 and 2000000"
            )
        if not isinstance(include_figure_images, bool):
            raise LiteratureBrowserClientError(
                "include_figure_images must be a boolean"
            )
        if not 1 <= max_figures <= 30:
            raise LiteratureBrowserClientError(
                "max_figures must be between 1 and 30"
            )
        if not 0 <= figure_offset <= 1000:
            raise LiteratureBrowserClientError(
                "figure_offset must be between 0 and 1000"
            )
        if not 45 <= timeout_seconds <= 900:
            raise LiteratureBrowserClientError(
                "timeout_seconds must be between 45 and 900"
            )
        if affinity_key and not re.fullmatch(
            r"[a-z0-9][a-z0-9-]{0,63}", affinity_key
        ):
            raise LiteratureBrowserClientError(
                "affinity_key must be a lowercase publisher identifier"
            )
        effective_resource_kind = resource_kind or infer_resource_kind(url)
        if effective_resource_kind not in {"article", "supplementary_pdf"}:
            raise LiteratureBrowserClientError(
                "resource_kind must be article or supplementary_pdf"
            )
        route = self.route_for(
            url,
            affinity_key=affinity_key,
            landing_url_hint=landing_url_hint,
        )
        route_options = (
            {"base_url": route["base_url"], "route_id": route["id"]}
            if route
            else {}
        )
        value = self.request_json(
            "/v1/literature/read",
            payload={
                "url": url,
                "wait_ms": wait_ms,
                "max_chars": max_chars,
                "include_figure_images": include_figure_images,
                "max_figures": max_figures,
                "figure_offset": figure_offset,
                "timeout_seconds": timeout_seconds,
                "affinity_key": affinity_key,
                "resource_kind": effective_resource_kind,
                "landing_url_hint": landing_url_hint,
            },
            timeout_seconds=timeout_seconds + 30,
            **route_options,
        )
        return sanitize_read_result(value)
