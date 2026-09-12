#!/usr/bin/env python3
from __future__ import annotations

import base64
import hashlib
import html
import io
import ipaddress
import json
import os
import queue
import re
import threading
import time
import urllib.error
import urllib.request
import uuid
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import (
    parse_qs,
    parse_qsl,
    quote,
    unquote,
    urlencode,
    urljoin,
    urlparse,
    urlunsplit,
)


HOST = os.environ.get("LITERATURE_BROWSER_HOST", "0.0.0.0")
PORT = int(os.environ.get("LITERATURE_BROWSER_PORT", "9020"))
PROFILE_DIR = Path(
    os.environ.get("LITERATURE_BROWSER_PROFILE", "/browser-profile/chromium")
)
PROFILE_ID = os.environ.get("LITERATURE_BROWSER_PROFILE_ID", "browser-single").strip()
SUPPLEMENT_FALLBACKS_PATH = Path(
    os.environ.get(
        "LITERATURE_SUPPLEMENT_FALLBACKS",
        "/config/supplementary-fallbacks.json",
    )
)
CHROMIUM_PATH = os.environ.get("LITERATURE_BROWSER_CHROMIUM", "/usr/bin/chromium")
CDP_URL = os.environ.get("LITERATURE_BROWSER_CDP_URL", "").strip()
HEADLESS = os.environ.get("LITERATURE_BROWSER_HEADLESS", "true").lower() == "true"
DISABLE_DEV_SHM_USAGE = (
    os.environ.get("LITERATURE_BROWSER_DISABLE_DEV_SHM_USAGE", "true").lower()
    == "true"
)
DEFAULT_MAX_CHARS = int(os.environ.get("LITERATURE_BROWSER_MAX_CHARS", "0"))
DEFAULT_WAIT_MS = int(os.environ.get("LITERATURE_BROWSER_WAIT_MS", "5000"))
MAX_WAIT_MS = 20000
MAX_QUEUED_READ_TIMEOUT_SECONDS = 1200


def bounded_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


MAX_OPEN_PAGES = bounded_int_env(
    "LITERATURE_BROWSER_MAX_OPEN_PAGES",
    2,
    2,
    8,
)
MAX_PENDING_READS = bounded_int_env(
    "LITERATURE_BROWSER_MAX_PENDING_READS",
    8,
    1,
    64,
)
ACTIVE_READ_STALL_GRACE_SECONDS = bounded_int_env(
    "LITERATURE_BROWSER_STALL_GRACE_SECONDS",
    30,
    10,
    300,
)
MAX_PDF_BYTES = int(
    os.environ.get("LITERATURE_BROWSER_MAX_PDF_BYTES", str(64 * 1024 * 1024))
)
MAX_PDF_CANDIDATES = 3
MAX_FIGURE_IMAGE_BYTES = 8 * 1024 * 1024
MAX_FIGURES_PER_READ = 30
READ_BUDGET_SECONDS = min(
    540,
    max(
        120,
        int(os.environ.get("LITERATURE_BROWSER_READ_BUDGET_SECONDS", "480")),
    ),
)
PDF_FETCH_TIMEOUT_MS = min(
    240000,
    max(
        10000,
        int(os.environ.get("LITERATURE_BROWSER_PDF_FETCH_TIMEOUT_MS", "240000")),
    ),
)
NAVIGATION_TIMEOUT_MS = int(
    os.environ.get("LITERATURE_BROWSER_NAVIGATION_TIMEOUT_MS", "60000")
)
DOM_READY_TIMEOUT_MS = int(
    os.environ.get("LITERATURE_BROWSER_DOM_READY_TIMEOUT_MS", "15000")
)
NAVIGATION_DNS_RETRIES = 2
DOI_RESOLVE_TIMEOUT_SECONDS = 30
FLARESOLVERR_URL = os.environ.get(
    "LITERATURE_FLARESOLVERR_URL",
    "",
).strip()
FLARESOLVERR_FALLBACK_URLS = tuple(
    dict.fromkeys(
        value.strip()
        for value in os.environ.get(
            "LITERATURE_FLARESOLVERR_FALLBACK_URLS",
            "",
        ).split(",")
        if value.strip() and value.strip() != FLARESOLVERR_URL
    )
)
FLARESOLVERR_MAX_TIMEOUT_MS = min(
    300000,
    max(
        30000,
        int(os.environ.get("LITERATURE_FLARESOLVERR_MAX_TIMEOUT_MS", "240000")),
    ),
)
FLARESOLVERR_MAINTENANCE_MAX_TIMEOUT_MS = bounded_int_env(
    "LITERATURE_FLARESOLVERR_MAINTENANCE_MAX_TIMEOUT_MS",
    180000,
    15000,
    240000,
)
FLARESOLVERR_WAIT_SECONDS = min(
    10,
    max(
        0,
        int(os.environ.get("LITERATURE_FLARESOLVERR_WAIT_SECONDS", "3")),
    ),
)
FLARESOLVERR_ATTEMPT_TIMEOUT_MS = bounded_int_env(
    "LITERATURE_FLARESOLVERR_ATTEMPT_TIMEOUT_MS",
    240000,
    30000,
    300000,
)
FLARESOLVERR_MAX_CONCURRENT = bounded_int_env(
    "LITERATURE_FLARESOLVERR_MAX_CONCURRENT",
    4,
    1,
    16,
)
FLARESOLVERR_QUEUE_SECONDS = bounded_int_env(
    "LITERATURE_FLARESOLVERR_QUEUE_SECONDS",
    300,
    30,
    1200,
)
FLARESOLVERR_SESSION_TTL_MINUTES = bounded_int_env(
    "LITERATURE_FLARESOLVERR_SESSION_TTL_MINUTES",
    10,
    5,
    60,
)
FLARESOLVERR_RSC_TABS_TILL_VERIFY = bounded_int_env(
    "LITERATURE_FLARESOLVERR_RSC_TABS_TILL_VERIFY",
    0,
    0,
    20,
)
SESSION_HANDOFF_ENABLED = (
    os.environ.get("LITERATURE_SESSION_HANDOFF_ENABLED", "false").lower()
    == "true"
)
SESSION_HANDOFF_DESTINATION = os.environ.get(
    "LITERATURE_SESSION_HANDOFF_DESTINATION",
    "none",
).strip()
SESSION_HANDOFF_SCOPE = os.environ.get(
    "LITERATURE_SESSION_HANDOFF_SCOPE",
    "none",
).strip()
SESSION_HANDOFF_BROWSER_VERSION_MATCH = os.environ.get(
    "LITERATURE_SESSION_HANDOFF_BROWSER_VERSION_MATCH",
    "required",
).strip()


class SolverSessionState:
    __slots__ = ("user_agent", "cookies")

    def __init__(
        self,
        *,
        user_agent: str,
        cookies: tuple[dict[str, Any], ...],
    ) -> None:
        self.user_agent = user_agent
        self.cookies = cookies

    def __repr__(self) -> str:
        return "SolverSessionState(<redacted>)"


class FlaresolverrPriorityLock:
    def __init__(self, capacity: int = 1) -> None:
        self._condition = threading.Condition()
        self._capacity = max(1, int(capacity))
        self._active = 0
        self._production_waiters = 0
        self._maintenance_waiters = 0

    def acquire(self, *, timeout: float, maintenance: bool = False) -> bool:
        deadline = time.monotonic() + max(0.0, float(timeout))
        waiter_name = (
            "_maintenance_waiters" if maintenance else "_production_waiters"
        )
        with self._condition:
            setattr(self, waiter_name, getattr(self, waiter_name) + 1)
            try:
                while self._active >= self._capacity or (
                    maintenance and self._production_waiters > 0
                ):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._condition.wait(timeout=remaining)
                self._active += 1
                return True
            finally:
                setattr(self, waiter_name, max(0, getattr(self, waiter_name) - 1))

    def release(self) -> None:
        with self._condition:
            if self._active <= 0:
                raise RuntimeError("cannot release an inactive solver lock")
            self._active -= 1
            self._condition.notify_all()

    def status(self) -> dict[str, int]:
        with self._condition:
            return {
                "capacity": self._capacity,
                "active": self._active,
                "production_waiters": self._production_waiters,
                "maintenance_waiters": self._maintenance_waiters,
            }


_FLARESOLVERR_REQUEST_SLOTS = FlaresolverrPriorityLock(
    FLARESOLVERR_MAX_CONCURRENT
)
_FLARESOLVERR_REQUEST_CONTEXT = threading.local()
PUBLISHER_SHELL_SETTLE_TIMEOUT_MS = min(
    30000,
    max(
        0,
        int(os.environ.get("LITERATURE_PUBLISHER_SHELL_SETTLE_TIMEOUT_MS", "15000")),
    ),
)
PUBLISHER_SHELL_SETTLE_POLL_MS = min(
    5000,
    max(
        500,
        int(os.environ.get("LITERATURE_PUBLISHER_SHELL_SETTLE_POLL_MS", "2000")),
    ),
)
RSC_ARTICLE_WARM_TIMEOUT_MS = min(
    60000,
    max(
        0,
        int(os.environ.get("LITERATURE_RSC_ARTICLE_WARM_TIMEOUT_MS", "20000")),
    ),
)
RSC_ARTICLE_WARM_RELOAD = (
    os.environ.get("LITERATURE_RSC_ARTICLE_WARM_RELOAD", "false").lower()
    not in {"0", "false", "no"}
)
RSC_OPEN_REPOSITORY_FIRST = (
    os.environ.get("LITERATURE_RSC_OPEN_REPOSITORY_FIRST", "false").lower()
    not in {"0", "false", "no"}
)
ELSEVIER_API_KEY = os.environ.get("LITERATURE_ELSEVIER_API_KEY", "").strip()
ELSEVIER_INSTTOKEN = os.environ.get("LITERATURE_ELSEVIER_INSTTOKEN", "").strip()
ELSEVIER_API_TIMEOUT_SECONDS = min(
    90,
    max(
        5,
        int(os.environ.get("LITERATURE_ELSEVIER_API_TIMEOUT_SECONDS", "30")),
    ),
)
MAX_ELSEVIER_API_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_ELSEVIER_API_OBJECT_BYTES = max(MAX_PDF_BYTES, MAX_FIGURE_IMAGE_BYTES)
MAX_FLARESOLVERR_RESPONSE_BYTES = 8 * 1024 * 1024
FLARESOLVERR_ALLOWED_DOMAINS = tuple(
    domain.strip().lower().lstrip(".")
    for domain in os.environ.get(
        "LITERATURE_FLARESOLVERR_ALLOWED_DOMAINS",
        "acs.org,figshare.com,rsc.org,rscj.silverchair-cdn.com,wiley.com,science.org",
    ).split(",")
    if domain.strip()
)
SCIENCEDIRECT_CHINA_STATIC_HOST = "sciencedirect.elseviercdn.cn"
SCIENCEDIRECT_STATIC_FALLBACK_HOSTS = (
    "sdfestaticassets-us-east-1.sciencedirectassets.com",
    "sdfestaticassets-eu-west-1.sciencedirectassets.com",
)
SCIENCEDIRECT_STATIC_PATH_PREFIXES = ("/prod/", "/shared-assets/")
DEFAULT_BROWSER_PDF_FETCH_DOMAINS = (
    "acs.org",
    "figshare.com",
    "rsc.org",
    "rscj.silverchair-cdn.com",
    "nature.com",
    "springer.com",
    "springernature.com",
    "wiley.com",
    "pnas.org",
    "science.org",
)
BROWSER_PDF_FETCH_DOMAINS = tuple(
    domain.strip().lower().lstrip(".")
    for domain in os.environ.get(
        "LITERATURE_BROWSER_PDF_FETCH_DOMAINS",
        ",".join(DEFAULT_BROWSER_PDF_FETCH_DOMAINS),
    ).split(",")
    if domain.strip()
)

DEFAULT_ALLOWED_DOMAINS = (
    "xmu.edu.cn",
    "doi.org",
    "acs.org",
    "sciencedirect.com",
    "sciencedirectassets.com",
    "els-cdn.com",
    "elsevier.com",
    "elsevier.io",
    "springer.com",
    "springernature.com",
    "nature.com",
    "wiley.com",
    "rsc.org",
    "rscj.silverchair-cdn.com",
    "science.org",
    "tandfonline.com",
    "webofscience.com",
    "clarivate.com",
    "scopus.com",
    "jstor.org",
    "ieee.org",
    "aip.org",
    "aps.org",
    "oup.com",
    "cambridge.org",
    "sagepub.com",
    "cell.com",
    "pnas.org",
    "cnki.net",
    "wanfangdata.com.cn",
    "researchsquare.com",
    "ssrn.com",
    "chemrxiv.org",
    "arxiv.org",
    "zenodo.org",
    "figshare.com",
    "ebi.ac.uk",
    "berkeley.edu",
    "scholarsmine.mst.edu",
)

SUPPLEMENTARY_PDF_PATH_MARKERS = (
    "/doi/suppl/",
    "/suppdata/",
    "/supplement/",
    "/supplementary/",
    "/supporting-information/",
    "/suppl_file/",
    "/esm/",
)
SUPPLEMENTARY_PDF_FILENAME_RE = re.compile(
    r"(?:-mmc\d+|_moesm\d+_esm|[-_.](?:si|esi|supinfo|supp(?:lement(?:ary)?)?|"
    r"supporting[-_]?information))\.pdf$",
    flags=re.IGNORECASE,
)
RSC_SUPPLEMENT_DOI_RE = re.compile(
    r"^(?P<doi>[a-z]\d[a-z]{2}\d{5}[a-z])\d*(?:_suppl)?(?:\.pdf)?$",
    flags=re.IGNORECASE,
)
RSC_ARTICLE_DOI_RE = re.compile(
    r"^10\.1039/(?P<code>[a-z0-9]+)$",
    flags=re.IGNORECASE,
)
RSC_LEGACY_ARTICLE_CODE_RE = re.compile(
    r"^(?P<journal>(?:[a-z]\d|[a-z]{2}))(?P<year3>\d{3})(?P<page>\d{7})$",
    flags=re.IGNORECASE,
)
RSC_MODERN_ARTICLE_CODE_RE = re.compile(
    r"^(?P<era>[a-z])(?P<year_digit>\d)(?P<journal>[a-z]{2})(?P<rest>[a-z0-9]+)$",
    flags=re.IGNORECASE,
)
RSC_LEGACY_LANDING_ARTICLE_RE = re.compile(
    r"^(?P<journal>[a-z][a-z0-9]*)/article/(?P<volume>\d+)/"
    r"(?P<issue>\d+)/(?P<first_page>\d+)(?:-\d+)?/(?P<article_id>\d+)$",
    flags=re.IGNORECASE,
)
RSC_LEGACY_LANDING_YEAR_OFFSET_BY_JOURNAL = {
    # Old Chemical Communications URLs encode volume/page but not the DOI suffix.
    # For this archive route volume 21 maps to 1985, and the RSC article code is
    # c3 + yyy + page. Leave other old landing routes on the browser path until
    # they have an explicitly reviewed journal mapping.
    "c3": 1964,
}
RSC_ARTICLE_DECADE_BY_PREFIX = {
    "b": 2000,
    "c": 2010,
    "d": 2020,
    "e": 2030,
}
WILEY_SUPPLEMENT_DOI_RE = re.compile(
    r"^10\.1002/[A-Z0-9][A-Z0-9._;()/:+-]*$",
    flags=re.IGNORECASE,
)
PNAS_SUPPLEMENT_PATH_RE = re.compile(
    r"^/doi/suppl/(?P<doi>10\.1073/[^/]+)/suppl_file/"
    r"(?P<filename>[A-Z0-9._+-]+\.pdf)$",
    flags=re.IGNORECASE,
)
PNAS_DOI_RE = re.compile(
    r"^10\.1073/[A-Z0-9][A-Z0-9._;()+-]*$",
    flags=re.IGNORECASE,
)
ACS_SUPPLEMENT_PATH_RE = re.compile(
    r"^/(?P<journal>[^/]+)/article-supplement/(?P<article_id>\d+)/pdf/"
    r"(?P<file_stem>[a-z0-9._-]+)/?$",
    flags=re.IGNORECASE,
)
ACS_DOI_RE = re.compile(
    r"^10\.1021/[A-Z0-9][A-Z0-9._;()+-]*$",
    flags=re.IGNORECASE,
)
SCIENCEDIRECT_SUPPLEMENT_EXTENSIONS = (
    ".pdf",
    ".doc",
    ".docx",
    ".zip",
    ".xls",
    ".xlsx",
)
ALLOWED_DOMAINS = tuple(
    domain.strip().lower().lstrip(".")
    for domain in os.environ.get(
        "LITERATURE_BROWSER_ALLOWED_DOMAINS",
        ",".join(DEFAULT_ALLOWED_DOMAINS),
    ).split(",")
    if domain.strip()
)


class LiteratureBrowserError(RuntimeError):
    pass


class LiteratureBrowserBusyError(LiteratureBrowserError):
    pass


class LiteratureBrowserTimeoutError(LiteratureBrowserError):
    pass


class SameSessionDownloadError(LiteratureBrowserError):
    """A selected FlareSolverr download path failed and must not fall back."""

    pass


class SameSessionAssetError(LiteratureBrowserError):
    """A selected FlareSolverr figure path failed and must not fall back."""

    pass


def host_in_domains(host: str, domains: tuple[str, ...]) -> bool:
    normalized = host.strip().lower().rstrip(".").lstrip(".")
    return bool(normalized) and any(
        normalized == domain or normalized.endswith(f".{domain}")
        for domain in domains
    )


def host_allowed(host: str) -> bool:
    normalized = host.strip().lower().rstrip(".")
    if not normalized:
        return False
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        address = None
    if address is not None:
        return False
    return host_in_domains(normalized, ALLOWED_DOMAINS)


def flaresolverr_host_allowed(host: str) -> bool:
    return host_in_domains(host, FLARESOLVERR_ALLOWED_DOMAINS)


def browser_pdf_fetch_host_allowed(host: str) -> bool:
    return host_in_domains(host, BROWSER_PDF_FETCH_DOMAINS)


def landing_hint_allowed_for_target(target_host: str, candidate_host: str) -> bool:
    if candidate_host == target_host:
        return True
    try:
        return flaresolverr_publisher_slug(
            f"https://{target_host}/"
        ) == flaresolverr_publisher_slug(f"https://{candidate_host}/")
    except LiteratureBrowserError:
        return False


def validate_browser_pdf_result_url(source_url: str, raw_final_url: str) -> str:
    source = validate_url(source_url)
    value = raw_final_url.strip()
    try:
        final_url = validate_url(value)
    except LiteratureBrowserError:
        source_host = urlparse(source).hostname or ""
        parsed = urlparse(value)
        final_host = (parsed.hostname or "").lower().rstrip(".")
        trusted_acs_object = (
            parsed.scheme == "https"
            and not parsed.username
            and not parsed.password
            and host_in_domains(source_host, ("figshare.com",))
            and final_host == "s3-eu-west-1.amazonaws.com"
            and bool(
                re.fullmatch(
                    r"/pstorage-acs-\d+/\d+/[^/]+\.pdf",
                    unquote(parsed.path),
                    flags=re.IGNORECASE,
                )
            )
        )
        if trusted_acs_object:
            return value
        raise LiteratureBrowserError(
            "browser PDF redirected outside its publisher allowlist"
        )
    final_host = urlparse(final_url).hostname or ""
    if not browser_pdf_fetch_host_allowed(final_host):
        raise LiteratureBrowserError(
            "browser PDF redirected outside its publisher allowlist"
        )
    return final_url


def should_try_flaresolverr(
    url: str,
    page_state: str,
    text: str,
    *,
    full_text_dom: bool = False,
) -> bool:
    host = urlparse(url).hostname or ""
    if not FLARESOLVERR_URL or not flaresolverr_host_allowed(host):
        return False
    if page_state == "challenge":
        return True
    if rsc_pdf_only_article_shell(url, text):
        return False
    return (
        page_state == "content"
        and host_in_domains(
            host,
            ("acs.org", "rsc.org", "sciencedirect.com", "elsevier.com"),
        )
        and not full_text_dom
        and not full_text_visible(text)
    )


def should_retry_supplement_with_flaresolverr(
    url: str,
    fast_error: str,
) -> bool:
    host = urlparse(url).hostname or ""
    return bool(
        FLARESOLVERR_URL
        and flaresolverr_host_allowed(host)
        and re.search(
            r"\bHTTP\s+(?:401|403|429|503)\b|challenge|cloudflare|turnstile",
            fast_error,
            flags=re.IGNORECASE,
        )
    )


def rsc_pdf_only_article_shell(
    url: str,
    text: str,
    *,
    pdf_links: list[dict[str, str]] | None = None,
) -> bool:
    host = urlparse(url).hostname or ""
    if not host_in_domains(host, ("rsc.org",)):
        return False
    normalized = re.sub(r"\s+", " ", text or "").strip().lower()
    if "only available via pdf" not in normalized:
        return False
    if "open the pdf" in normalized:
        return True
    return bool(pdf_links and select_pdf_candidates(pdf_links))


def unresolved_rsc_solver(
    url: str,
    challenge_bypass: dict[str, Any],
    text: str,
    *,
    full_text_dom: bool,
    pdf_links: list[dict[str, str]] | None = None,
) -> bool:
    host = urlparse(url).hostname or ""
    return (
        host_in_domains(host, ("rsc.org",))
        and bool(challenge_bypass.get("attempted"))
        and not full_text_dom
        and not full_text_visible(text)
        and not rsc_pdf_only_article_shell(url, text, pdf_links=pdf_links)
    )


def validate_url(raw: str) -> str:
    value = raw.strip()
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        raise LiteratureBrowserError("literature URL must use http or https")
    if parsed.username or parsed.password:
        raise LiteratureBrowserError("literature URL must not contain credentials")
    if not parsed.hostname or not host_allowed(parsed.hostname):
        raise LiteratureBrowserError("literature URL host is not allowlisted")
    return value


def strip_transient_challenge_query(target: str) -> str:
    value = validate_url(target)
    parsed = urlparse(value)
    retained = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("__cf_chl_")
    ]
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(retained, doseq=True),
            parsed.fragment,
        )
    )


def governed_supplement_fallback(target: str) -> dict[str, str] | None:
    source_url = validate_url(target)
    try:
        payload = json.loads(SUPPLEMENT_FALLBACKS_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise LiteratureBrowserError(
            f"supplement fallback policy is invalid: {type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schema") != (
        "scientist-literature-supplement-fallbacks/v1"
    ):
        raise LiteratureBrowserError("supplement fallback policy schema is invalid")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise LiteratureBrowserError("supplement fallback policy entries are invalid")
    matches: list[dict[str, str]] = []
    for raw_entry in entries:
        if not isinstance(raw_entry, dict):
            raise LiteratureBrowserError("supplement fallback entry must be an object")
        try:
            entry_source = validate_url(str(raw_entry.get("source_url") or ""))
            fallback_url = validate_url(str(raw_entry.get("fallback_url") or ""))
        except LiteratureBrowserError:
            raise
        expected_doi = str(raw_entry.get("expected_doi") or "").strip().lower()
        expected_sha256 = str(raw_entry.get("sha256") or "").strip().lower()
        if not re.fullmatch(r"10\.\d{4,9}/\S+", expected_doi):
            raise LiteratureBrowserError("supplement fallback DOI is invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
            raise LiteratureBrowserError("supplement fallback SHA-256 is invalid")
        if entry_source == source_url:
            matches.append(
                {
                    "source_url": entry_source,
                    "fallback_url": fallback_url,
                    "expected_doi": expected_doi,
                    "sha256": expected_sha256,
                    "provenance": str(raw_entry.get("provenance") or "").strip(),
                }
            )
    if len(matches) > 1:
        raise LiteratureBrowserError("supplement fallback source is ambiguous")
    return matches[0] if matches else None


def rsc_legacy_landing_article_code_from_path(lowered_path: str) -> str:
    """Resolve a reviewed old RSC landing path to an RSC article code.

    Example:
    c3/article/21/4/216-218/136707 -> c39850000216.

    This is deliberately not a general DOI decoder. The bare old landing URL has
    no DOI suffix, so we only derive a code for journal routes with an explicit
    volume-to-year mapping.
    """
    legacy_landing = RSC_LEGACY_LANDING_ARTICLE_RE.fullmatch(lowered_path)
    if not legacy_landing:
        return ""
    journal = legacy_landing.group("journal").lower()
    offset = RSC_LEGACY_LANDING_YEAR_OFFSET_BY_JOURNAL.get(journal)
    if offset is None:
        return ""
    try:
        volume = int(legacy_landing.group("volume"))
        first_page = int(legacy_landing.group("first_page"))
    except ValueError:
        return ""
    year = offset + volume
    if 1900 <= year <= 1999 and first_page > 0:
        return f"{journal}{year % 1000:03d}{first_page:07d}"
    return ""


def rsc_article_code_from_url(raw: str) -> str:
    """Extract a reviewed RSC article code from a DOI, PDF URL, or old landing URL.

    For DOI URLs this only returns the DOI suffix after 10.1039/. Route
    construction happens later in rsc_article_pdf_routes_from_code().
    """
    try:
        value = validate_url(raw)
    except LiteratureBrowserError:
        return ""
    parsed = urlparse(value)
    host = (parsed.hostname or "").lower().rstrip(".")
    path = unquote(parsed.path).strip("/")
    if host == "doi.org":
        match = RSC_ARTICLE_DOI_RE.fullmatch(path)
        return match.group("code").lower() if match else ""
    if not host_in_domains(host, ("rsc.org",)):
        return ""
    lowered_path = path.lower()
    content_match = re.search(
        r"(?:^|/)content/(?:articlepdf|articlehtml|articlelanding)/"
        r"\d{4}/[^/]+/(?P<code>[a-z0-9]+)$",
        lowered_path,
    )
    if content_match:
        return content_match.group("code").lower()
    legacy_landing_code = rsc_legacy_landing_article_code_from_path(lowered_path)
    if legacy_landing_code:
        return legacy_landing_code
    filename = lowered_path.rsplit("/", 1)[-1]
    if filename.endswith(".pdf"):
        stem = filename[:-4]
        if re.fullmatch(r"[a-z0-9]+", stem):
            return stem
    return ""


def rsc_article_doi_code_from_url(raw: str) -> str:
    """Compatibility alias for older tests/callers.

    Prefer rsc_article_code_from_url(); the value is an RSC article code, not a
    decoded DOI.
    """
    return rsc_article_code_from_url(raw)


def rsc_article_pdf_routes_from_code(code: str) -> list[str]:
    """Build official RSC article PDF routes for reviewed article-code families."""
    normalized = code.strip().lower()
    if not re.fullmatch(r"[a-z0-9]+", normalized):
        return []
    candidates: list[str] = []
    legacy = RSC_LEGACY_ARTICLE_CODE_RE.fullmatch(normalized)
    if legacy:
        year3 = int(legacy.group("year3"))
        year = 1000 + year3 if year3 >= 900 else 2000 + year3
        journal = legacy.group("journal").lower()
        candidates.append(
            f"https://pubs.rsc.org/en/content/articlepdf/{year}/{journal}/{normalized}"
        )
    modern = RSC_MODERN_ARTICLE_CODE_RE.fullmatch(normalized)
    if modern:
        era = modern.group("era").lower()
        decade = RSC_ARTICLE_DECADE_BY_PREFIX.get(era)
        if decade is not None:
            year = decade + int(modern.group("year_digit"))
            journal = normalized[2:4]
            candidates.append(
                f"https://pubs.rsc.org/en/content/articlepdf/{year}/{journal}/{normalized}"
            )
    return list(dict.fromkeys(candidates))


def rsc_article_pdf_navigation_candidates(*urls: str) -> list[str]:
    """Official RSC PDF routes to navigate with the warmed RSC browser profile."""
    candidates: list[str] = []
    codes: list[str] = []
    for raw in urls:
        if not raw:
            continue
        try:
            value = validate_url(raw)
        except LiteratureBrowserError:
            continue
        parsed = urlparse(value)
        host = parsed.hostname or ""
        path = unquote(parsed.path).lower()
        if (
            host_in_domains(host, ("rsc.org",))
            and not is_supplementary_pdf_url(value)
            and (
                "/article-pdf/" in path
                or "/content/articlepdf/" in path
                or path.endswith(".pdf")
            )
        ):
            candidates.append(value)
        code = rsc_article_code_from_url(value)
        if code:
            codes.append(code)
    for code in dict.fromkeys(codes):
        candidates.extend(rsc_article_pdf_routes_from_code(code))
    return list(dict.fromkeys(candidates))


def rsc_legacy_article_pdf_navigation_target(*urls: str) -> str:
    """Return an official RSC PDF route for reviewed legacy PDF-only articles.

    The caller should navigate this URL with the warmed RSC browser profile. This
    helper only normalizes old DOI/landing forms; it does not perform challenge
    solving or cookie-free PDF handoff.
    """
    for raw in urls:
        code = rsc_article_code_from_url(raw)
        if not code or not RSC_LEGACY_ARTICLE_CODE_RE.fullmatch(code):
            continue
        routes = rsc_article_pdf_routes_from_code(code)
        if routes:
            return routes[0]
    return ""


def rsc_legacy_article_pdf_navigation_candidates(
    original_url: str,
    target_url: str,
    *,
    original_is_article_pdf_request: bool,
) -> list[str]:
    candidates: list[str] = []
    code = rsc_article_code_from_url(original_url) or rsc_article_code_from_url(
        target_url
    )
    if code and RSC_LEGACY_ARTICLE_CODE_RE.fullmatch(code):
        candidates.extend(
            rsc_article_pdf_navigation_candidates(original_url, target_url)
        )
    elif original_is_article_pdf_request:
        candidates.extend(
            rsc_article_pdf_navigation_candidates(original_url, target_url)
        )
    return list(dict.fromkeys(candidates))


BROWSER_OWNED_DOI_PREFIXES = (
    "10.1021/",
    "10.1039/",
    "10.1002/",
    "10.1126/",
    "10.1073/",
)


def doi_should_resolve_in_browser(target: str) -> bool:
    parsed = urlparse(target)
    host = (parsed.hostname or "").lower().rstrip(".")
    if host not in {"doi.org", "dx.doi.org"}:
        return False
    doi = unquote(parsed.path.lstrip("/")).lower()
    return doi.startswith(BROWSER_OWNED_DOI_PREFIXES)


def canonical_navigation_target(raw: str) -> str:
    target = validate_url(raw)
    parsed = urlparse(target)
    host = (parsed.hostname or "").lower().rstrip(".")
    rsc_legacy_pdf_target = rsc_legacy_article_pdf_navigation_target(target)
    if rsc_legacy_pdf_target:
        return rsc_legacy_pdf_target
    if host_in_domains(host, ("sciencedirect.com",)):
        article_download = re.fullmatch(
            r"(/science/article/pii/[A-Za-z0-9]+)/(?:pdf|pdfft)/?",
            parsed.path,
        )
        if article_download:
            return parsed._replace(
                path=article_download.group(1),
                query="",
                fragment="",
            ).geturl()
    if (parsed.hostname or "").lower().rstrip(".") != "doi.org":
        return target
    if doi_should_resolve_in_browser(target):
        return target
    request = urllib.request.Request(
        target,
        headers={"User-Agent": "Mozilla/5.0"},
        method="HEAD",
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=DOI_RESOLVE_TIMEOUT_SECONDS,
        ) as response:
            resolved = validate_url(response.geturl())
    except Exception:
        return target
    resolved_parsed = urlparse(resolved)
    host = (resolved_parsed.hostname or "").lower().rstrip(".")
    pii = re.fullmatch(
        r"/retrieve/pii/([A-Za-z0-9]+)",
        resolved_parsed.path,
    )
    if host == "linkinghub.elsevier.com" and pii:
        return (
            "https://www.sciencedirect.com/science/article/pii/"
            f"{pii.group(1)}?via%3Dihub"
        )
    if (
        host == "link.springer.com"
        and resolved_parsed.path.startswith("/10.1007/")
    ):
        return resolved_parsed._replace(
            path=f"/article{resolved_parsed.path}"
        ).geturl()
    return resolved


def chromium_launch_args() -> list[str]:
    return [
        *(["--disable-dev-shm-usage"] if DISABLE_DEV_SHM_USAGE else []),
        (
            "--disable-features="
            "AsyncDns,DnsOverHttps,OptimizationHints,Translate,"
            "UseDnsHttpsSvcbAlpn"
        ),
        "--no-first-run",
        "--no-default-browser-check",
        "--no-sandbox",
    ]


def sciencedirect_static_fallback_urls(raw: str) -> tuple[str, ...]:
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower().rstrip(".")
    if (
        parsed.scheme != "https"
        or host != SCIENCEDIRECT_CHINA_STATIC_HOST
        or not parsed.path.startswith(SCIENCEDIRECT_STATIC_PATH_PREFIXES)
    ):
        return ()
    return tuple(
        parsed._replace(netloc=fallback_host).geturl()
        for fallback_host in SCIENCEDIRECT_STATIC_FALLBACK_HOSTS
    )


def proxy_sciencedirect_static_asset(route: Any) -> None:
    request = route.request
    fallbacks = sciencedirect_static_fallback_urls(str(request.url))
    if not fallbacks or str(request.method).upper() != "GET":
        route.continue_()
        return

    headers = {
        key: value
        for key, value in dict(request.headers).items()
        if key.lower()
        not in {"authorization", "cookie", "host", "origin", "proxy-authorization"}
    }
    for fallback_url in fallbacks:
        response = None
        try:
            response = route.fetch(
                url=fallback_url,
                headers=headers,
                timeout=30000,
            )
            if 200 <= int(response.status) < 300:
                route.fulfill(response=response)
                return
        except Exception:
            pass
        finally:
            if response is not None:
                try:
                    response.dispose()
                except Exception:
                    pass
    route.continue_()


def goto_with_dns_retry(
    page: Any,
    target: str,
    *,
    deadline: float | None = None,
) -> Any:
    url = validate_url(target)
    for attempt in range(NAVIGATION_DNS_RETRIES + 1):
        remaining_ms = (
            NAVIGATION_TIMEOUT_MS
            if deadline is None
            else int(max(0.0, deadline - time.monotonic()) * 1000)
        )
        if remaining_ms < 1000:
            raise LiteratureBrowserError(
                "publisher navigation reached the request time budget"
            )
        try:
            return page.goto(
                url,
                wait_until="commit",
                timeout=min(NAVIGATION_TIMEOUT_MS, remaining_ms),
            )
        except Exception as exc:
            if (
                "ERR_NAME_NOT_RESOLVED" not in str(exc)
                or attempt >= NAVIGATION_DNS_RETRIES
            ):
                raise
            page.wait_for_timeout(2000 * (attempt + 1))
    raise LiteratureBrowserError("publisher navigation failed")


@contextmanager
def flaresolverr_request_lock(
    queue_timeout_seconds: float | None = None,
) -> Any:
    wait_seconds = (
        float(FLARESOLVERR_QUEUE_SECONDS)
        if queue_timeout_seconds is None
        else min(
            float(FLARESOLVERR_QUEUE_SECONDS),
            max(0.1, float(queue_timeout_seconds)),
        )
    )
    maintenance = bool(
        getattr(_FLARESOLVERR_REQUEST_CONTEXT, "maintenance", False)
    )
    acquired = _FLARESOLVERR_REQUEST_SLOTS.acquire(
        timeout=wait_seconds,
        maintenance=maintenance,
    )
    if not acquired:
        raise LiteratureBrowserError(
            "publisher challenge solver queue wait timed out"
        )
    try:
        yield
    finally:
        _FLARESOLVERR_REQUEST_SLOTS.release()


def flaresolverr_api(payload: dict[str, Any], *, timeout_seconds: float) -> dict[str, Any]:
    endpoint = str(
        getattr(_FLARESOLVERR_REQUEST_CONTEXT, "endpoint", FLARESOLVERR_URL)
        or FLARESOLVERR_URL
    )
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read(MAX_FLARESOLVERR_RESPONSE_BYTES + 1)
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        raise LiteratureBrowserError(
            f"publisher challenge solver request failed: {exc}"
        ) from exc
    if len(raw) > MAX_FLARESOLVERR_RESPONSE_BYTES:
        raise LiteratureBrowserError(
            "publisher challenge solver response is too large"
        )
    try:
        result = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LiteratureBrowserError(
            "publisher challenge solver returned invalid JSON"
        ) from exc
    if not isinstance(result, dict):
        raise LiteratureBrowserError(
            "publisher challenge solver returned invalid JSON"
        )
    return result


@contextmanager
def flaresolverr_endpoint(endpoint: str):
    had_previous = hasattr(_FLARESOLVERR_REQUEST_CONTEXT, "endpoint")
    previous = getattr(_FLARESOLVERR_REQUEST_CONTEXT, "endpoint", None)
    _FLARESOLVERR_REQUEST_CONTEXT.endpoint = endpoint
    try:
        yield
    finally:
        if had_previous:
            _FLARESOLVERR_REQUEST_CONTEXT.endpoint = previous
        else:
            del _FLARESOLVERR_REQUEST_CONTEXT.endpoint


def flaresolverr_publisher_slug(target: str) -> str:
    host = urlparse(validate_url(target)).hostname or ""
    publisher_domains = (
        (("acs.org", "figshare.com", "acs.silverchair-cdn.com"), "acs"),
        (("rsc.org", "rscj.silverchair-cdn.com"), "rsc"),
        (("nature.com", "springer.com", "springernature.com"), "nature-springer"),
        (("wiley.com",), "wiley"),
        (("science.org",), "science"),
        (("pnas.org",), "pnas"),
    )
    for domains, publisher in publisher_domains:
        if host_in_domains(host, domains):
            return publisher
    raise LiteratureBrowserError(
        "challenge solver is not configured for this publisher host"
    )


def normalize_flaresolverr_cookies(
    raw_cookies: list[dict[str, Any]],
    publisher: str,
) -> list[dict[str, Any]]:
    cookies: list[dict[str, Any]] = []
    for item in raw_cookies:
        name = str(item.get("name") or "")
        value = str(item.get("value") or "")
        domain = str(item.get("domain") or "").strip().lower()
        if not name or not domain or not flaresolverr_host_allowed(domain):
            continue
        try:
            cookie_publisher = flaresolverr_publisher_slug(
                f"https://{domain.lstrip('.')}/"
            )
        except LiteratureBrowserError:
            continue
        if cookie_publisher != publisher:
            continue
        cookie: dict[str, Any] = {
            "name": name,
            "value": value,
            "domain": domain,
            "path": str(item.get("path") or "/"),
            "secure": bool(item.get("secure", True)),
            "httpOnly": bool(item.get("httpOnly", False)),
        }
        expires = item.get("expires")
        if isinstance(expires, (int, float)) and expires > 0:
            cookie["expires"] = expires
        same_site = item.get("sameSite")
        if same_site in {"Strict", "Lax", "None"}:
            cookie["sameSite"] = same_site
        cookies.append(cookie)
    return cookies


def chromium_major_from_user_agent(user_agent: str) -> int | None:
    match = re.search(r"(?:Chrome|Chromium)/(\d+)", user_agent)
    return int(match.group(1)) if match else None


def validate_same_profile_handoff(
    *,
    publisher: str,
    solver_user_agent: str,
    engine_user_agent: str,
) -> None:
    if not (
        SESSION_HANDOFF_ENABLED
        and SESSION_HANDOFF_DESTINATION == "same_profile"
        and SESSION_HANDOFF_SCOPE == "same_publisher"
        and SESSION_HANDOFF_BROWSER_VERSION_MATCH == "required"
    ):
        raise LiteratureBrowserError("solver profile handoff is not enabled")
    if not publisher:
        raise LiteratureBrowserError("solver profile handoff has no publisher")
    solver_major = chromium_major_from_user_agent(solver_user_agent)
    engine_major = chromium_major_from_user_agent(engine_user_agent)
    if solver_major is None or engine_major is None or solver_major != engine_major:
        raise LiteratureBrowserError(
            "solver and engine Chromium versions do not match"
        )


def apply_flaresolverr_handoff(
    page: Any,
    solved: dict[str, Any],
    *,
    publisher: str,
) -> str:
    session_state = solved.pop("session_state", None)
    if not isinstance(session_state, SolverSessionState):
        raise LiteratureBrowserError(
            "challenge solver returned no reusable profile state"
        )
    solver_user_agent = session_state.user_agent.strip()
    solver_cookies = list(session_state.cookies)
    if not solver_user_agent or not solver_cookies:
        raise LiteratureBrowserError(
            "challenge solver returned incomplete profile state"
        )
    engine_user_agent = str(
        page.evaluate("() => navigator.userAgent") or ""
    ).strip()
    validate_same_profile_handoff(
        publisher=publisher,
        solver_user_agent=solver_user_agent,
        engine_user_agent=engine_user_agent,
    )
    page.context.add_cookies(solver_cookies)
    page.set_extra_http_headers({"User-Agent": solver_user_agent})
    return solver_user_agent


def publisher_flaresolverr_session_id(publisher: str) -> str:
    normalized_publisher = publisher.strip().lower()
    profile_id = PROFILE_ID.strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9-]{1,31}", normalized_publisher):
        raise LiteratureBrowserError("solver publisher identity is invalid")
    if not re.fullmatch(r"browser-(?:single|\d{3})", profile_id):
        raise LiteratureBrowserError("browser profile identity is invalid")
    return f"scientist-{normalized_publisher}-{profile_id}"


def ensure_flaresolverr_session(session_id: str) -> None:
    listed = flaresolverr_api(
        {"cmd": "sessions.list"},
        timeout_seconds=30,
    )
    sessions = listed.get("sessions") or []
    if session_id in sessions:
        return
    created = flaresolverr_api(
        {"cmd": "sessions.create", "session": session_id},
        timeout_seconds=30,
    )
    if created.get("status") != "ok":
        raise LiteratureBrowserError(
            "publisher challenge solver session could not be created"
        )


def destroy_flaresolverr_session(session_id: str) -> None:
    destroyed = flaresolverr_api(
        {"cmd": "sessions.destroy", "session": session_id},
        timeout_seconds=30,
    )
    if destroyed.get("status") != "ok":
        raise LiteratureBrowserError(
            "publisher challenge solver session could not be destroyed"
        )


def request_flaresolverr(
    target: str,
    *,
    queue_timeout_seconds: float | None = None,
    max_timeout_ms: int | None = None,
    session_id: str = "",
) -> dict[str, Any]:
    if not FLARESOLVERR_URL:
        raise LiteratureBrowserError("publisher challenge solver is not configured")
    if not session_id:
        raise LiteratureBrowserError(
            "challenge solver requires a profile-scoped named session"
        )
    url = validate_url(target)
    host = urlparse(url).hostname or ""
    if not flaresolverr_host_allowed(host):
        raise LiteratureBrowserError(
            "challenge solver is not enabled for this publisher"
        )
    effective_max_timeout_ms = (
        FLARESOLVERR_MAX_TIMEOUT_MS
        if max_timeout_ms is None
        else min(
            FLARESOLVERR_MAX_TIMEOUT_MS,
            max(10000, int(max_timeout_ms)),
        )
    )
    if bool(getattr(_FLARESOLVERR_REQUEST_CONTEXT, "maintenance", False)):
        effective_max_timeout_ms = min(
            effective_max_timeout_ms,
            FLARESOLVERR_MAINTENANCE_MAX_TIMEOUT_MS,
        )
    publisher = flaresolverr_publisher_slug(url)
    request_payload: dict[str, Any] = {
        "cmd": "request.get",
        "url": url,
        "maxTimeout": effective_max_timeout_ms,
        "waitInSeconds": FLARESOLVERR_WAIT_SECONDS,
        "disableMedia": False,
    }
    if publisher == "rsc" and FLARESOLVERR_RSC_TABS_TILL_VERIFY > 0:
        request_payload["tabs_till_verify"] = (
            FLARESOLVERR_RSC_TABS_TILL_VERIFY
        )
    with flaresolverr_request_lock(queue_timeout_seconds):
        ensure_flaresolverr_session(session_id)
        request_payload.update(
            {
                "session": session_id,
            }
        )
        try:
            result = flaresolverr_api(
                request_payload,
                timeout_seconds=(effective_max_timeout_ms / 1000) + 15,
            )
        finally:
            destroy_flaresolverr_session(session_id)
    solution = result.get("solution") or {}
    if result.get("status") != "ok":
        message = str(result.get("message") or "challenge was not solved")
        raise LiteratureBrowserError(
            f"publisher challenge solver failed: {message[:300]}"
        )
    status = solution.get("status")
    if not isinstance(status, int) or status >= 400:
        raise LiteratureBrowserError(
            f"publisher challenge solver returned HTTP {status}"
        )
    final_url = validate_url(str(solution.get("url") or url))
    final_host = urlparse(final_url).hostname or ""
    if not flaresolverr_host_allowed(final_host):
        raise LiteratureBrowserError(
            "publisher challenge solver redirected outside its allowlist"
        )
    solver_html = str(solution.get("response") or "")
    user_agent = str(solution.get("userAgent") or "").strip()
    cookies = normalize_flaresolverr_cookies(
        solution.get("cookies") or [],
        publisher,
    )
    final_path = urlparse(final_url).path.lower()
    final_is_pdf = final_path.endswith(".pdf") or "/articlepdf/" in final_path
    if not final_is_pdf and not solver_html.strip():
        raise LiteratureBrowserError(
            "publisher challenge solver did not return same-session content"
        )
    session_state = SolverSessionState(
        user_agent=user_agent,
        cookies=tuple(cookies),
    )
    return {
        "final_url": final_url,
        "status": status,
        "session_state": session_state,
        "solver_mode": "profile_lease",
    }


def flaresolverr_download_artifact(
    download_id: str,
    *,
    expected_bytes: int,
    expected_sha256: str,
    timeout_seconds: float,
) -> bytes:
    if not re.fullmatch(r"[A-Za-z0-9_-]{43}", download_id):
        raise LiteratureBrowserError(
            "publisher challenge solver returned an invalid download token"
        )
    artifact_url = f"{FLARESOLVERR_URL.rstrip('/')}/download/{download_id}"
    request = urllib.request.Request(
        artifact_url,
        headers={"Accept": "application/pdf"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            content_type = str(response.headers.get("Content-Type") or "")
            raw = response.read(MAX_PDF_BYTES + 1)
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        raise LiteratureBrowserError(
            f"same-session solver PDF transfer failed: {exc}"
        ) from exc
    if len(raw) > MAX_PDF_BYTES:
        raise LiteratureBrowserError(
            "same-session solver PDF exceeds the size limit"
        )
    if (
        len(raw) != expected_bytes
        or not raw.startswith(b"%PDF-")
        or "application/pdf" not in content_type.lower()
        or hashlib.sha256(raw).hexdigest() != expected_sha256
    ):
        raise LiteratureBrowserError(
            "same-session solver PDF failed integrity validation"
        )
    return raw


def solver_image_bytes_match_mime(raw: bytes, mime_type: str) -> bool:
    mime = mime_type.split(";", 1)[0].strip().casefold()
    signatures = {
        "image/png": raw.startswith(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg": raw.startswith(b"\xff\xd8\xff"),
        "image/gif": raw.startswith((b"GIF87a", b"GIF89a")),
        "image/webp": raw.startswith(b"RIFF") and raw[8:12] == b"WEBP",
        "image/tiff": raw.startswith((b"II*\x00", b"MM\x00*")),
        "image/avif": len(raw) >= 12
        and raw[4:12] in {b"ftypavif", b"ftypavis"},
        "image/svg+xml": b"<svg" in raw[:4096].lstrip(),
    }
    return bool(signatures.get(mime))


def flaresolverr_image_artifact(
    asset_id: str,
    *,
    expected_bytes: int,
    expected_sha256: str,
    expected_content_type: str,
    timeout_seconds: float,
) -> bytes:
    if not re.fullmatch(r"[A-Za-z0-9_-]{43}", asset_id):
        raise LiteratureBrowserError(
            "publisher challenge solver returned an invalid image token"
        )
    artifact_url = f"{FLARESOLVERR_URL.rstrip('/')}/asset/{asset_id}"
    request = urllib.request.Request(
        artifact_url,
        headers={"Accept": expected_content_type},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            content_type = str(response.headers.get("Content-Type") or "")
            raw = response.read(MAX_FIGURE_IMAGE_BYTES + 1)
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        raise LiteratureBrowserError(
            f"same-session solver image transfer failed: {exc}"
        ) from exc
    if len(raw) > MAX_FIGURE_IMAGE_BYTES:
        raise LiteratureBrowserError(
            "same-session solver image exceeds the size limit"
        )
    actual_mime = content_type.split(";", 1)[0].strip().casefold()
    expected_mime = expected_content_type.split(";", 1)[0].strip().casefold()
    if (
        len(raw) != expected_bytes
        or actual_mime != expected_mime
        or not solver_image_bytes_match_mime(raw, expected_mime)
        or hashlib.sha256(raw).hexdigest() != expected_sha256
    ):
        raise LiteratureBrowserError(
            "same-session solver image failed integrity validation"
        )
    return raw


def request_flaresolverr_same_session_download(
    article_target: str,
    pdf_target: str,
    *,
    queue_timeout_seconds: float | None = None,
    max_timeout_ms: int | None = None,
) -> tuple[bytes, str, str, int]:
    raise LiteratureBrowserError(
        "direct solver downloads are disabled; hand off the solved session to "
        "Playwright and download there"
    )
    if not FLARESOLVERR_URL:
        raise LiteratureBrowserError("publisher challenge solver is not configured")
    article_url = strip_transient_challenge_query(article_target)
    pdf_url = validate_url(pdf_target)
    article_host = urlparse(article_url).hostname or ""
    pdf_host = urlparse(pdf_url).hostname or ""
    if not (
        flaresolverr_host_allowed(article_host)
        and flaresolverr_host_allowed(pdf_host)
    ):
        raise LiteratureBrowserError(
            "same-session solver download is outside its allowlist"
        )
    publisher = flaresolverr_publisher_slug(article_url)
    if publisher != flaresolverr_publisher_slug(pdf_url):
        raise LiteratureBrowserError(
            "same-session solver article and PDF must use the same publisher"
        )
    effective_total_timeout_ms = min(
        FLARESOLVERR_MAX_TIMEOUT_MS,
        max(30000, int(max_timeout_ms or FLARESOLVERR_MAX_TIMEOUT_MS)),
    )
    if bool(getattr(_FLARESOLVERR_REQUEST_CONTEXT, "maintenance", False)):
        effective_total_timeout_ms = min(
            effective_total_timeout_ms,
            FLARESOLVERR_MAINTENANCE_MAX_TIMEOUT_MS,
        )
    persistent_session = publisher == "acs"
    session_id = (
        publisher_flaresolverr_session_id(publisher)
        if persistent_session
        else f"scientist-{publisher}-{uuid.uuid4().hex}"
    )
    created = False
    with flaresolverr_request_lock(queue_timeout_seconds):
        operation_deadline = (
            time.monotonic() + (effective_total_timeout_ms / 1000)
        )
        try:
            create_timeout_seconds = min(
                30.0,
                max(5.0, operation_deadline - time.monotonic()),
            )
            if persistent_session:
                ensure_flaresolverr_session(session_id)
            else:
                created_result = flaresolverr_api(
                    {"cmd": "sessions.create", "session": session_id},
                    timeout_seconds=create_timeout_seconds,
                )
                if created_result.get("status") != "ok":
                    raise LiteratureBrowserError(
                        "publisher challenge solver session creation failed"
                    )
                created = True
            article_result = flaresolverr_api(
                {
                    "cmd": "request.get",
                    "url": article_url,
                    "session": session_id,
                    "session_ttl_minutes": FLARESOLVERR_SESSION_TTL_MINUTES,
                    "maxTimeout": min(
                        90000,
                        max(
                            30000,
                            int(
                                max(
                                    0.0,
                                    operation_deadline - time.monotonic() - 30.0,
                                )
                                * 1000
                            ),
                        ),
                    ),
                    "waitInSeconds": FLARESOLVERR_WAIT_SECONDS,
                    "disableMedia": True,
                },
                timeout_seconds=min(
                    105.0,
                    max(45.0, operation_deadline - time.monotonic()),
                ),
            )
            article_solution = article_result.get("solution") or {}
            article_status = article_solution.get("status")
            if (
                article_result.get("status") != "ok"
                or not isinstance(article_status, int)
                or article_status >= 400
            ):
                message = str(
                    article_result.get("message")
                    or "publisher article challenge was not solved"
                )
                raise LiteratureBrowserError(
                    f"same-session solver article initialization failed: "
                    f"{message[:300]}"
                )
            article_final = validate_url(
                str(article_solution.get("url") or article_url)
            )
            if not flaresolverr_host_allowed(
                urlparse(article_final).hostname or ""
            ):
                raise LiteratureBrowserError(
                    "same-session solver article redirected outside its allowlist"
                )
            download_timeout_ms = int(
                max(0.0, operation_deadline - time.monotonic() - 15.0) * 1000
            )
            if download_timeout_ms < 30000:
                raise LiteratureBrowserError(
                    "same-session solver has insufficient time for PDF download"
                )
            download_result = flaresolverr_api(
                {
                    "cmd": "request.download",
                    "url": pdf_url,
                    "session": session_id,
                    "maxTimeout": download_timeout_ms,
                },
                timeout_seconds=(download_timeout_ms / 1000) + 10,
            )
            solution = download_result.get("solution") or {}
            status = solution.get("status")
            if (
                download_result.get("status") != "ok"
                or not isinstance(status, int)
                or status >= 400
            ):
                message = str(
                    download_result.get("message")
                    or "same-session PDF download failed"
                )
                raise LiteratureBrowserError(
                    f"same-session solver PDF download failed: {message[:300]}"
                )
            try:
                expected_bytes = int(solution.get("bytes") or 0)
            except (TypeError, ValueError):
                expected_bytes = 0
            expected_sha256 = str(solution.get("sha256") or "").lower()
            download_id = str(solution.get("downloadId") or "")
            if (
                expected_bytes < 4096
                or expected_bytes > MAX_PDF_BYTES
                or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)
            ):
                raise LiteratureBrowserError(
                    "same-session solver returned invalid PDF metadata"
                )
            raw = flaresolverr_download_artifact(
                download_id,
                expected_bytes=expected_bytes,
                expected_sha256=expected_sha256,
                timeout_seconds=min(
                    120.0,
                    max(5.0, operation_deadline - time.monotonic()),
                ),
            )
            final_url = validate_url(str(solution.get("url") or pdf_url))
            if not flaresolverr_host_allowed(urlparse(final_url).hostname or ""):
                raise LiteratureBrowserError(
                    "same-session solver PDF redirected outside its allowlist"
                )
            return raw, "application/pdf", final_url, status
        finally:
            if created:
                try:
                    flaresolverr_api(
                        {"cmd": "sessions.destroy", "session": session_id},
                        timeout_seconds=30,
                    )
                except LiteratureBrowserError:
                    pass


def request_flaresolverr_same_session_article(
    target: str,
    *,
    figure_offset: int,
    max_figures: int,
    queue_timeout_seconds: float | None = None,
    max_timeout_ms: int | None = None,
) -> dict[str, Any]:
    raise LiteratureBrowserError(
        "direct solver article extraction is disabled; hand off the solved "
        "session to Playwright"
    )
    if not FLARESOLVERR_URL:
        raise LiteratureBrowserError("publisher challenge solver is not configured")
    article_url = strip_transient_challenge_query(target)
    article_host = urlparse(article_url).hostname or ""
    if not flaresolverr_host_allowed(article_host):
        raise LiteratureBrowserError(
            "same-session solver article is outside its allowlist"
        )
    publisher = flaresolverr_publisher_slug(article_url)
    effective_total_timeout_ms = min(
        FLARESOLVERR_MAX_TIMEOUT_MS,
        max(30000, int(max_timeout_ms or FLARESOLVERR_MAX_TIMEOUT_MS)),
    )
    if bool(getattr(_FLARESOLVERR_REQUEST_CONTEXT, "maintenance", False)):
        effective_total_timeout_ms = min(
            effective_total_timeout_ms,
            FLARESOLVERR_MAINTENANCE_MAX_TIMEOUT_MS,
        )
    persistent_session = publisher == "acs"
    session_id = (
        publisher_flaresolverr_session_id(publisher)
        if persistent_session
        else f"scientist-{publisher}-figures-{uuid.uuid4().hex}"
    )
    created = False
    with flaresolverr_request_lock(queue_timeout_seconds):
        operation_deadline = time.monotonic() + (effective_total_timeout_ms / 1000)
        try:
            if persistent_session:
                ensure_flaresolverr_session(session_id)
            else:
                created_result = flaresolverr_api(
                    {"cmd": "sessions.create", "session": session_id},
                    timeout_seconds=min(
                        30.0,
                        max(5.0, operation_deadline - time.monotonic()),
                    ),
                )
                if created_result.get("status") != "ok":
                    raise LiteratureBrowserError(
                        "publisher challenge solver session creation failed"
                    )
                created = True
            article_timeout_ms = min(
                120000,
                max(
                    30000,
                    int(max(0.0, operation_deadline - time.monotonic() - 30.0) * 1000),
                ),
            )
            article_result = flaresolverr_api(
                {
                    "cmd": "request.get",
                    "url": article_url,
                    "session": session_id,
                    "session_ttl_minutes": FLARESOLVERR_SESSION_TTL_MINUTES,
                    "maxTimeout": article_timeout_ms,
                    "waitInSeconds": FLARESOLVERR_WAIT_SECONDS,
                    "disableMedia": False,
                },
                timeout_seconds=min(
                    (article_timeout_ms / 1000) + 15,
                    max(45.0, operation_deadline - time.monotonic()),
                ),
            )
            solution = article_result.get("solution") or {}
            status = solution.get("status")
            if (
                article_result.get("status") != "ok"
                or not isinstance(status, int)
                or status >= 400
            ):
                message = str(
                    article_result.get("message")
                    or "publisher article challenge was not solved"
                )
                raise LiteratureBrowserError(
                    "same-session solver article initialization failed: "
                    + message[:300]
                )
            final_url = validate_url(str(solution.get("url") or article_url))
            if (
                not flaresolverr_host_allowed(urlparse(final_url).hostname or "")
                or flaresolverr_publisher_slug(final_url) != publisher
            ):
                raise LiteratureBrowserError(
                    "same-session solver article redirected outside its publisher"
                )
            markup = str(solution.get("response") or "")
            if not markup.strip():
                raise LiteratureBrowserError(
                    "same-session solver article returned no HTML"
                )
            figure_extraction = extract_figure_manifest_from_markup(
                markup,
                final_url,
                figure_offset=figure_offset,
                max_figures=max_figures,
            )
            figures = [dict(item) for item in figure_extraction.get("items") or []]
            for index, figure in enumerate(figures):
                source = str(figure.get("src") or "").strip()
                if not source:
                    raise LiteratureBrowserError(
                        "same-session solver figure has no governed image URL"
                    )
                source_url = validate_url(urljoin(final_url, source))
                if (
                    not flaresolverr_host_allowed(
                        urlparse(source_url).hostname or ""
                    )
                    or flaresolverr_publisher_slug(source_url) != publisher
                ):
                    raise LiteratureBrowserError(
                        "same-session solver figure is outside the article publisher"
                    )
                remaining_count = max(1, len(figures) - index)
                remaining_seconds = operation_deadline - time.monotonic()
                asset_timeout_ms = min(
                    60000,
                    int(max(0.0, remaining_seconds - 10.0) * 1000 / remaining_count),
                )
                if asset_timeout_ms < 10000:
                    raise LiteratureBrowserError(
                        "same-session solver has insufficient time for figure capture"
                    )
                asset_result = flaresolverr_api(
                    {
                        "cmd": "request.asset",
                        "url": source_url,
                        "session": session_id,
                        "maxTimeout": asset_timeout_ms,
                    },
                    timeout_seconds=(asset_timeout_ms / 1000) + 10,
                )
                asset = asset_result.get("solution") or {}
                asset_status = asset.get("status")
                if (
                    asset_result.get("status") != "ok"
                    or not isinstance(asset_status, int)
                    or asset_status >= 400
                ):
                    message = str(
                        asset_result.get("message")
                        or "same-session figure capture failed"
                    )
                    raise LiteratureBrowserError(
                        "same-session solver figure capture failed: "
                        + message[:300]
                    )
                try:
                    expected_bytes = int(asset.get("bytes") or 0)
                except (TypeError, ValueError):
                    expected_bytes = 0
                expected_sha256 = str(asset.get("sha256") or "").lower()
                content_type = str(asset.get("contentType") or "").split(
                    ";", 1
                )[0].strip().casefold()
                asset_id = str(asset.get("assetId") or "")
                if (
                    expected_bytes < 1
                    or expected_bytes > MAX_FIGURE_IMAGE_BYTES
                    or not content_type.startswith("image/")
                    or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)
                ):
                    raise LiteratureBrowserError(
                        "same-session solver returned invalid image metadata"
                    )
                raw = flaresolverr_image_artifact(
                    asset_id,
                    expected_bytes=expected_bytes,
                    expected_sha256=expected_sha256,
                    expected_content_type=content_type,
                    timeout_seconds=min(
                        60.0,
                        max(5.0, operation_deadline - time.monotonic()),
                    ),
                )
                asset_final_url = validate_url(
                    str(asset.get("url") or source_url)
                )
                if (
                    not flaresolverr_host_allowed(
                        urlparse(asset_final_url).hostname or ""
                    )
                    or flaresolverr_publisher_slug(asset_final_url) != publisher
                ):
                    raise LiteratureBrowserError(
                        "same-session solver figure redirected outside its publisher"
                    )
                figure.update(
                    {
                        "src": asset_final_url,
                        "image_base64": base64.b64encode(raw).decode("ascii"),
                        "mime_type": content_type,
                        "image_bytes": len(raw),
                        "image_extraction_method": "flaresolverr_same_session_asset",
                        "image_access_state": "available_same_session",
                    }
                )
            session_state = SolverSessionState(
                user_agent=str(solution.get("userAgent") or "").strip(),
                cookies=tuple(
                    normalize_flaresolverr_cookies(
                        solution.get("cookies") or [],
                        publisher,
                    )
                ),
            )
            return {
                "final_url": final_url,
                "status": status,
                "html": markup,
                "solver_mode": "named_same_session_assets",
                "session_state": session_state,
                "figures": figures,
                "figure_extraction": {
                    key: figure_extraction[key]
                    for key in ("total", "offset", "returned", "has_more")
                    if key in figure_extraction
                },
            }
        finally:
            if created:
                try:
                    flaresolverr_api(
                        {"cmd": "sessions.destroy", "session": session_id},
                        timeout_seconds=30,
                    )
                except LiteratureBrowserError:
                    pass


def request_publisher_flaresolverr(
    target: str,
    *,
    queue_timeout_seconds: float | None = None,
    max_timeout_ms: int | None = None,
    allow_retry: bool = False,
    include_figure_images: bool = False,
    figure_offset: int = 0,
    max_figures: int = 12,
) -> dict[str, Any]:
    if include_figure_images:
        raise LiteratureBrowserError(
            "challenge solver asset extraction is disabled; apply the profile "
            "handoff and extract assets with Playwright"
        )
    errors: list[str] = []
    publisher = flaresolverr_publisher_slug(target)
    total_timeout_ms = min(
        FLARESOLVERR_ATTEMPT_TIMEOUT_MS,
        max_timeout_ms or FLARESOLVERR_ATTEMPT_TIMEOUT_MS,
    )
    deadline = time.monotonic() + (total_timeout_ms / 1000)
    endpoints = (FLARESOLVERR_URL, *FLARESOLVERR_FALLBACK_URLS)
    attempt_limit = max(2, len(endpoints)) if allow_retry else 1
    for attempt in range(1, attempt_limit + 1):
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds < 30:
            errors.append(f"attempt-{attempt}: shared solver deadline exhausted")
            break
        queue_budget = min(
            float(queue_timeout_seconds or FLARESOLVERR_QUEUE_SECONDS),
            max(1.0, min(30.0, remaining_seconds / 5)),
        )
        usable_seconds = remaining_seconds - queue_budget - 15.0
        if len(endpoints) > 1:
            remaining_attempts = attempt_limit - attempt + 1
            usable_seconds /= remaining_attempts
        attempt_timeout_ms = max(10000, int(usable_seconds * 1000))
        try:
            session_id = publisher_flaresolverr_session_id(publisher)
            endpoint = endpoints[(attempt - 1) % len(endpoints)]
            with flaresolverr_endpoint(endpoint):
                solved = request_flaresolverr(
                    target,
                    queue_timeout_seconds=queue_budget,
                    max_timeout_ms=attempt_timeout_ms,
                    session_id=session_id,
                )
            return solved
        except LiteratureBrowserError as exc:
            message = str(exc)
            errors.append(f"attempt-{attempt}: {message[:240]}")
            retryable = any(
                marker in message.casefold()
                for marker in (
                    "solver request failed",
                    "solver failed",
                    "solver returned http",
                    "did not return same-session content",
                )
            )
            if not retryable:
                raise
    raise LiteratureBrowserError(
        f"{publisher} publisher challenge attempts exhausted: " + "; ".join(errors)
    )


def request_acs_flaresolverr(
    target: str,
    *,
    queue_timeout_seconds: float | None = None,
    max_timeout_ms: int | None = None,
    allow_retry: bool = True,
) -> dict[str, Any]:
    if flaresolverr_publisher_slug(target) != "acs":
        raise LiteratureBrowserError("ACS solver received a non-ACS target")
    return request_publisher_flaresolverr(
        target,
        queue_timeout_seconds=queue_timeout_seconds,
        max_timeout_ms=max_timeout_ms,
        allow_retry=allow_retry,
    )


def sciencedirect_problem_shell(text: str) -> bool:
    lowered = text.casefold()
    return (
        "there was a problem providing the content you requested" in lowered
        and "reference number:" in lowered
        and "user agent:" in lowered
    )


def classify_page(final_url: str, title: str, text: str) -> str:
    combined = f"{final_url}\n{title}\n{text[:12000]}".lower()
    if any(
        marker in combined
        for marker in (
            "just a moment",
            "verify you are human",
            "performing security verification",
            "review the security of your connection",
            "checking your browser",
            "security verification",
            "enable javascript and cookies to continue",
            "正在进行安全验证",
            "cf-chl-",
            "cloudflare",
        )
    ):
        return "challenge"
    host = urlparse(final_url).hostname or ""
    if host_in_domains(host, ("sciencedirect.com", "elsevier.com")):
        if sciencedirect_problem_shell(text):
            return "challenge"
    if any(
        marker in combined
        for marker in (
            "idp.xmu.edu.cn",
            "统一身份认证",
            "xiamen university login",
        )
    ):
        return "login_required"
    return "content"


def page_read_succeeded(state: str) -> bool:
    return state == "content"


def access_signals(text: str) -> list[str]:
    lowered = text.lower()
    markers = (
        "Access provided by",
        "Xiamen University",
        "XIAMEN UNIV",
        "厦门大学",
        "Access through your institution",
        "Institutional access",
        "Open access",
        "Full Access",
        "Open PDF",
        "View PDF",
        "Download PDF",
        "Read the full text",
        "Sign in via your institution",
        "Get access",
        "Purchase access",
    )
    return [marker for marker in markers if marker.lower() in lowered]


FULL_TEXT_SECTION_GROUPS = (
    ("abstract",),
    ("significance",),
    ("introduction", "background"),
    ("results", "results and discussion"),
    ("discussion",),
    (
        "materials and methods",
        "methods",
        "experimental section",
        "experimental",
    ),
    ("conclusion", "conclusions"),
    ("references", "bibliography"),
)


def section_heading_count(text: str) -> int:
    normalized_lines = {
        re.sub(
            r"^\s*(?:\d+(?:\.\d+)*[.)]?\s+)?",
            "",
            line.strip().lower().rstrip(":"),
        )
        for line in text.splitlines()
        if line.strip()
    }
    return sum(
        any(heading in normalized_lines for heading in group)
        for group in FULL_TEXT_SECTION_GROUPS
    )


def full_text_visible(text: str) -> bool:
    text_length = len(text.strip())
    sections = section_heading_count(text)
    return (
        (text_length >= 12000 and sections >= 4)
        or (text_length >= 30000 and sections >= 3)
    )


FULL_TEXT_DOM_SCRIPT = """() => Array.from(document.querySelectorAll(
  '.widget-ArticleFulltext, .article-body, .article__body, '
  + '.article-section__content, .Body, #body, '
  + '.c-article-body, #main-content, main article, '
  + '[data-aa-name="articleBody"]'
)).some((node) => (node.innerText || '').trim().length >= 8000)"""


def publisher_shell_needs_settle(
    final_url: str,
    title: str,
    text: str,
    page_state: str,
    *,
    full_text_dom: bool = False,
) -> bool:
    if full_text_dom or full_text_visible(text):
        return False
    host = urlparse(final_url).hostname or ""
    if host_in_domains(host, ("rsc.org",)) and page_state == "challenge":
        return True
    if host_in_domains(host, ("sciencedirect.com", "elsevier.com")):
        return page_state == "challenge" or sciencedirect_problem_shell(text)
    return False


def extract_structured_html_article(
    markup: str,
    final_url: str,
    max_chars: int,
) -> dict[str, Any]:
    host = urlparse(final_url).hostname or ""
    if not host_in_domains(
        host,
        (
            "wiley.com",
            "springer.com",
            "springernature.com",
            "nature.com",
            "rsc.org",
            "acs.org",
        ),
    ):
        return {"attempted": False, "success": False, "candidates": []}
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(markup, "html.parser")
    candidate_texts: list[tuple[str, str, int]] = []
    for node in soup.select('script[type="application/ld+json"]'):
        try:
            payload = json.loads(node.get_text(strip=True))
        except (TypeError, json.JSONDecodeError):
            continue
        stack = [payload]
        while stack:
            value = stack.pop()
            if isinstance(value, dict):
                article_body = value.get("articleBody")
                if isinstance(article_body, str) and article_body.strip():
                    candidate_texts.append(
                        ("json_ld_article_body", article_body, 1)
                    )
                stack.extend(value.values())
            elif isinstance(value, list):
                stack.extend(value)

    for node in soup.select(
        "script, style, noscript, nav, header, footer, aside, form, button"
    ):
        node.decompose()
    selectors = (
        "article",
        ".article__body",
        ".article-body",
        "#article-section__full",
        ".article-section__content",
        ".article-section",
        '[class*="article-section"]',
        ".c-article-body",
        ".widget-ArticleFulltext",
        ".Body",
        "#body",
        '[data-aa-name="articleBody"]',
        ".main-content",
        "#main-content",
        "main",
    )
    for selector in selectors:
        nodes = soup.select(selector)
        if not nodes:
            continue
        text = "\n\n".join(
            value
            for value in (
                node.get_text("\n", strip=True)
                for node in nodes
            )
            if value
        )
        if text:
            candidate_texts.append((selector, text, len(nodes)))

    diagnostics: list[dict[str, Any]] = []
    ranked: list[tuple[int, str, str, int, bool]] = []
    wiley_section_nodes = len(soup.select(".article-section__content"))
    springer_section_nodes = len(
        soup.select(
            ".c-article-body section, "
            ".c-article-section, "
            "section[data-title]"
        )
    )
    acs_section_nodes = len(
        soup.select(
            ".widget-ArticleFulltext section, "
            ".Body section, "
            "#body section, "
            '[data-aa-name="articleBody"] section, '
            ".article-body section"
        )
    )
    selector_priority = {
        ".article__body": 300000,
        ".article-body": 300000,
        ".article-section__content": 200000,
        ".c-article-body": 300000,
        ".widget-ArticleFulltext": 300000,
        ".Body": 300000,
        "#body": 250000,
        '[data-aa-name="articleBody"]': 300000,
        "#main-content": 150000,
        ".main-content": 150000,
        "article": 100000,
    }
    for selector, raw_text, node_count in candidate_texts:
        text, _truncated = trim_text(raw_text, max_chars)
        if not text:
            continue
        sections = section_heading_count(text)
        structured = full_text_visible(text)
        wiley_structured_body = (
            (
                selector == ".article__body"
                and wiley_section_nodes >= 3
                and len(text) >= 20000
            )
            or (
                selector == ".article-section__content"
                and node_count >= 3
                and len(text) >= 12000
            )
        )
        springer_structured_body = (
            host_in_domains(
                host,
                ("springer.com", "springernature.com", "nature.com"),
            )
            and selector
            in {
                ".c-article-body",
                ".main-content",
                "#main-content",
                "article",
                "main",
            }
            and len(text) >= 12000
            and (springer_section_nodes >= 3 or sections >= 3)
        )
        rsc_structured_body = (
            host_in_domains(host, ("rsc.org",))
            and selector == ".article-body"
            and (
                len(text) >= 12000
                or (len(text) >= 3000 and sections >= 1)
                or len(text) >= 5000
            )
        )
        acs_structured_body = (
            host_in_domains(host, ("acs.org",))
            and selector
            in {
                ".widget-ArticleFulltext",
                ".Body",
                "#body",
                '[data-aa-name="articleBody"]',
                ".article-body",
                "article",
                "main",
            }
            and len(text) >= 12000
            and (acs_section_nodes >= 3 or sections >= 3)
        )
        publisher_structured_body = (
            wiley_structured_body
            or springer_structured_body
            or rsc_structured_body
            or acs_structured_body
        )
        usable = structured or publisher_structured_body
        score = (
            (1000000 if usable else 0)
            + selector_priority.get(selector, 0)
            + (sections * 100000)
            + len(text)
        )
        diagnostics.append(
            {
                "selector": selector,
                "node_count": node_count,
                "text_chars": len(text),
                "section_headings": sections,
                "structured_full_text": structured,
                "publisher_structured_body": publisher_structured_body,
                "usable_article": usable,
            }
        )
        ranked.append((score, selector, text, sections, usable))
    diagnostics.sort(
        key=lambda item: (
            bool(item["usable_article"]),
            int(item["section_headings"]),
            int(item["text_chars"]),
        ),
        reverse=True,
    )
    if not ranked:
        return {
            "attempted": True,
            "success": False,
            "candidates": [],
        }
    ranked.sort(reverse=True)
    _score, selector, text, sections, usable = ranked[0]
    return {
        "attempted": True,
        "success": usable,
        "selector": selector,
        "text": text if usable else "",
        "text_chars": len(text),
        "section_headings": sections,
        "candidates": diagnostics[:8],
    }


def classify_access(
    page_state: str,
    text: str,
    signals: list[str],
    *,
    full_text_dom: bool = False,
) -> str:
    if page_state != "content":
        return page_state
    signal_set = {signal.lower() for signal in signals}
    institution = bool(
        signal_set
        & {
            "access provided by",
            "xiamen university",
            "xiamen univ",
            "厦门大学",
            "access through your institution",
            "institutional access",
        }
    )
    actual_full_text = full_text_dom or full_text_visible(text)
    if institution and actual_full_text:
        return "institutional_full_text"
    if "full access" in signal_set and actual_full_text:
        return "publisher_full_text"
    if "open access" in signal_set and actual_full_text:
        return "open_access_full_text"
    if actual_full_text:
        return "full_text_visible"
    if signal_set & {
        "sign in via your institution",
        "get access",
        "purchase access",
    }:
        return "restricted_or_signin"
    return "metadata_or_abstract"


def select_pdf_candidates(links: list[dict[str, str]]) -> list[str]:
    ranked: list[tuple[int, str]] = []
    seen: set[str] = set()
    for item in links:
        href = str(item.get("href") or "").strip()
        label = str(item.get("text") or "").strip().lower()
        if not href or href in seen:
            continue
        try:
            target = validate_url(href)
        except LiteratureBrowserError:
            continue
        combined = f"{target} {label}".lower()
        if not re.search(r"\bpdf\b|\.pdf(?:[?#]|$)", combined):
            continue
        if is_supplementary_pdf_url(target) or re.search(
            r"supplement|supporting|\besi\b|additional file",
            combined,
        ):
            continue
        score = 0
        if re.search(r"\b(?:download|view|open)\s+pdf\b", label):
            score += 8
        if ".pdf" in urlparse(target).path.lower():
            score += 5
        if "article" in combined or "full" in combined:
            score += 2
        ranked.append((score, target))
        seen.add(target)
    ranked.sort(key=lambda value: value[0], reverse=True)
    return [target for _score, target in ranked[:MAX_PDF_CANDIDATES]]


FIGURE_EXTRACTION_SCRIPT = r"""({offset, limit}) => {
  const clean = (value, maxLength) => String(value || '')
    .replace(/\s+/g, ' ').trim().slice(0, maxLength);
  const sourceFor = (image) => {
    if (!image) return '';
    const srcset = image.getAttribute('srcset') || image.getAttribute('data-srcset') || '';
    const srcsetCandidates = srcset.split(',').map((item) => item.trim().split(/\s+/)[0]).filter(Boolean);
    return image.getAttribute('data-src') || image.getAttribute('data-original') ||
      image.getAttribute('data-lazy-src') ||
      srcsetCandidates[srcsetCandidates.length - 1] || image.currentSrc || image.src || '';
  };
  const captionSelectors = [
    'figcaption', '.caption', '.figure__caption', '.figure-caption',
    '.NLM_caption', '.caption-text', '.graphic_title',
    '[data-test="bottom-caption"]', '[class*="caption" i]'
  ];
  const containerSelectors = [
    'figure', '.NLM_fig', '.figure', '.figure-wrap', '.figureWrapper',
    '.article__figure', '.article-figure', '.image_table', '.imgHolder',
    '[data-test*="figure" i]', '[data-testid*="figure" i]',
    '[id^="fig" i]', '[id^="scheme" i]'
  ];
  const containers = [];
  const seenContainers = new Set();
  for (const selector of containerSelectors) {
    for (const node of document.querySelectorAll(selector)) {
      if (seenContainers.has(node)) continue;
      seenContainers.add(node);
      containers.push(node);
    }
  }
  const candidates = [];
  const seen = new Set();
  const addCandidate = (container, image, method) => {
    const src = sourceFor(image);
    let caption = '';
    for (const selector of captionSelectors) {
      const node = container?.querySelector?.(selector);
      if (node) {
        caption = clean(node.innerText || node.textContent, 8000);
        if (caption) break;
      }
    }
    const alt = clean(image?.getAttribute?.('alt'), 2000);
    if (!caption && container && container !== image) {
      const text = clean(container.innerText || container.textContent, 8000);
      if (/^(figure|fig\.|scheme|chart|table)\s*\d+/i.test(text)) caption = text;
    }
    if (!src && !caption) return;
    const lower = `${src} ${alt} ${caption}`.toLowerCase();
    if (!caption && /logo|icon|avatar|author|spinner|tracking|pixel/.test(lower)) return;
    let sourceKey = src;
    try {
      const sourceUrl = new URL(src, document.baseURI);
      sourceKey = `${sourceUrl.origin}${sourceUrl.pathname}`;
    } catch (_error) {}
    const key = sourceKey || caption;
    if (seen.has(key)) return;
    seen.add(key);
    const kindMatch = caption.match(/^\s*(figure|fig\.|scheme|chart|table)\b/i);
    candidates.push({
      src,
      alt,
      caption,
      kind: kindMatch ? kindMatch[1].replace('.', '').toLowerCase() : 'figure',
      extraction_method: method,
      image
    });
  };
  for (const container of containers) {
    const image = container.matches?.('img') ? container :
      container.querySelector('img, picture img, svg[role="img"], object[type="image/svg+xml"]');
    addCandidate(container, image, 'semantic_container');
  }
  for (const image of document.querySelectorAll(
    'article img, main img, .article-body img, .article__body img, .widget-ArticleFulltext img'
  )) {
    const container = image.closest(containerSelectors.join(',')) || image.parentElement || image;
    const width = Number(image.naturalWidth || image.width || 0);
    const height = Number(image.naturalHeight || image.height || 0);
    const context = clean(`${image.alt || ''} ${container?.innerText || ''}`, 1000);
    if (width < 120 && height < 120 && !/(figure|scheme|chart|crystal)/i.test(context)) continue;
    addCandidate(container, image, 'article_image_fallback');
  }
  candidates.forEach((item, index) => {
    item.index = index;
    if (item.image?.setAttribute) {
      item.image.setAttribute('data-scientist-figure-index', String(index));
    }
  });
  const start = Math.max(0, Number(offset) || 0);
  const count = Math.max(1, Math.min(Number(limit) || 12, 30));
  const items = candidates.slice(start, start + count).map(({image, ...item}) => item);
  return {
    items,
    total: candidates.length,
    offset: start,
    returned: items.length,
    has_more: start + items.length < candidates.length
  };
}"""


def extract_figure_manifest(
    page: Any,
    *,
    figure_offset: int,
    max_figures: int,
) -> dict[str, Any]:
    value = page.evaluate(
        FIGURE_EXTRACTION_SCRIPT,
        {
            "offset": max(0, figure_offset),
            "limit": max(1, min(max_figures, MAX_FIGURES_PER_READ)),
        },
    )
    if not isinstance(value, dict) or not isinstance(value.get("items"), list):
        return {
            "items": [],
            "total": 0,
            "offset": max(0, figure_offset),
            "returned": 0,
            "has_more": False,
        }
    return value


def extract_figure_manifest_from_markup(
    markup: str,
    final_url: str,
    *,
    figure_offset: int,
    max_figures: int,
) -> dict[str, Any]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(markup, "html.parser")
    container_selectors = (
        "figure",
        ".NLM_fig",
        ".figure",
        ".figure-wrap",
        ".figureWrapper",
        ".article__figure",
        ".article-figure",
        ".image_table",
        ".imgHolder",
        '[data-test*="figure" i]',
        '[data-testid*="figure" i]',
        '[id^="fig" i]',
        '[id^="scheme" i]',
    )
    caption_selectors = (
        "figcaption",
        ".caption",
        ".figure__caption",
        ".figure-caption",
        ".NLM_caption",
        ".caption-text",
        ".graphic_title",
        '[data-test="bottom-caption"]',
        '[class*="caption" i]',
    )

    def clean(value: Any, limit: int) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]

    def image_source(image: Any) -> str:
        if image is None:
            return ""
        for attribute in (
            "data-hi-res-src",
            "data-src",
            "data-original",
            "data-lazy-src",
        ):
            value = clean(image.get(attribute), 4096)
            if value:
                return value
        srcset = clean(
            image.get("srcset") or image.get("data-srcset"),
            12000,
        )
        if srcset:
            values = [
                item.strip().split()[0]
                for item in srcset.split(",")
                if item.strip()
            ]
            if values:
                return values[-1]
        value = clean(image.get("src") or image.get("data"), 4096)
        if value:
            return value
        picture = image.find_parent("picture")
        source = picture.select_one("source[srcset], source[data-srcset]") if picture else None
        if source is not None:
            values = [
                item.strip().split()[0]
                for item in clean(
                    source.get("srcset") or source.get("data-srcset"),
                    12000,
                ).split(",")
                if item.strip()
            ]
            if values:
                return values[-1]
        return ""

    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_candidate(container: Any, image: Any, method: str) -> None:
        source = image_source(image)
        caption = ""
        for selector in caption_selectors:
            node = container.select_one(selector) if container is not None else None
            if node is not None:
                caption = clean(node.get_text(" ", strip=True), 8000)
                if caption:
                    break
        alt = clean(image.get("alt") if image is not None else "", 2000)
        if not caption and container is not None:
            container_text = clean(container.get_text(" ", strip=True), 8000)
            if re.match(r"^(figure|fig\.|scheme|chart|table)\s*\d+", container_text, re.I):
                caption = container_text
        if not source and not caption:
            return
        if source:
            try:
                source = validate_url(urljoin(final_url, html.unescape(source)))
            except LiteratureBrowserError:
                return
        combined = f"{source} {alt} {caption}".casefold()
        if not caption and re.search(
            r"logo|icon|avatar|author|spinner|tracking|pixel",
            combined,
        ):
            return
        parsed = urlparse(source) if source else None
        key = (
            f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if parsed is not None
            else caption
        )
        if key in seen:
            return
        seen.add(key)
        kind_match = re.match(
            r"^\s*(figure|fig\.|scheme|chart|table)\b",
            caption,
            re.I,
        )
        candidates.append(
            {
                "src": source,
                "alt": alt,
                "caption": caption,
                "kind": (
                    kind_match.group(1).replace(".", "").lower()
                    if kind_match
                    else "figure"
                ),
                "extraction_method": method,
            }
        )

    containers: list[Any] = []
    seen_containers: set[int] = set()
    for selector in container_selectors:
        for container in soup.select(selector):
            identity = id(container)
            if identity in seen_containers:
                continue
            seen_containers.add(identity)
            containers.append(container)
    for container in containers:
        image = (
            container
            if getattr(container, "name", "") in {"img", "object"}
            else container.select_one(
                'img, picture img, svg[role="img"], object[type="image/svg+xml"]'
            )
        )
        add_candidate(container, image, "solver_html_semantic_container")
    for image in soup.select(
        "article img, main img, .article-body img, .article__body img, "
        ".widget-ArticleFulltext img, .Body img, #body img"
    ):
        container = image.find_parent(
            ["figure"]
        ) or image.parent
        context = clean(
            f"{image.get('alt') or ''} "
            f"{container.get_text(' ', strip=True) if container else ''}",
            1000,
        )
        if not re.search(r"figure|scheme|chart|graphic|crystal|visual abstract", context, re.I):
            continue
        add_candidate(container or image, image, "solver_html_article_image_fallback")

    for index, candidate in enumerate(candidates):
        candidate["index"] = index
    start = max(0, int(figure_offset))
    count = max(1, min(int(max_figures), MAX_FIGURES_PER_READ))
    items = candidates[start:start + count]
    return {
        "items": items,
        "total": len(candidates),
        "offset": start,
        "returned": len(items),
        "has_more": start + len(items) < len(candidates),
        "source": "challenge_solver_html",
    }


def attach_figure_images(
    page: Any,
    figures: list[dict[str, Any]],
    *,
    request_headers: dict[str, str] | None = None,
    allow_rendered: bool = True,
) -> None:
    for figure in figures:
        fetch_error = ""
        source = str(figure.get("src") or "").strip()
        if source:
            response = None
            try:
                target = validate_url(urljoin(page.url, source))
                request_options: dict[str, Any] = {"timeout": 20000}
                if request_headers:
                    request_options["headers"] = request_headers
                response = page.context.request.get(target, **request_options)
                content_type = str(response.headers.get("content-type") or "")
                mime_type = content_type.split(";", 1)[0].strip().casefold()
                if response.ok and mime_type.startswith("image/"):
                    raw = response.body()
                    if raw and len(raw) <= MAX_FIGURE_IMAGE_BYTES:
                        figure["image_base64"] = base64.b64encode(raw).decode("ascii")
                        figure["mime_type"] = mime_type
                        figure["image_bytes"] = len(raw)
                        figure["image_extraction_method"] = (
                            "profile_authenticated_fetch"
                        )
                        figure["image_access_state"] = "available"
                        continue
                    fetch_error = "publisher image exceeds the image size limit"
                    figure["image_access_state"] = "publisher_asset_invalid"
                else:
                    fetch_error = (
                        f"publisher image fetch returned {response.status} {content_type}"
                    )
                    figure["image_access_state"] = (
                        "publisher_asset_challenge"
                        if response.status in {401, 403, 429}
                        else "publisher_asset_http_error"
                    )
            except Exception as exc:
                fetch_error = re.sub(r"\s+", " ", str(exc)).strip()[:180]
                figure["image_access_state"] = "publisher_asset_error"
            finally:
                if response is not None:
                    try:
                        response.dispose()
                    except Exception:
                        pass
        else:
            fetch_error = "publisher figure does not expose an image URL"
            figure["image_access_state"] = "publisher_asset_missing_url"
        if not allow_rendered:
            figure["image_error"] = fetch_error[:240]
            continue
        try:
            index = int(figure.get("index"))
            locator = page.locator(
                f'[data-scientist-figure-index="{index}"]'
            ).first
            if locator.count() < 1:
                figure["image_error"] = "; ".join(
                    part
                    for part in (
                        fetch_error,
                        "rendered figure element is unavailable",
                    )
                    if part
                )[:240]
                continue
            locator.scroll_into_view_if_needed(timeout=5000)
            raw = locator.screenshot(
                type="png",
                animations="disabled",
                timeout=15000,
            )
            if not raw or len(raw) > MAX_FIGURE_IMAGE_BYTES:
                figure["image_error"] = "rendered figure exceeds the image size limit"
                continue
            figure["image_base64"] = base64.b64encode(raw).decode("ascii")
            figure["mime_type"] = "image/png"
            figure["image_bytes"] = len(raw)
            figure["image_extraction_method"] = "rendered_element_screenshot"
            figure["image_access_state"] = "available_rendered"
        except Exception as exc:
            screenshot_error = re.sub(r"\s+", " ", str(exc)).strip()[:180]
            figure["image_error"] = "; ".join(
                part for part in (fetch_error, screenshot_error) if part
            )[:240]


def extract_supplementary_artifact_links(
    markup: str,
    final_url: str,
) -> list[dict[str, str]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(markup, "html.parser")
    attributes = (
        "href",
        "src",
        "data-url",
        "data-href",
        "data-download-url",
        "data-file-url",
    )
    artifact_markers = (
        "/article-supplement/",
        "/suppl/",
        "/esm/",
        "downloadsupplement",
        "mmc",
        "moesm",
        "suppl_file",
        "supporting-information",
        "_si_",
    )
    candidates: list[tuple[str, str, str]] = []
    for node in soup.find_all(True):
        label = node.get_text(" ", strip=True)[:2000]
        for attribute in attributes:
            raw = str(node.get(attribute) or "").strip()
            if raw:
                candidates.append((raw, label, f"html:{attribute}"))
    normalized = html.unescape(markup).replace("\\/", "/")
    for match in re.finditer(
        r"(?P<url>(?:https?:)?//[^\s\"'<>]+|/[^\s\"'<>]+)",
        normalized,
        re.IGNORECASE,
    ):
        raw = match.group("url").rstrip("),.;")
        folded_raw = raw.casefold()
        if ";base64," in folded_raw or folded_raw.startswith("/octet-stream"):
            continue
        if any(marker in raw.casefold() for marker in artifact_markers):
            candidates.append((raw, "Supplementary artifact", "embedded_url"))

    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in sciencedirect_supplementary_artifact_links(candidates, final_url):
        href = item["href"]
        if href in seen:
            continue
        seen.add(href)
        links.append(item)
        if len(links) >= 30:
            return links
    for raw, label, source in candidates:
        searchable = f"{raw} {label}".casefold()
        if not any(marker in searchable for marker in artifact_markers):
            continue
        target = urljoin(final_url, raw)
        try:
            target = validate_url(target)
        except LiteratureBrowserError:
            continue
        path = urlparse(target).path.casefold()
        if path.startswith("/octet-stream"):
            continue
        if path.endswith(".cif") or "/cif/" in path:
            continue
        if target in seen:
            continue
        seen.add(target)
        links.append(
            {
                "href": target,
                "text": label or "Supplementary artifact",
                "source": source,
            }
        )
        if len(links) >= 30:
            break
    return links


def sciencedirect_article_pii(target: str) -> str:
    try:
        url = validate_url(target)
    except LiteratureBrowserError:
        return ""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host_in_domains(host, ("sciencedirect.com",)):
        return ""
    match = re.search(
        r"/science/article/pii/([A-Za-z0-9]+)",
        parsed.path,
        flags=re.IGNORECASE,
    )
    return match.group(1) if match else ""


def same_article_landing_url(candidate: str, final_url: str) -> bool:
    try:
        target = validate_url(candidate)
        final = validate_url(final_url)
    except LiteratureBrowserError:
        return False
    parsed_target = urlparse(target)
    parsed_final = urlparse(final)
    return (
        (parsed_target.hostname or "").lower()
        == (parsed_final.hostname or "").lower()
        and parsed_target.path.rstrip("/") == parsed_final.path.rstrip("/")
    )


def sciencedirect_supplementary_artifact_links(
    candidates: list[tuple[str, str, str]],
    final_url: str,
) -> list[dict[str, str]]:
    pii = sciencedirect_article_pii(final_url)
    if not pii:
        return []
    landing_signal = ""
    for raw, label, _source in candidates:
        searchable = f"{raw} {label}".casefold()
        if not re.search(r"supplement|supporting|appendix", searchable):
            continue
        target = urljoin(final_url, raw)
        if same_article_landing_url(target, final_url) or raw.casefold().startswith(
            "javascript:"
        ):
            landing_signal = label or "Supplementary data"
            break
    if not landing_signal:
        return []
    links: list[dict[str, str]] = []
    for index in range(1, 6):
        for extension in SCIENCEDIRECT_SUPPLEMENT_EXTENSIONS:
            links.append(
                {
                    "href": (
                        "https://ars.els-cdn.com/content/image/"
                        f"1-s2.0-{pii}-mmc{index}{extension}"
                    ),
                    "text": landing_signal,
                    "source": "sciencedirect_supplementary_resolver",
                }
            )
    return links


def deduplicate_resource_links(
    links: list[dict[str, str]],
    *,
    limit: int = 60,
) -> list[dict[str, str]]:
    deduplicated: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in links:
        href = str(item.get("href") or "").strip()
        if not href:
            continue
        parsed = urlparse(href)
        key = urlunsplit(
            (parsed.scheme.casefold(), parsed.netloc.casefold(), parsed.path, parsed.query, "")
        )
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(item)
        if len(deduplicated) >= limit:
            break
    return deduplicated


def extract_sciencedirect_pdf_links(
    markup: str,
    final_url: str,
) -> list[dict[str, str]]:
    host = urlparse(final_url).hostname or ""
    if not host_in_domains(host, ("sciencedirect.com",)):
        return []

    normalized = html.unescape(markup).replace("\\/", "/")
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    for marker in re.finditer(r'"pdfDownload"\s*:', normalized):
        block = normalized[marker.start():marker.start() + 4000]

        def value(name: str) -> str:
            match = re.search(
                rf'"{re.escape(name)}"\s*:\s*"([^"]+)"',
                block,
            )
            return match.group(1) if match else ""

        md5 = value("md5")
        pid = value("pid")
        pii = value("pii")
        extension = value("pdfExtension")
        path = value("path")
        if not all((md5, pid, pii, extension, path)):
            continue
        target = (
            f"https://www.sciencedirect.com/{path.strip('/')}/"
            f"{pii}{extension}?{urlencode({'md5': md5, 'pid': pid})}"
        )
        try:
            target = validate_url(target)
        except LiteratureBrowserError:
            continue
        if target in seen:
            continue
        seen.add(target)
        candidates.append(
            {
                "href": target,
                "text": "View PDF",
                "source": "sciencedirect_pdfDownload",
            }
        )
        if len(candidates) >= MAX_PDF_CANDIDATES:
            break
    return candidates


def append_pdf_text_chunk(
    chunks: list[str],
    value: str,
    max_chars: int,
    total: int,
) -> tuple[int, bool, bool]:
    value = (value or "").strip()
    if not value:
        return total, False, False
    if max_chars > 0:
        remaining = max_chars - total
        if remaining <= 0:
            return total, True, True
        if len(value) > remaining:
            chunks.append(value[:remaining])
            return total + remaining, True, True
    chunks.append(value)
    return total + len(value) + 2, False, False


def pdf_text_result(
    chunks: list[str],
    max_chars: int,
    page_count: int,
    truncated: bool,
) -> tuple[str, bool, int]:
    text, normalization_truncated = trim_text("\n\n".join(chunks), max_chars)
    return text, truncated or normalization_truncated, page_count


def extract_pdf_text_with_pypdf(
    raw: bytes,
    max_chars: int,
) -> tuple[str, bool, int]:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(raw), strict=False)
    chunks: list[str] = []
    total = 0
    truncated = False
    for page in reader.pages:
        total, page_truncated, stop = append_pdf_text_chunk(
            chunks,
            page.extract_text() or "",
            max_chars,
            total,
        )
        truncated = truncated or page_truncated
        if stop:
            break
    return pdf_text_result(chunks, max_chars, len(reader.pages), truncated)


def extract_pdf_text_with_pdfium(
    raw: bytes,
    max_chars: int,
) -> tuple[str, bool, int]:
    import pypdfium2 as pdfium

    document = pdfium.PdfDocument(raw)
    try:
        page_count = len(document)
        chunks: list[str] = []
        total = 0
        truncated = False
        for index in range(page_count):
            page = document[index]
            try:
                text_page = page.get_textpage()
                try:
                    value = text_page.get_text_range() or ""
                finally:
                    close = getattr(text_page, "close", None)
                    if callable(close):
                        close()
            finally:
                page.close()
            total, page_truncated, stop = append_pdf_text_chunk(
                chunks,
                value,
                max_chars,
                total,
            )
            truncated = truncated or page_truncated
            if stop:
                break
        return pdf_text_result(chunks, max_chars, page_count, truncated)
    finally:
        document.close()


def extract_pdf_text(
    raw: bytes,
    max_chars: int,
) -> tuple[str, bool, int]:
    if len(raw) > MAX_PDF_BYTES:
        raise LiteratureBrowserError("publisher PDF exceeds the configured size limit")
    primary_error: Exception | None = None
    primary_result: tuple[str, bool, int] | None = None
    try:
        primary_result = extract_pdf_text_with_pypdf(raw, max_chars)
        if primary_result[0]:
            return primary_result
    except Exception as exc:
        primary_error = exc
    try:
        fallback = extract_pdf_text_with_pdfium(raw, max_chars)
        if fallback[0] or primary_error is not None:
            return fallback
    except Exception:
        if primary_error is not None:
            raise primary_error
        if primary_result is not None:
            return primary_result
        raise
    if primary_result is not None:
        return primary_result
    raise LiteratureBrowserError("publisher PDF text extraction failed")


def render_pdf_page(raw: bytes, page_index: int) -> dict[str, Any]:
    if page_index < 0:
        raise LiteratureBrowserError("PDF page index must be non-negative")
    import pypdfium2 as pdfium

    document = pdfium.PdfDocument(raw)
    try:
        page_count = len(document)
        if page_index >= page_count:
            raise LiteratureBrowserError(
                f"PDF page index {page_index} is unavailable; PDF has {page_count} pages"
            )
        page = document[page_index]
        try:
            bitmap = page.render(scale=1.5)
            try:
                image = bitmap.to_pil()
                output = io.BytesIO()
                image.save(output, format="PNG", optimize=True)
                rendered = output.getvalue()
            finally:
                bitmap.close()
        finally:
            page.close()
    finally:
        document.close()
    if not rendered or len(rendered) > MAX_FIGURE_IMAGE_BYTES:
        raise LiteratureBrowserError("rendered PDF page exceeds the image size limit")
    return {
        "index": page_index,
        "kind": "pdf_page",
        "alt": f"PDF page {page_index + 1}",
        "caption": f"PDF page {page_index + 1}",
        "src": "",
        "extraction_method": "pdf_page_render",
        "image_extraction_method": "pdfium_page_render",
        "image_base64": base64.b64encode(rendered).decode("ascii"),
        "mime_type": "image/png",
        "image_bytes": len(rendered),
    }


def pdf_page_payload(
    raw_pdf: bytes,
    page_count: int,
    render_page_index: int | None,
) -> dict[str, Any]:
    if render_page_index is None:
        return {}
    return {
        "figures": [render_pdf_page(raw_pdf, render_page_index)],
        "figure_extraction": {
            "total": page_count,
            "offset": render_page_index,
            "returned": 1,
            "has_more": render_page_index + 1 < page_count,
        },
    }


def pdf_is_usable_article(
    pdf_text: str,
    html_text: str,
    *,
    pages: int,
    source_url: str,
    browser_originated: bool,
) -> bool:
    lowered = source_url.lower()
    is_supplement = bool(
        re.search(
            r"supplement|supporting|misc_information|additional[_-]?file",
            lowered,
        )
    ) or is_supplementary_pdf_url(source_url)
    if is_supplement:
        return False
    if len(pdf_text) > len(html_text) or full_text_visible(pdf_text):
        return True
    return (
        browser_originated
        and not is_supplement
        and bool(re.search(r"/doi/(?:e?pdf)/", urlparse(source_url).path.lower()))
        and pages >= 3
        and len(pdf_text) >= 8000
    )


def fetch_pdf_in_browser(
    page: Any,
    target: str,
    *,
    timeout_ms: int = PDF_FETCH_TIMEOUT_MS,
    user_agent: str = "",
) -> tuple[bytes, str, str, int]:
    url = validate_url(target)
    host = urlparse(url).hostname or ""
    if not browser_pdf_fetch_host_allowed(host):
        raise LiteratureBrowserError(
            "browser-originated PDF fetch is not enabled for this publisher"
        )
    result = page.evaluate(
        """async ({url, maxBytes, timeoutMs}) => {
          const controller = new AbortController();
          const timer = setTimeout(() => controller.abort(), timeoutMs);
          try {
            const response = await fetch(url, {
              credentials: 'include',
              redirect: 'follow',
              signal: controller.signal,
              headers: {
                Accept: 'application/pdf,application/octet-stream;q=0.9,*/*;q=0.1'
              }
            });
            const declaredLength = Number(
              response.headers.get('content-length') || '0'
            );
            if (declaredLength > maxBytes) {
              return {error: 'publisher PDF exceeds the configured size limit'};
            }
            const buffer = await response.arrayBuffer();
            if (buffer.byteLength > maxBytes) {
              return {error: 'publisher PDF exceeds the configured size limit'};
            }
            const bytes = new Uint8Array(buffer);
            const chunks = [];
            for (let offset = 0; offset < bytes.length; offset += 32768) {
              chunks.push(String.fromCharCode(
                ...bytes.subarray(offset, offset + 32768)
              ));
            }
            return {
              status: response.status,
              finalUrl: response.url,
              contentType: response.headers.get('content-type') || '',
              bodyBase64: btoa(chunks.join(''))
            };
          } catch (error) {
            return {error: String(error)};
          } finally {
            clearTimeout(timer);
          }
        }""",
        {
            "url": url,
            "maxBytes": MAX_PDF_BYTES,
            "timeoutMs": timeout_ms,
        },
    )
    if not isinstance(result, dict):
        raise LiteratureBrowserError(
            "browser-originated PDF fetch returned an invalid response"
        )
    if result.get("error"):
        browser_error = str(result["error"])
        if "exceeds the configured size limit" in browser_error:
            raise LiteratureBrowserError(browser_error)
        return fetch_pdf_with_context_request(
            page,
            url,
            timeout_ms=timeout_ms,
            browser_error=browser_error,
            user_agent=user_agent,
        )
    status = result.get("status")
    if not isinstance(status, int) or status >= 400:
        raise LiteratureBrowserError(
            f"publisher PDF returned HTTP {status}"
        )
    final_url = validate_browser_pdf_result_url(
        url,
        str(result.get("finalUrl") or url),
    )
    try:
        raw = base64.b64decode(
            str(result.get("bodyBase64") or ""),
            validate=True,
        )
    except (ValueError, TypeError) as exc:
        raise LiteratureBrowserError(
            "browser-originated PDF fetch returned invalid content"
        ) from exc
    if len(raw) > MAX_PDF_BYTES:
        raise LiteratureBrowserError(
            "publisher PDF exceeds the configured size limit"
        )
    return (
        raw,
        str(result.get("contentType") or "").lower(),
        final_url,
        status,
    )


def fetch_pdf_with_context_request(
    page: Any,
    target: str,
    *,
    timeout_ms: int,
    browser_error: str,
    user_agent: str = "",
) -> tuple[bytes, str, str, int]:
    response = None
    try:
        headers = {
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.1"
        }
        if user_agent:
            headers["User-Agent"] = user_agent
        try:
            referrer = validate_url(str(page.url or ""))
            if (urlparse(referrer).hostname or "") == (
                urlparse(target).hostname or ""
            ):
                headers["Referer"] = referrer
        except LiteratureBrowserError:
            pass
        response = page.request.get(
            target,
            headers=headers,
            timeout=timeout_ms,
            fail_on_status_code=False,
        )
        status = int(response.status)
        if status >= 400:
            raise LiteratureBrowserError(
                f"publisher PDF returned HTTP {status}"
            )
        final_url = validate_browser_pdf_result_url(target, str(response.url))
        headers = response.headers
        declared_length = int(str(headers.get("content-length") or "0"))
        if declared_length > MAX_PDF_BYTES:
            raise LiteratureBrowserError(
                "publisher PDF exceeds the configured size limit"
            )
        raw = response.body()
        if len(raw) > MAX_PDF_BYTES:
            raise LiteratureBrowserError(
                "publisher PDF exceeds the configured size limit"
            )
        return (
            raw,
            str(headers.get("content-type") or "").lower(),
            final_url,
            status,
        )
    except LiteratureBrowserError:
        raise
    except Exception as exc:
        detail = re.sub(r"\s+", " ", browser_error).strip()[:160]
        raise LiteratureBrowserError(
            "browser PDF fetch failed and context request fallback failed: "
            f"{detail}; {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        if response is not None:
            try:
                response.dispose()
            except Exception:
                pass


def fetch_public_figshare_pdf(
    target: str,
    *,
    timeout_ms: int = PDF_FETCH_TIMEOUT_MS,
) -> tuple[bytes, str, str, int]:
    url = validate_url(target)
    host = urlparse(url).hostname or ""
    if (
        not host_in_domains(host, ("figshare.com",))
        or not is_supplementary_pdf_url(url)
    ):
        raise LiteratureBrowserError(
            "public repository fetch is restricted to governed Figshare files"
        )
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.1",
            "User-Agent": "Mozilla/5.0",
        },
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=max(5.0, min(120.0, timeout_ms / 1000)),
        ) as response:
            status = int(response.status)
            final_url = validate_browser_pdf_result_url(url, response.geturl())
            content_type = str(
                response.headers.get("content-type") or ""
            ).lower()
            declared_length = str(
                response.headers.get("content-length") or ""
            ).strip()
            if (
                declared_length.isdigit()
                and int(declared_length) > MAX_PDF_BYTES
            ):
                raise LiteratureBrowserError(
                    "publisher PDF exceeds the configured size limit"
                )
            raw = response.read(MAX_PDF_BYTES + 1)
    except urllib.error.HTTPError as exc:
        raise LiteratureBrowserError(
            f"publisher PDF returned HTTP {exc.code}"
        ) from exc
    except LiteratureBrowserError:
        raise
    except Exception as exc:
        raise LiteratureBrowserError(
            f"public Figshare PDF fetch failed: {type(exc).__name__}: {exc}"
        ) from exc
    if len(raw) > MAX_PDF_BYTES:
        raise LiteratureBrowserError(
            "publisher PDF exceeds the configured size limit"
        )
    return raw, content_type, final_url, status


def fetch_pdf_via_browser_download(
    page: Any,
    target: str,
    *,
    timeout_ms: int,
) -> tuple[bytes, str, str, int]:
    url = validate_url(target)
    host = urlparse(url).hostname or ""
    if not browser_pdf_fetch_host_allowed(host) or not is_supplementary_pdf_url(url):
        raise LiteratureBrowserError(
            "browser downloads are restricted to governed supplementary files"
        )

    download = None
    try:
        with page.expect_download(timeout=timeout_ms) as download_info:
            try:
                page.goto(
                    url,
                    wait_until="commit",
                    timeout=timeout_ms,
                )
            except Exception as exc:
                if "download is starting" not in str(exc).lower():
                    raise
        download = download_info.value
        failure = download.failure()
        if failure:
            raise LiteratureBrowserError(
                f"publisher browser download failed: {str(failure)[:240]}"
            )
        path = Path(download.path())
        size = path.stat().st_size
        if size > MAX_PDF_BYTES:
            raise LiteratureBrowserError(
                "publisher PDF exceeds the configured size limit"
            )
        raw = path.read_bytes()
        if len(raw) > MAX_PDF_BYTES:
            raise LiteratureBrowserError(
                "publisher PDF exceeds the configured size limit"
            )
        return raw, "application/pdf", url, 200
    except LiteratureBrowserError:
        raise
    except Exception as exc:
        raise LiteratureBrowserError(
            f"publisher browser download failed: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        if download is not None:
            try:
                download.delete()
            except Exception:
                pass


def load_acs_solver_article_context(
    page: Any,
    markup: str,
    final_url: str,
    *,
    timeout_ms: int,
) -> None:
    url = validate_url(final_url)
    host = urlparse(url).hostname or ""
    if not host_in_domains(host, ("pubs.acs.org",)):
        raise LiteratureBrowserError("ACS solver context has an invalid origin")
    extraction = extract_structured_html_article(markup, url, 0)
    if not extraction.get("success"):
        raise LiteratureBrowserError(
            "ACS solver context did not contain verified article text"
        )
    artifact_links = extract_supplementary_artifact_links(markup, url)
    if not artifact_links:
        raise LiteratureBrowserError(
            "ACS solver context did not contain a verified supplementary link"
        )
    context_markup = "<!doctype html><html><body>" + "".join(
        (
            f'<a href="{html.escape(item["href"], quote=True)}">'
            f'{html.escape(item.get("text") or "Supporting Information")}</a>'
        )
        for item in artifact_links
    ) + "</body></html>"

    def fulfill_article(route: Any) -> None:
        route.fulfill(
            status=200,
            headers={"Content-Type": "text/html; charset=utf-8"},
            body=context_markup,
        )

    page.route(url, fulfill_article)
    try:
        page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=min(30000, max(5000, timeout_ms)),
        )
    finally:
        page.unroute(url, fulfill_article)


def fetch_acs_supplement_via_article_click(
    page: Any,
    target: str,
    *,
    timeout_ms: int,
) -> tuple[bytes, str, str, int]:
    target_url = validate_url(target)
    article_id, file_stem = acs_supplement_identity(target_url)
    if not article_id or not file_stem:
        raise LiteratureBrowserError("ACS SI click target identity is invalid")
    link_index = page.evaluate(
        """target => Array.from(document.querySelectorAll('a[href]')).findIndex(
          (node) => new URL(node.getAttribute('href'), document.baseURI).href === target
        )""",
        target_url,
    )
    if not isinstance(link_index, int) or link_index < 0:
        raise LiteratureBrowserError(
            "verified ACS article does not expose the exact SI link"
        )

    download = None
    try:
        with page.expect_download(timeout=timeout_ms) as download_info:
            page.locator("a[href]").nth(link_index).click(
                timeout=timeout_ms,
                no_wait_after=True,
            )
        download = download_info.value
        failure = download.failure()
        if failure:
            raise LiteratureBrowserError(
                f"publisher browser download failed: {str(failure)[:240]}"
            )
        suggested_stem = Path(str(download.suggested_filename or "")).stem.casefold()
        if "_si_" in suggested_stem and suggested_stem != file_stem:
            raise LiteratureBrowserError(
                "ACS SI downloaded filename does not match the verified file identity"
            )
        path = Path(download.path())
        if path.stat().st_size > MAX_PDF_BYTES:
            raise LiteratureBrowserError(
                "publisher PDF exceeds the configured size limit"
            )
        raw = path.read_bytes()
        if len(raw) > MAX_PDF_BYTES:
            raise LiteratureBrowserError(
                "publisher PDF exceeds the configured size limit"
            )
        validate_pdf_payload(raw, "application/pdf", 200)
        return raw, "application/pdf", target_url, 200
    except LiteratureBrowserError:
        raise
    except Exception as exc:
        raise LiteratureBrowserError(
            f"ACS SI article-link download failed: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        if download is not None:
            try:
                download.delete()
            except Exception:
                pass


def is_supplementary_pdf_url(target: str) -> bool:
    try:
        url = validate_url(target)
    except LiteratureBrowserError:
        return False
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not browser_pdf_fetch_host_allowed(host):
        return False

    path = unquote(parsed.path).lower()
    query = unquote(parsed.query).lower()
    if rsc_supplement_doi(url):
        return True
    if (
        host_in_domains(host, ("acs.figshare.com",))
        and re.fullmatch(r"/ndownloader/files/\d+/?", path)
    ) or (
        host_in_domains(host, ("ndownloader.figshare.com",))
        and re.fullmatch(r"/files/\d+/?", path)
    ):
        return True
    if host_in_domains(host, ("pubs.acs.org",)) and re.fullmatch(
        r"/[^/]+/article-supplement/\d+/pdf/[^/]+/?",
        path,
    ):
        return True
    if host_in_domains(host, ("wiley.com",)) and (
        path.rstrip("/") == "/action/downloadsupplement"
    ):
        return True
    if path.endswith(".pdf") and any(
        marker in path for marker in SUPPLEMENTARY_PDF_PATH_MARKERS
    ):
        return True
    filename = path.rsplit("/", 1)[-1]
    if path.endswith(".pdf") and SUPPLEMENTARY_PDF_FILENAME_RE.search(filename):
        return True
    return bool(
        path.rstrip("/").endswith("/downloadsupplement")
        and re.search(r"(?:^|&)file=[^&]*\.pdf(?:&|$)", query)
    )


def rsc_supplement_doi(target: str) -> str:
    try:
        url = validate_url(target)
    except LiteratureBrowserError:
        return ""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    path = unquote(parsed.path).lower()
    if not host_in_domains(host, ("rsc.org",)) or not any(
        marker in path for marker in ("/suppdata/", "/article-supplement/")
    ):
        return ""
    filename = path.rstrip("/").rsplit("/", 1)[-1]
    match = RSC_SUPPLEMENT_DOI_RE.fullmatch(filename)
    return str(match.group("doi")) if match else ""


def rsc_supplement_file_stem(target: str) -> str:
    doi = rsc_supplement_doi(target)
    if not doi:
        return ""
    path = unquote(urlparse(validate_url(target)).path).lower()
    stem = path.rstrip("/").rsplit("/", 1)[-1]
    if stem.endswith(".pdf"):
        stem = stem[:-4]
    if stem.endswith("_suppl"):
        stem = stem[: -len("_suppl")]
    if not RSC_SUPPLEMENT_DOI_RE.fullmatch(stem):
        return ""
    return stem


def acs_supplement_identity(target: str) -> tuple[str, str]:
    try:
        parsed = urlparse(validate_url(target))
    except LiteratureBrowserError:
        return "", ""
    if not host_in_domains(parsed.hostname or "", ("pubs.acs.org",)):
        return "", ""
    match = ACS_SUPPLEMENT_PATH_RE.fullmatch(unquote(parsed.path))
    if not match:
        return "", ""
    return (
        str(match.group("article_id")),
        str(match.group("file_stem")).lower(),
    )


def acs_article_doi(target: str) -> str:
    try:
        parsed = urlparse(validate_url(target))
    except LiteratureBrowserError:
        return ""
    if not host_in_domains(parsed.hostname or "", ("pubs.acs.org", "doi.org")):
        return ""
    path = unquote(parsed.path).strip("/")
    if path.lower().startswith("doi/"):
        path = path[4:]
    for prefix in ("abs/", "full/", "pdf/", "pdfdirect/"):
        if path.lower().startswith(prefix):
            path = path[len(prefix):]
            break
    return path if ACS_DOI_RE.fullmatch(path) else ""


def acs_article_numeric_id(target: str) -> str:
    try:
        parsed = urlparse(validate_url(target))
    except LiteratureBrowserError:
        return ""
    if not host_in_domains(parsed.hostname or "", ("pubs.acs.org",)):
        return ""
    match = re.match(
        r"^/[^/]+/article/[^/]+/[^/]+/[^/]+/(?P<article_id>\d+)(?:/|$)",
        unquote(parsed.path),
        flags=re.IGNORECASE,
    )
    return str(match.group("article_id")) if match else ""


def resolve_acs_supplement_from_solved_article(
    markup: str,
    final_url: str,
    target: str,
    *,
    landing_url_hint: str = "",
) -> str:
    article_id, file_stem = acs_supplement_identity(target)
    if not article_id or not file_stem:
        raise LiteratureBrowserError("ACS SI target identity is invalid")

    relationship_verified = False
    for article_url in (landing_url_hint, final_url):
        if not article_url:
            continue
        doi = acs_article_doi(article_url)
        if doi:
            doi_suffix = doi.rsplit(".", 1)[-1].lower()
            if doi_suffix not in file_stem:
                raise LiteratureBrowserError(
                    "ACS SI file does not match the solved article DOI"
                )
            relationship_verified = True
        solved_article_id = acs_article_numeric_id(article_url)
        if solved_article_id:
            if solved_article_id != article_id:
                raise LiteratureBrowserError(
                    "ACS SI article id does not match the solved article"
                )
            relationship_verified = True
    if not relationship_verified:
        raise LiteratureBrowserError(
            "ACS SI requires a DOI- or article-bound solved article context"
        )

    target_url = validate_url(target)
    for item in extract_supplementary_artifact_links(markup, final_url):
        candidate = str(item.get("href") or "")
        candidate_id, candidate_stem = acs_supplement_identity(candidate)
        if candidate_id == article_id and candidate_stem == file_stem:
            return validate_url(candidate)
    return target_url


def rsc_article_supplement_candidates_from_page_url(
    page_url: str,
    target: str,
) -> list[str]:
    stem = rsc_supplement_file_stem(target)
    if not stem:
        return []
    try:
        parsed = urlparse(validate_url(page_url))
    except LiteratureBrowserError:
        return []
    host = parsed.hostname or ""
    if not host_in_domains(host, ("pubs.rsc.org",)):
        return []
    parts = [part for part in unquote(parsed.path).split("/") if part]
    try:
        article_index = parts.index("article")
    except ValueError:
        return []
    if article_index < 1 or article_index + 4 >= len(parts):
        return []
    journal = parts[article_index - 1]
    article_id = parts[article_index + 4]
    if not journal or not article_id.isdigit():
        return []
    return [
        f"https://pubs.rsc.org/{journal}/article-supplement/"
        f"{article_id}/pdf/{stem}_suppl/"
    ]


def rsc_supplement_candidates_from_page(
    page: Any,
    target: str,
) -> list[str]:
    doi = rsc_supplement_doi(target)
    if not doi:
        return []
    base_url = str(getattr(page, "url", "") or target)
    ranked: list[tuple[int, str]] = []
    seen: set[str] = set()
    for candidate in rsc_article_supplement_candidates_from_page_url(
        base_url,
        target,
    ):
        ranked.append((60, candidate))
        seen.add(candidate)
    try:
        raw_values = page.evaluate(
            """() => Array.from(
                document.querySelectorAll('a[href], iframe[src], embed[src], object[data]')
            ).map((el) => el.getAttribute('href')
                || el.getAttribute('src')
                || el.getAttribute('data')
                || '')"""
        )
    except Exception:
        raw_values = []
    if not isinstance(raw_values, list):
        return [candidate for _score, candidate in ranked]
    for raw in raw_values:
        if not isinstance(raw, str) or not raw.strip():
            continue
        candidate = urljoin(base_url, raw.strip())
        try:
            candidate = validate_url(candidate)
        except LiteratureBrowserError:
            continue
        if candidate in seen:
            continue
        parsed = urlparse(candidate)
        host = parsed.hostname or ""
        if not host_in_domains(host, ("rsc.org",)):
            continue
        candidate_doi = rsc_supplement_doi(candidate)
        if candidate_doi and candidate_doi.lower() != doi.lower():
            continue
        if not candidate_doi and doi.lower() not in unquote(parsed.path).lower():
            continue
        if not is_supplementary_pdf_url(candidate):
            continue
        path = unquote(parsed.path).lower()
        score = 0
        if host_in_domains(host, ("pubs.rsc.org",)):
            score += 20
        if "/article-supplement/" in path:
            score += 20
        if path.rstrip("/").endswith(".pdf"):
            score += 5
        ranked.append((score, candidate))
        seen.add(candidate)
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [candidate for _score, candidate in ranked[:4]]


def wiley_supplement_doi(target: str) -> str:
    try:
        url = validate_url(target)
    except LiteratureBrowserError:
        return ""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if (
        not host_in_domains(host, ("wiley.com",))
        or unquote(parsed.path).lower().rstrip("/")
        != "/action/downloadsupplement"
    ):
        return ""
    values = parse_qs(parsed.query, keep_blank_values=False).get("doi", [])
    if len(values) != 1:
        return ""
    doi = str(values[0]).strip()
    return doi if WILEY_SUPPLEMENT_DOI_RE.fullmatch(doi) else ""


def pnas_supplement_identity(target: str) -> tuple[str, str]:
    try:
        parsed = urlparse(validate_url(target))
    except LiteratureBrowserError:
        return "", ""
    if not host_in_domains(parsed.hostname or "", ("pnas.org",)):
        return "", ""
    match = PNAS_SUPPLEMENT_PATH_RE.fullmatch(unquote(parsed.path))
    if not match:
        return "", ""
    return (
        str(match.group("doi")).lower(),
        str(match.group("filename")).lower(),
    )


def pnas_article_doi(target: str) -> str:
    try:
        parsed = urlparse(validate_url(target))
    except LiteratureBrowserError:
        return ""
    host = parsed.hostname or ""
    if not host_in_domains(host, ("pnas.org", "doi.org")):
        return ""
    path = unquote(parsed.path).strip("/")
    if host_in_domains(host, ("pnas.org",)) and path.lower().startswith("doi/"):
        path = path[4:]
    return path.lower() if PNAS_DOI_RE.fullmatch(path) else ""


def supplementary_pdf_landing_url(target: str) -> str:
    url = validate_url(target)
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not is_supplementary_pdf_url(url):
        raise LiteratureBrowserError(
            "supplementary PDF URL does not match a governed publisher path"
        )
    if host_in_domains(host, ("pubs.acs.org",)):
        return "https://pubs.acs.org/"
    rsc_doi = rsc_supplement_doi(url)
    if rsc_doi:
        return f"https://doi.org/10.1039/{rsc_doi}"
    wiley_doi = wiley_supplement_doi(url)
    if wiley_doi:
        encoded_doi = quote(wiley_doi, safe="/:._;()+-")
        return f"https://onlinelibrary.wiley.com/doi/{encoded_doi}"
    for publisher_domain, doi_prefix in (
        ("pnas.org", "10.1073"),
        ("science.org", "10.1126"),
    ):
        if not host_in_domains(host, (publisher_domain,)):
            continue
        match = re.match(
            rf"^/doi/suppl/(?P<doi>{re.escape(doi_prefix)}/[^/]+)/",
            unquote(parsed.path),
            flags=re.IGNORECASE,
        )
        if match:
            encoded_doi = quote(match.group("doi"), safe="/:._;()+-")
            return f"{parsed.scheme}://{host}/doi/{encoded_doi}"
    return f"{parsed.scheme}://{host}/robots.txt"


def science_supplement_landing_url(target: str) -> str:
    url = validate_url(target)
    host = urlparse(url).hostname or ""
    if not host_in_domains(host, ("science.org",)):
        raise LiteratureBrowserError(
            "same-origin supplementary fetch is restricted to Science"
        )
    return supplementary_pdf_landing_url(url)


def is_science_supplement_url(target: str) -> bool:
    try:
        science_supplement_landing_url(target)
    except LiteratureBrowserError:
        return False
    return True


def fetch_supplementary_pdf_from_origin(
    page: Any,
    target: str,
    *,
    timeout_ms: int,
    landing_url_hint: str = "",
) -> tuple[bytes, str, str, int]:
    landing_url = supplementary_pdf_landing_url(target)
    target_host = urlparse(validate_url(target)).hostname or ""
    if landing_url_hint and (
        host_in_domains(target_host, ("pubs.acs.org",))
        or host_in_domains(target_host, ("rsc.org",))
    ):
        try:
            candidate = validate_url(landing_url_hint)
            candidate_host = urlparse(candidate).hostname or ""
            if (
                landing_hint_allowed_for_target(target_host, candidate_host)
                and not is_supplementary_pdf_url(candidate)
            ):
                landing_url = candidate
        except LiteratureBrowserError:
            pass
    if host_in_domains(target_host, ("figshare.com",)):
        return fetch_public_figshare_pdf(
            target,
            timeout_ms=timeout_ms,
        )
    deadline = time.monotonic() + (timeout_ms / 1000)
    helper = page.context.new_page()
    try:
        initial_download_error = ""
        acs_target = host_in_domains(target_host, ("pubs.acs.org",))
        acs_bound_landing = bool(
            acs_target
            and landing_url_hint
            and (
                acs_article_doi(landing_url_hint)
                or acs_article_numeric_id(landing_url_hint)
            )
        )
        if acs_target and not acs_bound_landing:
            try:
                return fetch_pdf_via_browser_download(
                    helper,
                    target,
                    timeout_ms=min(5000, max(3000, timeout_ms // 6)),
                )
            except Exception as exc:
                # The persistent browser may still need its publisher origin
                # initialized or challenge cookies refreshed below.
                initial_download_error = f"{type(exc).__name__}: {exc}"[:300]
        request_user_agent = ""
        landing_host = urlparse(landing_url).hostname or ""
        acs_landing = host_in_domains(landing_host, ("pubs.acs.org",))
        landing_navigation_error: Exception | None = None
        if acs_bound_landing:
            landing_navigation_error = LiteratureBrowserError(
                "ACS SI uses its DOI-bound solver article context"
            )
            title = ""
            body = ""
        else:
            try:
                helper.goto(
                    landing_url,
                    wait_until="commit" if acs_landing else "domcontentloaded",
                    timeout=min(15000, max(5000, timeout_ms // 3)),
                )
            except Exception as exc:
                landing_navigation_error = exc
            try:
                title = helper.title().strip()
                body = helper.locator("body").inner_text(timeout=10000)
            except Exception:
                title = ""
                body = ""
        landing_state = classify_page(helper.url, title, body)
        download_error = ""
        rsc_doi = rsc_supplement_doi(target)
        rsc_presolve_candidates = (
            rsc_supplement_candidates_from_page(helper, target)
            if rsc_doi
            else []
        )
        if rsc_presolve_candidates and landing_state != "content":
            for candidate_target in dict.fromkeys(rsc_presolve_candidates):
                remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
                if remaining_ms < 5000:
                    break
                try:
                    return fetch_pdf_with_context_request(
                        helper,
                        candidate_target,
                        timeout_ms=min(15000, remaining_ms),
                        browser_error="RSC canonical SI context request",
                    )
                except Exception as exc:
                    download_error = (
                        f"{candidate_target}: {type(exc).__name__}: {exc}"
                    )[:240]
            if download_error and not FLARESOLVERR_URL:
                raise LiteratureBrowserError(
                    "publisher origin challenge requires RSC SI profile "
                    f"refresh before retry: {download_error}"
                )
        solver_url = landing_url if landing_navigation_error else helper.url
        recover_navigation = bool(
            landing_navigation_error
            and acs_landing
            and FLARESOLVERR_URL
            and flaresolverr_host_allowed(landing_host)
        )
        force_acs_solver = bool(
            initial_download_error
            and acs_landing
            and landing_state != "content"
            and FLARESOLVERR_URL
            and flaresolverr_host_allowed(landing_host)
        )
        if force_acs_solver:
            solver_url = landing_url
        if should_try_flaresolverr(
            solver_url,
            landing_state,
            body,
        ) or recover_navigation or force_acs_solver:
            try:
                remaining_seconds = max(0.0, deadline - time.monotonic())
                if remaining_seconds < 30:
                    raise LiteratureBrowserError(
                        "SI profile handoff has insufficient time for "
                        "the challenge solver"
                    )
                solver_budget_ms = max(
                    10000,
                    int((remaining_seconds - 10.0) * 1000),
                )
                solved = request_publisher_flaresolverr(
                    solver_url,
                    allow_retry=True,
                    include_figure_images=False,
                    max_timeout_ms=solver_budget_ms,
                )
                publisher = flaresolverr_publisher_slug(solver_url)
                request_user_agent = apply_flaresolverr_handoff(
                    helper,
                    solved,
                    publisher=publisher,
                )
                handoff_url = validate_url(
                    str(solved.get("final_url") or solver_url)
                )
                helper.goto(
                    handoff_url,
                    wait_until="domcontentloaded",
                    timeout=min(
                        60000,
                        max(10000, int((deadline - time.monotonic()) * 1000)),
                    ),
                )
                try:
                    helper.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
                handoff_sample = settle_publisher_shell(
                    helper,
                    page_access_sample(helper, body_timeout_ms=10000),
                )
                if str(handoff_sample.get("state") or "") != "content":
                    raise LiteratureBrowserError(
                        "Playwright did not verify publisher access after the "
                        "SI solver profile handoff"
                    )
                landing_navigation_error = None
                landing_state = "content"
                body = str(handoff_sample.get("text") or "")
            except Exception as exc:
                raise LiteratureBrowserError(
                    "publisher SI profile handoff failed: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
        elif landing_navigation_error:
            raise LiteratureBrowserError(
                "publisher origin initialization failed before SI fetch: "
                f"{type(landing_navigation_error).__name__}: "
                f"{landing_navigation_error}"
            ) from landing_navigation_error
        remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
        if remaining_ms < 5000:
            raise LiteratureBrowserError(
                "same-origin SI fetch reached the request time budget"
            )
        helper.wait_for_timeout(min(1000, max(0, remaining_ms - 5000)))
        remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
        if remaining_ms < 5000:
            raise LiteratureBrowserError(
                "same-origin SI fetch reached the request time budget"
            )
        candidate_targets = (
            rsc_supplement_candidates_from_page(helper, target)
            if rsc_doi
            else []
        )
        if rsc_doi:
            candidate_targets.append(target)
        for candidate_target in dict.fromkeys(candidate_targets):
            candidate_host = urlparse(validate_url(candidate_target)).hostname or ""
            if rsc_doi:
                try:
                    return fetch_pdf_with_context_request(
                        helper,
                        candidate_target,
                        timeout_ms=min(30000, remaining_ms),
                        browser_error="RSC canonical SI context request",
                        user_agent=request_user_agent,
                    )
                except Exception as exc:
                    download_error = (
                        f"{candidate_target}: {type(exc).__name__}: {exc}"
                    )[:240]
                if host_in_domains(candidate_host, ("www.rsc.org",)):
                    continue
                remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
                if remaining_ms < 5000:
                    break
            try:
                return fetch_pdf_via_browser_download(
                    helper,
                    candidate_target,
                    timeout_ms=remaining_ms,
                )
            except Exception as exc:
                download_error = (
                    f"{candidate_target}: {type(exc).__name__}: {exc}"
                )[:240]
            try:
                return fetch_pdf_in_browser(
                    helper,
                    candidate_target,
                    timeout_ms=remaining_ms,
                    user_agent=request_user_agent,
                )
            except Exception as exc:
                download_error = (
                    f"{candidate_target}: {type(exc).__name__}: {exc}"
                )[:240]
        if rsc_doi and download_error:
            raise LiteratureBrowserError(
                "publisher origin challenge requires RSC SI profile refresh "
                f"before retry: {download_error}"
            )
        if host_in_domains(target_host, ("pubs.acs.org", "wiley.com")):
            try:
                return fetch_pdf_with_context_request(
                    helper,
                    target,
                    timeout_ms=min(30000, remaining_ms),
                    browser_error="publisher authenticated SI context request",
                    user_agent=request_user_agent,
                )
            except Exception as exc:
                download_error = f"{type(exc).__name__}: {exc}"[:240]
            remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
            if remaining_ms < 5000:
                raise LiteratureBrowserError(
                    "publisher same-origin SI fetch reached the request time budget"
                )
        try:
            return fetch_pdf_in_browser(
                helper,
                target,
                timeout_ms=remaining_ms,
                user_agent=request_user_agent,
            )
        except Exception as exc:
            prefix = (
                f"browser download failed ({download_error}); "
                if download_error
                else ""
            )
            raise LiteratureBrowserError(
                "publisher same-origin SI fetch failed: " + prefix
                + f"{type(exc).__name__}: {exc}"
            ) from exc
    finally:
        helper.close()


def fetch_science_supplement_from_origin(
    page: Any,
    target: str,
    *,
    timeout_ms: int,
) -> tuple[bytes, str, str, int]:
    science_supplement_landing_url(target)
    return fetch_supplementary_pdf_from_origin(
        page,
        target,
        timeout_ms=timeout_ms,
    )


def extract_supplementary_pdf_from_origin(
    page: Any,
    target_url: str,
    max_chars: int,
    *,
    timeout_ms: int = PDF_FETCH_TIMEOUT_MS,
    landing_url_hint: str = "",
    render_page_index: int | None = None,
) -> dict[str, Any]:
    fetch_options = {"timeout_ms": timeout_ms}
    if landing_url_hint:
        fetch_options["landing_url_hint"] = landing_url_hint
    raw_pdf, content_type, source_url, status = (
        fetch_supplementary_pdf_from_origin(
            page,
            target_url,
            **fetch_options,
        )
    )
    validate_pdf_payload(raw_pdf, content_type, status)
    pdf_text, pdf_truncated, pdf_pages = extract_pdf_text(raw_pdf, max_chars)
    if not pdf_text:
        raise LiteratureBrowserError(
            "publisher PDF contained no extractable text"
        )
    return {
        "text": pdf_text,
        "text_truncated": pdf_truncated,
        "pages": pdf_pages,
        "source_url": source_url,
        "strategy": "publisher_same_origin_supplement_fetch",
        "bytes": len(raw_pdf),
        "content_type": content_type,
        "status": status,
        **pdf_page_payload(raw_pdf, pdf_pages, render_page_index),
    }


def extract_governed_supplement_fallback(
    target_url: str,
    max_chars: int,
    *,
    timeout_ms: int,
    render_page_index: int | None = None,
) -> dict[str, Any] | None:
    entry = governed_supplement_fallback(target_url)
    if entry is None:
        return None
    request = urllib.request.Request(
        entry["fallback_url"],
        headers={
            "Accept": "application/pdf,application/octet-stream;q=0.9",
            "User-Agent": "Scientist literature browser",
        },
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=max(5.0, min(60.0, timeout_ms / 1000)),
        ) as response:
            status = int(response.status)
            content_type = str(response.headers.get("content-type") or "").lower()
            source_url = validate_url(str(response.geturl() or entry["fallback_url"]))
            raw_pdf = response.read(MAX_PDF_BYTES + 1)
    except Exception as exc:
        raise LiteratureBrowserError(
            "governed supplement fallback fetch failed: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if len(raw_pdf) > MAX_PDF_BYTES:
        raise LiteratureBrowserError("governed supplement fallback exceeded PDF limit")
    if hashlib.sha256(raw_pdf).hexdigest() != entry["sha256"]:
        raise LiteratureBrowserError("governed supplement fallback SHA-256 mismatch")
    validate_pdf_payload(raw_pdf, content_type, status)
    pdf_text, pdf_truncated, pdf_pages = extract_pdf_text(raw_pdf, max_chars)
    normalized_text = re.sub(r"\s+", "", pdf_text).lower()
    if entry["expected_doi"] not in normalized_text:
        raise LiteratureBrowserError(
            "governed supplement fallback did not contain the expected DOI"
        )
    return {
        "text": pdf_text,
        "text_truncated": pdf_truncated,
        "pages": pdf_pages,
        "source_url": source_url,
        "strategy": "governed_exact_supplement_fallback",
        "expected_doi": entry["expected_doi"],
        "provenance": entry["provenance"],
        "bytes": len(raw_pdf),
        "content_type": content_type,
        "status": status,
        **pdf_page_payload(raw_pdf, pdf_pages, render_page_index),
    }


def supplementary_pdf_result(
    target_url: str,
    extracted: dict[str, Any],
) -> dict[str, Any]:
    details = dict(extracted)
    text = str(details.pop("text"))
    truncated = bool(details.pop("text_truncated"))
    figures = list(details.pop("figures", []))
    figure_extraction = dict(details.pop("figure_extraction", {}))
    final_url = str(details.get("source_url") or target_url)
    status = int(details.get("status") or 200)
    return {
        "success": True,
        "url": target_url,
        "final_url": final_url,
        "status": status,
        "page_state": "content",
        "title": "",
        "text": text,
        "text_truncated": truncated,
        "text_source": "pdf",
        "html_text_chars": 0,
        "full_text_dom_visible": False,
        "challenge_bypass": {"attempted": False, "success": False},
        "html_extraction": {
            "attempted": False,
            "success": False,
            "candidates": [],
        },
        "pdf_extraction": {
            "attempted": True,
            "success": True,
            "text_chars": len(text),
            **details,
        },
        "metadata": {},
        "figures": figures,
        "figure_extraction": figure_extraction,
        "pdf_links": [],
        "references": extract_numbered_references(text),
        # A validated PDF payload with extractable text is stronger evidence
        # than article-heading heuristics, which do not fit many SI documents.
        "access_state": "full_text_visible",
        "access_signals": [],
        "institution_markers": [],
    }


def article_pdf_result(
    target_url: str,
    extracted: dict[str, Any],
    *,
    challenge_bypass: dict[str, Any] | None = None,
    profile_warm: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    details = dict(extracted)
    text = str(details.pop("text"))
    truncated = bool(details.pop("text_truncated"))
    figures = list(details.pop("figures", []))
    figure_extraction = dict(details.pop("figure_extraction", {}))
    final_url = str(details.get("source_url") or target_url)
    status = int(details.get("status") or 200)
    return {
        "success": True,
        "url": target_url,
        "final_url": final_url,
        "status": status,
        "page_state": "content",
        "title": "",
        "text": text,
        "text_truncated": truncated,
        "text_source": "pdf",
        "html_text_chars": 0,
        "full_text_dom_visible": False,
        "challenge_bypass": challenge_bypass
        or {"attempted": False, "success": False},
        "profile_warm": profile_warm or {"attempted": False},
        "html_extraction": {
            "attempted": False,
            "success": False,
            "candidates": [],
        },
        "pdf_extraction": {
            "attempted": True,
            "success": True,
            "text_chars": len(text),
            **details,
        },
        "metadata": metadata or {},
        "figures": figures,
        "figure_extraction": figure_extraction,
        "pdf_links": [],
        "references": extract_numbered_references(text),
        "access_state": "full_text_visible",
        "access_signals": [],
        "institution_markers": [],
    }


def extract_pdf_from_signed_solver_url(
    source_url: str,
    *,
    referer: str,
    max_chars: int,
    timeout_ms: int,
    render_page_index: int | None = None,
) -> dict[str, Any]:
    raise LiteratureBrowserError(
        "solver URL replay is disabled; fetch the PDF from the handed-off "
        "Playwright profile"
    )
    validate_url(source_url)
    request = urllib.request.Request(
        source_url,
        headers={
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.1",
            "Referer": referer,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=max(1.0, timeout_ms / 1000),
        ) as response:
            status = int(response.status)
            content_type = str(response.headers.get("content-type") or "").lower()
            final_url = validate_browser_pdf_result_url(
                source_url,
                response.geturl(),
            )
            raw_pdf = response.read(MAX_PDF_BYTES + 1)
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        content_type = str(exc.headers.get("content-type") or "").lower()
        final_url = validate_browser_pdf_result_url(source_url, exc.geturl())
        raw_pdf = exc.read(MAX_PDF_BYTES + 1)
    validate_pdf_payload(raw_pdf, content_type, status)
    pdf_text, pdf_truncated, pdf_pages = extract_pdf_text(raw_pdf, max_chars)
    if not pdf_text:
        raise LiteratureBrowserError(
            "publisher PDF contained no extractable text"
        )
    return {
        "text": pdf_text,
        "text_truncated": pdf_truncated,
        "pages": pdf_pages,
        "source_url": referer,
        "strategy": "solver_resolved_cookie_free_pdf",
        "bytes": len(raw_pdf),
        "content_type": content_type,
        "status": status,
        **pdf_page_payload(raw_pdf, pdf_pages, render_page_index),
    }


def extract_rsc_article_pdf_via_solver(
    context: Any,
    page: Any,
    *source_urls: str,
    max_chars: int,
    timeout_ms: int,
    render_page_index: int | None = None,
) -> dict[str, Any]:
    raise LiteratureBrowserError(
        "direct solver PDF extraction is disabled; use the handed-off "
        "Playwright profile"
    )
    candidates = rsc_article_pdf_navigation_candidates(*source_urls)
    if not candidates:
        raise LiteratureBrowserError(
            "RSC article URL did not yield an official PDF route"
        )
    last_error = ""
    for candidate in candidates:
        try:
            raw_pdf, content_type, final_url, status = (
                request_flaresolverr_same_session_download(
                    candidate,
                    candidate,
                    max_timeout_ms=timeout_ms,
                )
            )
            validate_pdf_payload(raw_pdf, content_type, status)
            pdf_text, pdf_truncated, pdf_pages = extract_pdf_text(
                raw_pdf,
                max_chars=max_chars,
            )
            if not pdf_text:
                raise LiteratureBrowserError(
                    "RSC publisher PDF contained no extractable text"
                )
            return {
                "text": pdf_text,
                "text_truncated": pdf_truncated,
                "pages": pdf_pages,
                "source_url": final_url,
                "strategy": "flaresolverr_same_session_pdf",
                "bytes": len(raw_pdf),
                "content_type": content_type,
                "status": status,
                **pdf_page_payload(raw_pdf, pdf_pages, render_page_index),
            }
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"[:300]
    raise LiteratureBrowserError(
        "RSC signed PDF recovery failed: " + (last_error or "no candidate succeeded")
    )


def extract_direct_pdf_response(
    page: Any,
    response: Any,
    target_url: str,
    final_url: str,
    max_chars: int,
    *,
    timeout_ms: int = PDF_FETCH_TIMEOUT_MS,
    render_page_index: int | None = None,
) -> dict[str, Any]:
    content_type = str(
        (response.headers if response is not None else {}).get("content-type")
        or ""
    ).lower()
    source_url = final_url
    status = int(response.status)
    strategy = "navigation_response"

    def extract_payload(
        raw_payload: bytes,
        payload_content_type: str,
        payload_source_url: str,
        payload_status: int,
        payload_strategy: str,
    ) -> dict[str, Any]:
        validate_pdf_payload(raw_payload, payload_content_type, payload_status)
        pdf_text, pdf_truncated, pdf_pages = extract_pdf_text(
            raw_payload,
            max_chars,
        )
        if not pdf_text:
            raise LiteratureBrowserError(
                "publisher PDF contained no extractable text"
            )
        return {
            "text": pdf_text,
            "text_truncated": pdf_truncated,
            "pages": pdf_pages,
            "source_url": payload_source_url,
            "strategy": payload_strategy,
            "bytes": len(raw_payload),
            "content_type": payload_content_type,
            "status": payload_status,
            **pdf_page_payload(raw_payload, pdf_pages, render_page_index),
        }

    try:
        raw_pdf = response.body()
    except Exception:
        raw_pdf = b""
    navigation_is_pdf = (
        status < 400
        and bool(raw_pdf)
        and (
            "application/pdf" in content_type
            or raw_pdf.startswith(b"%PDF")
        )
    )
    if not navigation_is_pdf:
        if is_supplementary_pdf_url(target_url):
            return extract_supplementary_pdf_from_origin(
                page,
                target_url,
                max_chars,
                timeout_ms=timeout_ms,
                render_page_index=render_page_index,
            )
        raw_pdf, content_type, source_url, status = fetch_pdf_in_browser(
            page,
            target_url,
            timeout_ms=timeout_ms,
        )
        strategy = "browser_fetch"
    try:
        return extract_payload(
            raw_pdf,
            content_type,
            source_url,
            status,
            strategy,
        )
    except Exception as exc:
        if is_supplementary_pdf_url(target_url):
            return extract_supplementary_pdf_from_origin(
                page,
                target_url,
                max_chars,
                timeout_ms=timeout_ms,
                render_page_index=render_page_index,
            )
        if strategy != "navigation_response":
            raise
        try:
            raw_pdf, content_type, source_url, status = fetch_pdf_in_browser(
                page,
                target_url,
                timeout_ms=timeout_ms,
            )
            return extract_payload(
                raw_pdf,
                content_type,
                source_url,
                status,
                "browser_fetch_after_navigation_pdf_error",
            )
        except Exception as refetch_exc:
            raise LiteratureBrowserError(
                "publisher PDF extraction failed after browser refetch: "
                f"{refetch_exc}"
            ) from exc
    if is_supplementary_pdf_url(target_url):
        return extract_supplementary_pdf_from_origin(
            page,
            target_url,
            max_chars,
            timeout_ms=timeout_ms,
            render_page_index=render_page_index,
        )
    raise LiteratureBrowserError("publisher PDF extraction failed")


def validate_pdf_payload(raw_pdf: bytes, content_type: str, status: int) -> None:
    if status >= 400:
        raise LiteratureBrowserError(f"publisher PDF returned HTTP {status}")
    if len(raw_pdf) > MAX_PDF_BYTES:
        raise LiteratureBrowserError(
            "publisher PDF exceeds the configured size limit"
        )
    if "application/pdf" not in content_type and not raw_pdf.startswith(b"%PDF"):
        raise LiteratureBrowserError(
            "publisher supplementary link did not return a PDF"
        )


def safe_page_title(page: Any) -> str:
    try:
        return page.title().strip()
    except Exception as exc:
        if "execution context was destroyed" in str(exc).lower():
            try:
                page.wait_for_load_state("domcontentloaded", timeout=3000)
                return page.title().strip()
            except Exception:
                return ""
        return ""


def safe_page_body_text(page: Any, *, timeout_ms: int = 5000) -> str:
    try:
        return page.locator("body").inner_text(timeout=timeout_ms)
    except Exception:
        return ""


def page_full_text_dom_visible(page: Any) -> bool:
    try:
        return bool(page.evaluate(FULL_TEXT_DOM_SCRIPT))
    except Exception:
        return False


def page_access_sample(page: Any, *, body_timeout_ms: int = 5000) -> dict[str, Any]:
    title = safe_page_title(page)
    final_url = str(getattr(page, "url", "") or "")
    text = safe_page_body_text(page, timeout_ms=body_timeout_ms)
    state = classify_page(final_url, title, text)
    return {
        "title": title,
        "url": final_url,
        "text": text,
        "state": state,
        "full_text_dom": page_full_text_dom_visible(page),
    }


def settle_publisher_shell(
    page: Any,
    sample: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current = sample or page_access_sample(page, body_timeout_ms=10000)
    if PUBLISHER_SHELL_SETTLE_TIMEOUT_MS <= 0:
        return current
    if not publisher_shell_needs_settle(
        str(current.get("url") or ""),
        str(current.get("title") or ""),
        str(current.get("text") or ""),
        str(current.get("state") or ""),
        full_text_dom=bool(current.get("full_text_dom")),
    ):
        return current

    deadline = time.monotonic() + (PUBLISHER_SHELL_SETTLE_TIMEOUT_MS / 1000)
    while time.monotonic() < deadline:
        remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
        if remaining_ms <= 0:
            break
        try:
            page.wait_for_timeout(min(PUBLISHER_SHELL_SETTLE_POLL_MS, remaining_ms))
        except Exception:
            break
        current = page_access_sample(page)
        if not publisher_shell_needs_settle(
            str(current.get("url") or ""),
            str(current.get("title") or ""),
            str(current.get("text") or ""),
            str(current.get("state") or ""),
            full_text_dom=bool(current.get("full_text_dom")),
        ):
            break
    return current


def rsc_article_profile_warm_needed(
    final_url: str,
    page_state: str,
    text: str,
    *,
    full_text_dom: bool = False,
) -> bool:
    host = urlparse(final_url).hostname or ""
    if not host_in_domains(host, ("rsc.org",)):
        return False
    if full_text_dom or full_text_visible(text):
        return False
    if page_state == "challenge":
        return True
    return page_state == "content" and bool(text.strip())


def warm_rsc_article_profile(
    page: Any,
    sample: dict[str, Any],
    *,
    deadline: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    current = dict(sample)
    profile_warm: dict[str, Any] = {"attempted": False}
    if RSC_ARTICLE_WARM_TIMEOUT_MS <= 0:
        return current, profile_warm
    if not rsc_article_profile_warm_needed(
        str(current.get("url") or ""),
        str(current.get("state") or ""),
        str(current.get("text") or ""),
        full_text_dom=bool(current.get("full_text_dom")),
    ):
        return current, profile_warm
    profile_warm["publisher"] = "rsc"

    start = time.monotonic()
    warm_deadline = min(
        deadline - 5.0,
        start + (RSC_ARTICLE_WARM_TIMEOUT_MS / 1000),
    )
    profile_warm.update(
        {
            "attempted": True,
            "initial_state": str(current.get("state") or ""),
            "initial_full_text_dom": bool(current.get("full_text_dom")),
            "reload_attempted": False,
            "success": False,
        }
    )
    if warm_deadline <= start:
        profile_warm["message"] = "RSC warm skipped at request deadline"
        return current, profile_warm

    while time.monotonic() < warm_deadline:
        remaining_ms = int(max(0.0, warm_deadline - time.monotonic()) * 1000)
        if remaining_ms <= 0:
            break
        try:
            page.wait_for_timeout(min(PUBLISHER_SHELL_SETTLE_POLL_MS, remaining_ms))
        except Exception as exc:
            profile_warm["message"] = f"RSC passive warm failed: {exc}"[:240]
            break
        current = page_access_sample(page)
        if not rsc_article_profile_warm_needed(
            str(current.get("url") or ""),
            str(current.get("state") or ""),
            str(current.get("text") or ""),
            full_text_dom=bool(current.get("full_text_dom")),
        ):
            break

    if (
        RSC_ARTICLE_WARM_RELOAD
        and rsc_article_profile_warm_needed(
            str(current.get("url") or ""),
            str(current.get("state") or ""),
            str(current.get("text") or ""),
            full_text_dom=bool(current.get("full_text_dom")),
        )
        and str(current.get("state") or "") == "challenge"
    ):
        remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
        if remaining_ms >= 10000:
            try:
                profile_warm["reload_attempted"] = True
                page.reload(
                    wait_until="domcontentloaded",
                    timeout=min(30000, max(5000, remaining_ms - 5000)),
                )
                try:
                    page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
                current = settle_publisher_shell(page)
            except Exception as exc:
                profile_warm["message"] = f"RSC warm reload failed: {exc}"[:240]

    profile_warm.update(
        {
            "final_state": str(current.get("state") or ""),
            "final_full_text_dom": bool(current.get("full_text_dom")),
            "final_text_chars": len(str(current.get("text") or "")),
            "elapsed_ms": int(max(0.0, time.monotonic() - start) * 1000),
            "success": not rsc_article_profile_warm_needed(
                str(current.get("url") or ""),
                str(current.get("state") or ""),
                str(current.get("text") or ""),
                full_text_dom=bool(current.get("full_text_dom")),
            ),
        }
    )
    return current, profile_warm


def rsc_session_page_verified(page: Any, sample: dict[str, Any]) -> bool:
    final_url = str(sample.get("url") or "")
    if not host_in_domains(urlparse(final_url).hostname or "", ("rsc.org",)):
        return False
    if str(sample.get("state") or "") != "content":
        return False
    if bool(sample.get("full_text_dom")) or full_text_visible(
        str(sample.get("text") or "")
    ):
        return True
    title = str(sample.get("title") or "").strip().casefold()
    if not title or title in {"rsc publishing home", "royal society of chemistry"}:
        return False
    try:
        return bool(
            page.evaluate(
                """() => {
                  const doi = Array.from(document.querySelectorAll(
                    'meta[name="citation_doi"], meta[name="dc.identifier"]'
                  )).some((node) => /10\\.1039\\//i.test(node.content || ''));
                  const pdf = Array.from(document.querySelectorAll('a[href]'))
                    .some((node) => /articlepdf|\\.pdf(?:$|[?#])/i.test(node.href || ''));
                  return doi || pdf;
                }"""
            )
        )
    except Exception:
        return False


def browser_context_cookie_header(context: Any, url: str) -> str:
    try:
        cookies = context.cookies([url])
    except TypeError:
        cookies = context.cookies(url)
    except Exception:
        cookies = []
    if not isinstance(cookies, list):
        return ""
    values: list[str] = []
    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue
        name = str(cookie.get("name") or "").strip()
        value = str(cookie.get("value") or "")
        if name and value:
            values.append(f"{name}={value}")
    return "; ".join(values)


def rsc_article_landing_url_from_doi_redirect(
    target: str,
    *,
    timeout_ms: int,
) -> str:
    doi = rsc_supplement_doi(target)
    if not doi:
        return ""
    request = urllib.request.Request(
        f"https://doi.org/10.1039/{doi}",
        headers={"User-Agent": "Mozilla/5.0"},
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=max(5.0, min(20.0, timeout_ms / 1000)),
        ) as response:
            final_url = str(response.geturl() or "")
    except urllib.error.HTTPError as exc:
        final_url = str(exc.geturl() or "")
    except Exception:
        return ""
    try:
        final_url = validate_url(final_url)
    except LiteratureBrowserError:
        return ""
    return final_url if rsc_article_supplement_candidates_from_page_url(
        final_url,
        target,
    ) else ""


def fetch_pdf_with_profile_cookies(
    context: Any,
    target: str,
    *,
    timeout_ms: int,
    user_agent: str = "",
) -> tuple[bytes, str, str, int]:
    url = validate_url(target)
    headers = {
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.1",
        "User-Agent": user_agent or "Mozilla/5.0",
    }
    cookie_header = browser_context_cookie_header(context, url)
    if cookie_header:
        headers["Cookie"] = cookie_header
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(
            request,
            timeout=max(5.0, min(30.0, timeout_ms / 1000)),
        ) as response:
            status = int(response.status)
            final_url = validate_browser_pdf_result_url(url, response.geturl())
            content_type = str(response.headers.get("content-type") or "").lower()
            raw = response.read(MAX_PDF_BYTES + 1)
    except urllib.error.HTTPError as exc:
        raise LiteratureBrowserError(
            f"publisher PDF returned HTTP {exc.code}"
        ) from exc
    except LiteratureBrowserError:
        raise
    except Exception as exc:
        raise LiteratureBrowserError(
            f"profile-cookie PDF fetch failed: {type(exc).__name__}: {exc}"
        ) from exc
    if len(raw) > MAX_PDF_BYTES:
        raise LiteratureBrowserError("publisher PDF exceeds the configured size limit")
    return raw, content_type, final_url, status


def warm_rsc_profile_for_supplement(
    page: Any,
    landing_url: str,
    target: str,
    *,
    timeout_ms: int,
) -> str:
    landing = validate_url(landing_url)
    if not rsc_article_supplement_candidates_from_page_url(landing, target):
        return ""
    deadline = time.monotonic() + max(5.0, timeout_ms / 1000)
    navigation_error: Exception | None = None
    try:
        page.goto(
            landing,
            wait_until="domcontentloaded",
            timeout=min(30000, max(5000, timeout_ms // 2)),
        )
    except Exception as exc:
        navigation_error = exc
    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass
    remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
    if remaining_ms > 0:
        page.wait_for_timeout(min(10000, max(1000, remaining_ms // 3)))
    title = safe_page_title(page)
    body = safe_page_body_text(page, timeout_ms=5000)
    state = classify_page(page.url or landing, title, body)
    if navigation_error and state != "content":
        return ""
    # Keep the fast RSC SI path bounded. Heavy challenge solving is owned by
    # the maintenance lane; a normal read should only let the persistent
    # browser profile form session state passively, then retry the PDF.
    return ""


def europe_pmc_exact_pmcid(expected_doi: str, *, deadline: float) -> str:
    expected_doi = expected_doi.strip().lower()
    if not re.fullmatch(r"10\.\d{4,9}/\S+", expected_doi):
        raise LiteratureBrowserError("Europe PMC DOI is invalid")
    search_url = (
        "https://www.ebi.ac.uk/europepmc/webservices/rest/search?"
        + urlencode(
            {
                "query": f"DOI:{expected_doi}",
                "format": "json",
                "pageSize": "5",
            }
        )
    )
    validate_url(search_url)
    search_request = urllib.request.Request(
        search_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Scientist literature browser",
        },
    )
    try:
        with urllib.request.urlopen(
            search_request,
            timeout=max(5.0, min(30.0, deadline - time.monotonic())),
        ) as response:
            search_raw = response.read(2 * 1024 * 1024 + 1)
    except Exception as exc:
        raise LiteratureBrowserError(
            "Europe PMC DOI lookup failed: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if len(search_raw) > 2 * 1024 * 1024:
        raise LiteratureBrowserError("Europe PMC DOI response exceeded 2 MiB")
    try:
        search_result = json.loads(search_raw.decode("utf-8", "replace"))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LiteratureBrowserError(
            "Europe PMC DOI response was not valid JSON"
        ) from exc
    rows = (
        search_result.get("resultList", {}).get("result", [])
        if isinstance(search_result, dict)
        else []
    )
    pmcids = sorted(
        {
            str(row.get("pmcid") or "").upper()
            for row in rows
            if isinstance(row, dict)
            and str(row.get("doi") or "").lower() == expected_doi
            and re.fullmatch(r"PMC\d{4,12}", str(row.get("pmcid") or "").upper())
        }
    )
    if len(pmcids) != 1:
        raise LiteratureBrowserError(
            "Europe PMC did not return one exact DOI-to-PMC record"
        )
    return pmcids[0]


def extract_europe_pmc_supplement_pdf(
    expected_doi: str,
    max_chars: int,
    *,
    timeout_ms: int,
    strategy: str,
    expected_filename: str = "",
    rsc_doi_code: str = "",
    rsc_file_stem: str = "",
    render_page_index: int | None = None,
) -> dict[str, Any]:
    expected_doi = expected_doi.strip().lower()
    deadline = time.monotonic() + max(5.0, timeout_ms / 1000)
    pmcid = europe_pmc_exact_pmcid(expected_doi, deadline=deadline)

    archive_url = (
        "https://www.ebi.ac.uk/europepmc/webservices/rest/"
        f"{pmcid}/supplementaryFiles"
    )
    validate_url(archive_url)
    remaining = deadline - time.monotonic()
    if remaining < 5:
        raise LiteratureBrowserError(
            "Europe PMC SI fallback reached the request time budget"
        )
    archive_request = urllib.request.Request(
        archive_url,
        headers={
            "Accept": "application/zip,application/octet-stream;q=0.9",
            "User-Agent": "Scientist literature browser",
        },
    )
    max_archive_bytes = min(128 * 1024 * 1024, MAX_PDF_BYTES * 2)
    archive_raw = b""
    last_archive_error: Exception | None = None
    for attempt in range(2):
        remaining = deadline - time.monotonic()
        if remaining < 5:
            break
        try:
            with urllib.request.urlopen(
                archive_request,
                timeout=max(5.0, min(60.0, remaining)),
            ) as response:
                archive_raw = response.read(max_archive_bytes + 1)
            last_archive_error = None
            break
        except urllib.error.HTTPError as exc:
            last_archive_error = exc
            if exc.code not in {429, 502, 503, 504} or attempt > 0:
                break
        except Exception as exc:
            last_archive_error = exc
            break
        delay = min(2.0, max(0.0, deadline - time.monotonic() - 5.0))
        if delay:
            time.sleep(delay)
    if last_archive_error is not None:
        raise LiteratureBrowserError(
            "Europe PMC supplementary archive fetch failed: "
            f"{type(last_archive_error).__name__}: {last_archive_error}"
        ) from last_archive_error
    if len(archive_raw) > max_archive_bytes:
        raise LiteratureBrowserError(
            "Europe PMC supplementary archive exceeded the configured limit"
        )

    try:
        with zipfile.ZipFile(io.BytesIO(archive_raw)) as archive:
            members = [item for item in archive.infolist() if not item.is_dir()]
            if len(members) > 256:
                raise LiteratureBrowserError(
                    "Europe PMC supplementary archive contained too many files"
                )
            if sum(item.file_size for item in members) > max_archive_bytes * 2:
                raise LiteratureBrowserError(
                    "Europe PMC supplementary archive expanded beyond its limit"
                )
            pdf_members = [
                item
                for item in members
                if item.filename.lower().endswith(".pdf")
                and 0 < item.file_size <= MAX_PDF_BYTES
                and not item.flag_bits & 0x1
            ]
            if expected_filename:
                candidates = [
                    item
                    for item in pdf_members
                    if Path(item.filename).name.casefold()
                    == expected_filename.casefold()
                ]
            else:
                doi_named = [
                    item
                    for item in pdf_members
                    if rsc_doi_code.lower()
                    in re.sub(r"[^a-z0-9]", "", Path(item.filename).name.lower())
                ]
                suffix = rsc_file_stem[len(rsc_doi_code) :].strip("_-")
                indexed = []
                if suffix.isdigit():
                    supplement_index = int(suffix)
                    indexed = [
                        item
                        for item in doi_named
                        if re.search(
                            rf"[-_]s0*{supplement_index}\.pdf$",
                            Path(item.filename).name,
                            flags=re.IGNORECASE,
                        )
                    ]
                candidates = indexed or doi_named
            if len(candidates) != 1:
                raise LiteratureBrowserError(
                    "Europe PMC archive did not contain one matching SI PDF"
                )
            member = candidates[0]
            raw_pdf = archive.read(member)
    except LiteratureBrowserError:
        raise
    except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
        raise LiteratureBrowserError(
            "Europe PMC supplementary archive was invalid"
        ) from exc

    validate_pdf_payload(raw_pdf, "application/pdf", 200)
    pdf_text, pdf_truncated, pdf_pages = extract_pdf_text(raw_pdf, max_chars)
    if not pdf_text:
        raise LiteratureBrowserError(
            "Europe PMC SI PDF contained no extractable text"
        )
    return {
        "text": pdf_text,
        "text_truncated": pdf_truncated,
        "pages": pdf_pages,
        "source_url": archive_url,
        "strategy": strategy,
        "expected_doi": expected_doi,
        "repository_id": pmcid,
        "archive_member": member.filename,
        "bytes": len(raw_pdf),
        "content_type": "application/pdf",
        "status": 200,
        **pdf_page_payload(raw_pdf, pdf_pages, render_page_index),
    }


def extract_rsc_supplement_pdf_via_europe_pmc(
    target_url: str,
    max_chars: int,
    *,
    timeout_ms: int,
    render_page_index: int | None = None,
) -> dict[str, Any]:
    target = validate_url(target_url)
    doi_code = rsc_supplement_doi(target)
    file_stem = rsc_supplement_file_stem(target)
    if not doi_code or not file_stem:
        raise LiteratureBrowserError(
            "Europe PMC RSC SI fallback requires a governed RSC SI URL"
        )
    return extract_europe_pmc_supplement_pdf(
        f"10.1039/{doi_code}",
        max_chars,
        timeout_ms=timeout_ms,
        strategy="europe_pmc_rsc_supplement_fallback",
        rsc_doi_code=doi_code,
        rsc_file_stem=file_stem,
        render_page_index=render_page_index,
    )


def extract_pnas_supplement_pdf_via_europe_pmc(
    target_url: str,
    max_chars: int,
    *,
    timeout_ms: int,
    render_page_index: int | None = None,
) -> dict[str, Any]:
    target = validate_url(target_url)
    expected_doi, expected_filename = pnas_supplement_identity(target)
    if not expected_doi or not expected_filename:
        raise LiteratureBrowserError(
            "Europe PMC PNAS SI fallback requires a governed PNAS SI URL"
        )
    return extract_europe_pmc_supplement_pdf(
        expected_doi,
        max_chars,
        timeout_ms=timeout_ms,
        strategy="europe_pmc_pnas_supplement_fallback",
        expected_filename=expected_filename,
        render_page_index=render_page_index,
    )


def _extract_article_via_europe_pmc(
    target_url: str,
    expected_doi: str,
    max_chars: int,
    *,
    timeout_ms: int,
    source_label: str,
    include_figure_images: bool = False,
    max_figures: int = 12,
    figure_offset: int = 0,
) -> dict[str, Any]:
    target = validate_url(target_url)
    deadline = time.monotonic() + max(5.0, timeout_ms / 1000)
    pmcid = europe_pmc_exact_pmcid(expected_doi, deadline=deadline)
    full_text_url = (
        "https://www.ebi.ac.uk/europepmc/webservices/rest/"
        f"{pmcid}/fullTextXML"
    )
    validate_url(full_text_url)
    request = urllib.request.Request(
        full_text_url,
        headers={
            "Accept": "application/xml,text/xml;q=0.9",
            "User-Agent": "Scientist literature browser",
        },
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=max(5.0, min(60.0, deadline - time.monotonic())),
        ) as response:
            status = int(response.status)
            raw_xml = response.read(32 * 1024 * 1024 + 1)
    except Exception as exc:
        raise LiteratureBrowserError(
            "Europe PMC full-text XML fetch failed: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if status >= 400 or len(raw_xml) > 32 * 1024 * 1024:
        raise LiteratureBrowserError("Europe PMC full-text XML response is invalid")
    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError as exc:
        raise LiteratureBrowserError("Europe PMC full-text XML is invalid") from exc

    def node_text(node: Any) -> str:
        return re.sub(r"\s+", " ", "".join(node.itertext())).strip()

    title = ""
    lines: list[str] = []
    for node in root.iter():
        local_name = str(node.tag).rsplit("}", 1)[-1]
        if local_name == "article-title" and not title:
            title = node_text(node)
        if local_name in {"title", "p"}:
            value = node_text(node)
            if value and (not lines or lines[-1] != value):
                lines.append(value)
    full_text = "\n\n".join(lines)
    normalized_text = re.sub(r"\s+", "", full_text).lower()
    if expected_doi not in normalized_text or len(full_text) < 12000:
        raise LiteratureBrowserError(
            f"Europe PMC {source_label} record did not yield DOI-verified full text"
        )
    limit = max_chars if max_chars > 0 else len(full_text)
    text = full_text[:limit]
    truncated = len(text) < len(full_text)

    figure_candidates: list[dict[str, Any]] = []
    for node in root.iter():
        if str(node.tag).rsplit("}", 1)[-1] != "fig":
            continue
        label = ""
        caption = ""
        graphic_href = ""
        for child in node.iter():
            local_name = str(child.tag).rsplit("}", 1)[-1]
            if local_name == "label" and not label:
                label = node_text(child)
            elif local_name == "caption" and not caption:
                caption = node_text(child)
            elif local_name == "graphic" and not graphic_href:
                for key, value in child.attrib.items():
                    if str(key).rsplit("}", 1)[-1] == "href":
                        graphic_href = Path(str(value)).name
                        break
        if not graphic_href:
            continue
        figure_candidates.append(
            {
                "index": len(figure_candidates),
                "kind": "figure",
                "alt": label,
                "caption": " ".join(
                    part for part in (label, caption) if part
                ).strip(),
                "archive_member": graphic_href,
                "extraction_method": "europe_pmc_jats_xml",
            }
        )

    start = max(0, int(figure_offset))
    count = max(1, min(int(max_figures), MAX_FIGURES_PER_READ))
    figures = figure_candidates[start:start + count]
    if include_figure_images and figures:
        archive_url = (
            "https://www.ebi.ac.uk/europepmc/webservices/rest/"
            f"{pmcid}/supplementaryFiles?includeInlineImage=true"
        )
        validate_url(archive_url)
        archive_request = urllib.request.Request(
            archive_url,
            headers={
                "Accept": "application/zip,application/octet-stream;q=0.9",
                "User-Agent": "Scientist literature browser",
            },
        )
        try:
            with urllib.request.urlopen(
                archive_request,
                timeout=max(5.0, min(60.0, deadline - time.monotonic())),
            ) as response:
                archive_status = int(response.status)
                raw_archive = response.read(MAX_PDF_BYTES + 1)
        except Exception as exc:
            raise LiteratureBrowserError(
                "Europe PMC inline-image archive fetch failed: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        if archive_status >= 400 or len(raw_archive) > MAX_PDF_BYTES:
            raise LiteratureBrowserError(
                "Europe PMC inline-image archive response is invalid"
            )
        try:
            with zipfile.ZipFile(io.BytesIO(raw_archive)) as archive:
                members = {
                    Path(info.filename).name.casefold(): info
                    for info in archive.infolist()
                    if not info.is_dir()
                }
                for figure in figures:
                    member_name = str(figure["archive_member"])
                    member = members.get(member_name.casefold())
                    if member is None:
                        figure["image_access_state"] = "repository_asset_missing"
                        continue
                    if member.file_size <= 0 or member.file_size > MAX_FIGURE_IMAGE_BYTES:
                        figure["image_access_state"] = "repository_asset_invalid"
                        continue
                    raw_image = archive.read(member)
                    if len(raw_image) != member.file_size:
                        figure["image_access_state"] = "repository_asset_invalid"
                        continue
                    extension = Path(member_name).suffix.casefold()
                    mime_type = {
                        ".gif": "image/gif",
                        ".jpeg": "image/jpeg",
                        ".jpg": "image/jpeg",
                        ".png": "image/png",
                        ".tif": "image/tiff",
                        ".tiff": "image/tiff",
                        ".webp": "image/webp",
                    }.get(extension, "")
                    if not mime_type:
                        figure["image_access_state"] = "repository_asset_invalid"
                        continue
                    figure["image_base64"] = base64.b64encode(raw_image).decode(
                        "ascii"
                    )
                    figure["mime_type"] = mime_type
                    figure["image_bytes"] = len(raw_image)
                    figure["image_extraction_method"] = (
                        "europe_pmc_inline_image_archive"
                    )
                    figure["image_access_state"] = "available"
        except (OSError, zipfile.BadZipFile) as exc:
            raise LiteratureBrowserError(
                "Europe PMC inline-image archive was invalid"
            ) from exc

    figure_extraction = {
        "total": len(figure_candidates),
        "offset": start,
        "returned": len(figures),
        "has_more": start + len(figures) < len(figure_candidates),
        "source": "europe_pmc_jats_xml",
    }
    return {
        "success": True,
        "url": target,
        "final_url": full_text_url,
        "status": 200,
        "page_state": "content",
        "title": title,
        "text": text,
        "text_truncated": truncated,
        "text_source": "europe_pmc_full_text_xml",
        "html_text_chars": 0,
        "full_text_dom_visible": True,
        "challenge_bypass": {"attempted": False, "success": False},
        "profile_warm": {"attempted": False},
        "html_extraction": {
            "success": True,
            "strategy": "europe_pmc_full_text_xml",
        },
        "pdf_extraction": {"attempted": False, "success": False},
        "metadata": {"doi": expected_doi, "pmcid": pmcid},
        "figures": figures,
        "figure_extraction": figure_extraction,
        "pdf_links": [],
        "references": extract_numbered_references(text),
        "access_state": classify_access(
            "content",
            text,
            [],
            full_text_dom=True,
        ),
        "access_signals": ["Open repository full text"],
        "institution_markers": [],
        "repository_id": pmcid,
    }


def extract_pnas_article_via_europe_pmc(
    target_url: str,
    max_chars: int,
    *,
    timeout_ms: int,
) -> dict[str, Any]:
    target = validate_url(target_url)
    expected_doi = pnas_article_doi(target)
    if not expected_doi:
        raise LiteratureBrowserError(
            "Europe PMC PNAS article fallback requires a governed PNAS DOI URL"
        )
    return _extract_article_via_europe_pmc(
        target,
        expected_doi,
        max_chars,
        timeout_ms=timeout_ms,
        source_label="PNAS",
    )


def extract_rsc_article_via_europe_pmc(
    target_url: str,
    max_chars: int,
    *,
    timeout_ms: int,
    include_figure_images: bool = False,
    max_figures: int = 12,
    figure_offset: int = 0,
) -> dict[str, Any]:
    target = validate_url(target_url)
    article_code = rsc_article_code_from_url(target)
    if not article_code:
        raise LiteratureBrowserError(
            "Europe PMC RSC article recovery requires a governed RSC article URL"
        )
    expected_doi = f"10.1039/{article_code}"
    return _extract_article_via_europe_pmc(
        f"https://doi.org/{expected_doi}",
        expected_doi,
        max_chars,
        timeout_ms=timeout_ms,
        source_label="RSC",
        include_figure_images=include_figure_images,
        max_figures=max_figures,
        figure_offset=figure_offset,
    )


def extract_rsc_supplement_pdf_from_profile(
    context: Any,
    target_url: str,
    max_chars: int,
    *,
    timeout_ms: int,
    landing_url_hint: str = "",
    user_agent: str = "",
    page: Any | None = None,
    render_page_index: int | None = None,
) -> dict[str, Any]:
    target = validate_url(target_url)
    if not rsc_supplement_doi(target):
        raise LiteratureBrowserError("RSC profile SI fetch requires an RSC SI URL")
    deadline = time.monotonic() + max(5.0, timeout_ms / 1000)
    candidates: list[str] = []
    for landing in (landing_url_hint, rsc_article_landing_url_from_doi_redirect(
        target,
        timeout_ms=min(20000, timeout_ms),
    )):
        if not landing:
            continue
        candidates.extend(
            rsc_article_supplement_candidates_from_page_url(
                landing,
                target,
            )
        )
    target_host = urlparse(target).hostname or ""
    if not candidates or host_in_domains(target_host, ("pubs.rsc.org",)):
        candidates.append(target)
    unique_candidates = list(dict.fromkeys(candidates))
    download_error = ""

    def attempt_profile_fetch(
        *,
        via_context_request: bool = False,
        request_user_agent: str = "",
    ) -> dict[str, Any] | None:
        nonlocal download_error
        for candidate in unique_candidates:
            try:
                if via_context_request:
                    if page is None:
                        continue
                    raw_pdf, content_type, source_url, status = (
                        fetch_pdf_with_context_request(
                            page,
                            candidate,
                            timeout_ms=min(30000, timeout_ms),
                            browser_error="RSC warmed profile context request",
                            user_agent=request_user_agent or user_agent,
                        )
                    )
                else:
                    raw_pdf, content_type, source_url, status = (
                        fetch_pdf_with_profile_cookies(
                            context,
                            candidate,
                            timeout_ms=min(30000, timeout_ms),
                            user_agent=request_user_agent or user_agent,
                        )
                    )
                validate_pdf_payload(raw_pdf, content_type, status)
                pdf_text, pdf_truncated, pdf_pages = extract_pdf_text(
                    raw_pdf,
                    max_chars,
                )
                if not pdf_text:
                    raise LiteratureBrowserError(
                        "publisher PDF contained no extractable text"
                    )
                return {
                    "text": pdf_text,
                    "text_truncated": pdf_truncated,
                    "pages": pdf_pages,
                    "source_url": source_url,
                    "strategy": (
                        "rsc_profile_landing_warmed_supplement_fetch"
                        if via_context_request
                        else "rsc_profile_cookie_supplement_fetch"
                    ),
                    "bytes": len(raw_pdf),
                    "content_type": content_type,
                    "status": status,
                    **pdf_page_payload(raw_pdf, pdf_pages, render_page_index),
                }
            except Exception as exc:
                download_error = (
                    f"{candidate}: {type(exc).__name__}: {exc}"
                )[:240]
        return None

    first_result = attempt_profile_fetch()
    if first_result is not None:
        return first_result

    if page is not None:
        for landing in dict.fromkeys(
            landing
            for landing in (
                landing_url_hint,
                rsc_article_landing_url_from_doi_redirect(
                    target,
                    timeout_ms=min(20000, timeout_ms),
                ),
            )
            if landing
        ):
            remaining_ms = max(5000, min(45000, timeout_ms))
            warmed_user_agent = warm_rsc_profile_for_supplement(
                page,
                landing,
                target,
                timeout_ms=remaining_ms,
            )
            warmed_result = attempt_profile_fetch(
                via_context_request=True,
                request_user_agent=warmed_user_agent,
            )
            if warmed_result is not None:
                return warmed_result

    try:
        repository_timeout_ms = int(
            max(0.0, deadline - time.monotonic()) * 1000
        )
        if repository_timeout_ms < 5000:
            raise LiteratureBrowserError(
                "trusted repository fallback reached the request time budget"
            )
        return extract_rsc_supplement_pdf_via_europe_pmc(
            target,
            max_chars,
            timeout_ms=repository_timeout_ms,
            render_page_index=render_page_index,
        )
    except Exception as exc:
        repository_error = f"{type(exc).__name__}: {exc}"[:240]
    raise LiteratureBrowserError(
        "publisher origin challenge requires RSC SI profile refresh before retry: "
        f"{download_error}; trusted repository fallback failed: {repository_error}"
    )


def extract_pdf_urls_from_viewer(
    raw_html: bytes,
    base_url: str,
) -> list[str]:
    try:
        markup = html.unescape(raw_html.decode("utf-8", "replace")).replace(
            "\\/",
            "/",
        )
    except Exception:
        return []
    values = re.findall(
        r"""(?:href|src|data-[\w-]+)\s*=\s*["']([^"']+)["']""",
        markup,
        flags=re.IGNORECASE,
    )
    values.extend(
        re.findall(
            r"""https?://[^\s"'<>\\]+""",
            markup,
            flags=re.IGNORECASE,
        )
    )
    candidates: list[str] = []
    seen: set[str] = set()
    for value in values:
        target = urljoin(base_url, value.strip())
        if target == base_url or target in seen:
            continue
        if not re.search(
            r"/doi/(?:pdfdirect|pdf|am-pdf)/|\.pdf(?:[?#]|$)",
            target,
            flags=re.IGNORECASE,
        ):
            continue
        if re.search(
            r"supplement|supporting|misc_information|additional[_-]?file",
            target,
            flags=re.IGNORECASE,
        ):
            continue
        try:
            validated = validate_url(target)
        except LiteratureBrowserError:
            continue
        host = urlparse(validated).hostname or ""
        if not browser_pdf_fetch_host_allowed(host):
            continue
        seen.add(validated)
        candidates.append(validated)
        if len(candidates) >= MAX_PDF_CANDIDATES:
            break
    return candidates


def trim_text(value: str, limit: int) -> tuple[str, bool]:
    normalized = re.sub(r"\n{3,}", "\n\n", value).strip()
    if limit <= 0:
        return normalized, False
    if len(normalized) <= limit:
        return normalized, False
    return normalized[:limit].rstrip(), True


def xml_local_name(tag: Any) -> str:
    value = str(tag or "")
    if "}" in value:
        value = value.rsplit("}", 1)[1]
    if ":" in value:
        value = value.rsplit(":", 1)[1]
    return value.casefold()


def compact_xml_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def element_text(element: ET.Element) -> str:
    return compact_xml_text(" ".join(element.itertext()))


def metadata_first(metadata: dict[str, Any], *keys: str) -> str:
    folded = {str(key).casefold(): value for key, value in metadata.items()}
    for key in keys:
        value = folded.get(key.casefold())
        if isinstance(value, list):
            for item in value:
                text = str(item or "").strip()
                if text:
                    return text
        text = str(value or "").strip()
        if text:
            return text
    return ""


def doi_from_literature_url(target: str) -> str:
    try:
        parsed = urlparse(validate_url(target))
    except LiteratureBrowserError:
        return ""
    host = parsed.hostname or ""
    if not host_in_domains(host, ("doi.org",)):
        return ""
    doi = unquote(parsed.path.lstrip("/")).strip()
    return doi if re.match(r"^10\.1016/", doi, flags=re.IGNORECASE) else ""


def elsevier_api_identifiers(
    original_url: str,
    final_url: str,
    metadata: dict[str, Any] | None = None,
) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    metadata = metadata or {}
    for candidate in (
        doi_from_literature_url(original_url),
        doi_from_literature_url(final_url),
        metadata_first(metadata, "citation_doi", "prism.doi", "dc.identifier"),
    ):
        candidate = candidate.strip()
        if re.match(r"^10\.1016/", candidate, flags=re.IGNORECASE):
            values.append(("doi", candidate))
    for candidate in (
        sciencedirect_article_pii(original_url),
        sciencedirect_article_pii(final_url),
        elsevier_pii_from_url(original_url),
        elsevier_pii_from_url(final_url),
        metadata_first(metadata, "citation_pii", "pii", "prism.pii"),
    ):
        candidate = re.sub(r"\W+", "", candidate).strip()
        if candidate:
            values.append(("pii", candidate))
    deduplicated: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for kind, identifier in values:
        key = (kind, identifier.casefold())
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append((kind, identifier))
    return deduplicated


def elsevier_api_configured() -> bool:
    return bool(ELSEVIER_API_KEY and ELSEVIER_INSTTOKEN)


def elsevier_pii_from_url(target: str) -> str:
    pii = sciencedirect_article_pii(target)
    if pii:
        return pii
    try:
        url = validate_url(target)
    except LiteratureBrowserError:
        return ""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host_in_domains(
        host,
        (
            "sciencedirect.com",
            "els-cdn.com",
            "elsevier.com",
            "elsevier.io",
        ),
    ):
        return ""
    match = re.search(
        r"1-s2\.0-([A-Za-z0-9]+)-(?:gr|ga|mmc|fx|main)",
        unquote(parsed.path),
        flags=re.IGNORECASE,
    )
    return match.group(1) if match else ""


def elsevier_object_ref_from_url(target: str) -> str:
    try:
        url = validate_url(target)
    except LiteratureBrowserError:
        return ""
    match = re.search(
        r"1-s2\.0-[A-Za-z0-9]+-([A-Za-z]+\d+)(?:_lrg)?\.[A-Za-z0-9]+",
        unquote(urlparse(url).path),
        flags=re.IGNORECASE,
    )
    return match.group(1).lower() if match else ""


def elsevier_api_only_target(*targets: str) -> bool:
    for raw in targets:
        if doi_from_literature_url(raw):
            return True
        try:
            parsed = urlparse(validate_url(raw))
        except LiteratureBrowserError:
            continue
        host = parsed.hostname or ""
        if host_in_domains(
            host,
            (
                "sciencedirect.com",
                "sciencedirectassets.com",
                "els-cdn.com",
                "elsevier.com",
                "elsevier.io",
            ),
        ):
            return True
    return False


def elsevier_api_headers(accept: str) -> dict[str, str]:
    return {
        "Accept": accept,
        "User-Agent": "scientist-literature-browser-mcp/0.4",
        "X-ELS-APIKey": ELSEVIER_API_KEY,
        "X-ELS-Insttoken": ELSEVIER_INSTTOKEN,
    }


def fetch_elsevier_api_url(
    target: str,
    *,
    accept: str,
    max_bytes: int,
) -> dict[str, Any]:
    if not elsevier_api_configured():
        return {
            "success": False,
            "message": "Elsevier API credentials are not configured",
        }
    parsed = urlparse(target)
    if parsed.scheme != "https" or parsed.hostname != "api.elsevier.com":
        return {
            "success": False,
            "message": "Elsevier API object URL is not an api.elsevier.com HTTPS URL",
        }
    request = urllib.request.Request(
        target,
        headers=elsevier_api_headers(accept),
        method="GET",
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=ELSEVIER_API_TIMEOUT_SECONDS,
        ) as response:
            raw = response.read(max_bytes + 1)
            status = int(response.status)
            content_type = str(response.headers.get("content-type") or "").lower()
            final_url = response.geturl()
    except urllib.error.HTTPError as exc:
        return {
            "success": False,
            "status": int(exc.code),
            "message": f"Elsevier API returned HTTP {int(exc.code)}",
        }
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        return {
            "success": False,
            "message": f"Elsevier API request failed: {type(exc).__name__}",
        }
    if len(raw) > max_bytes:
        return {
            "success": False,
            "status": status,
            "message": "Elsevier API object exceeded the configured size limit",
        }
    return {
        "success": True,
        "status": status,
        "content_type": content_type,
        "final_url": final_url,
        "raw": raw,
    }


def publisher_pdf_request_url(target: str) -> bool:
    try:
        parsed = urlparse(validate_url(target))
    except LiteratureBrowserError:
        return False
    path = unquote(parsed.path).casefold().rstrip("/")
    return (
        path.endswith(".pdf")
        or path.endswith("/pdf")
        or path.endswith("/pdfft")
        or "/article-pdf/" in path
        or "/content/articlepdf/" in path
        or "/pdfft/" in path
    )


def elsevier_api_text_route_enabled(
    target: str,
    *,
    include_figure_images: bool,
    target_is_supplementary_pdf: bool,
) -> bool:
    return (
        not include_figure_images
        and not target_is_supplementary_pdf
        and not publisher_pdf_request_url(target)
    )


def elsevier_content_routes(extraction: dict[str, Any]) -> list[dict[str, str]]:
    pii = str(extraction.get("pii") or "").strip()
    if not pii and extraction.get("identifier_type") == "pii":
        pii = str(extraction.get("identifier") or "").strip()
    routes = [
        {
            "kind": "article_text",
            "route": "elsevier_article_retrieval_api",
            "state": "validated_full_text",
        },
        {
            "kind": "figures",
            "route": "elsevier_object_retrieval_api",
            "state": "api_only_request_with_include_figure_images",
        },
        {
            "kind": "article_pdf",
            "route": "elsevier_article_retrieval_api_pdf",
            "state": "api_only_request_direct_pdf_url",
        },
        {
            "kind": "supplementary_pdf",
            "route": "elsevier_object_retrieval_api",
            "state": "api_only_request_resource_kind_supplementary_pdf",
        },
    ]
    if pii:
        landing = f"https://www.sciencedirect.com/science/article/pii/{pii}"
        for item in routes:
            item["article_landing_url"] = landing
    return routes


def xml_attributes(element: ET.Element) -> dict[str, str]:
    return {xml_local_name(key): str(value) for key, value in element.attrib.items()}


def xml_first_descendant_text(element: ET.Element, *names: str) -> str:
    wanted = {name.casefold() for name in names}
    for candidate in element.iter():
        if candidate is element:
            continue
        if xml_local_name(candidate.tag) in wanted:
            text = element_text(candidate)
            if text:
                return text
    return ""


def elsevier_api_object_records(root: ET.Element) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for element in root.iter():
        if xml_local_name(element.tag) != "object":
            continue
        attrs = xml_attributes(element)
        ref = str(attrs.get("ref") or "").strip().lower()
        url = element_text(element)
        if not ref or not url:
            continue
        record: dict[str, Any] = {
            "ref": ref,
            "category": str(attrs.get("category") or "").strip().lower(),
            "type": str(attrs.get("type") or "").strip(),
            "mimetype": str(attrs.get("mimetype") or "").strip().lower(),
            "url": url,
        }
        for key in ("width", "height", "size"):
            try:
                record[key] = max(0, int(attrs.get(key) or 0))
            except (TypeError, ValueError):
                record[key] = 0
        records.append(record)
    return records


def natural_object_ref_key(ref: str) -> tuple[str, int, str]:
    match = re.fullmatch(r"([a-z]+)(\d+)", ref.casefold())
    if not match:
        return (ref.casefold(), 0, ref.casefold())
    return (match.group(1), int(match.group(2)), ref.casefold())


def preferred_elsevier_objects(
    objects: list[dict[str, Any]],
    *,
    mimetype_prefix: str,
) -> dict[str, dict[str, Any]]:
    category_rank = {"high": 0, "standard": 1, "thumbnail": 2}
    selected: dict[str, dict[str, Any]] = {}
    for item in objects:
        ref = str(item.get("ref") or "")
        mimetype = str(item.get("mimetype") or "").casefold()
        if not ref or not mimetype.startswith(mimetype_prefix):
            continue
        current = selected.get(ref)
        if current is None or category_rank.get(
            str(item.get("category") or ""), 99
        ) < category_rank.get(str(current.get("category") or ""), 99):
            selected[ref] = item
    return selected


def elsevier_api_figure_records(
    root: ET.Element,
    objects: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    image_objects = preferred_elsevier_objects(objects, mimetype_prefix="image/")
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for element in root.iter():
        if xml_local_name(element.tag) not in {"figure", "fig"}:
            continue
        refs: list[str] = []
        for candidate in element.iter():
            attrs = xml_attributes(candidate)
            for key in ("locator", "ref", "refid"):
                value = str(attrs.get(key) or "").strip().lower()
                if value in image_objects and value not in refs:
                    refs.append(value)
            href = str(attrs.get("href") or "").strip()
            match = re.search(r"/([A-Za-z]+\d+)$", href)
            if match:
                value = match.group(1).lower()
                if value in image_objects and value not in refs:
                    refs.append(value)
        if not refs:
            continue
        label = xml_first_descendant_text(element, "label")
        caption = xml_first_descendant_text(element, "caption")
        for ref in refs:
            if ref in seen:
                continue
            seen.add(ref)
            image_object = image_objects[ref]
            records.append(
                {
                    "index": len(records),
                    "ref": ref,
                    "kind": "figure",
                    "alt": label or ref,
                    "caption": caption or label or ref,
                    "src": str(image_object.get("url") or ""),
                    "extraction_method": "elsevier_article_retrieval_api",
                    "object_category": str(image_object.get("category") or ""),
                    "object_mimetype": str(image_object.get("mimetype") or ""),
                    "object_width": int(image_object.get("width") or 0),
                    "object_height": int(image_object.get("height") or 0),
                }
            )
    for ref in sorted(image_objects, key=natural_object_ref_key):
        if ref in seen:
            continue
        seen.add(ref)
        image_object = image_objects[ref]
        records.append(
            {
                "index": len(records),
                "ref": ref,
                "kind": "figure",
                "alt": ref,
                "caption": ref,
                "src": str(image_object.get("url") or ""),
                "extraction_method": "elsevier_article_retrieval_api_object",
                "object_category": str(image_object.get("category") or ""),
                "object_mimetype": str(image_object.get("mimetype") or ""),
                "object_width": int(image_object.get("width") or 0),
                "object_height": int(image_object.get("height") or 0),
            }
        )
    return records


def extract_elsevier_api_xml(
    raw_xml: bytes,
    *,
    max_chars: int,
) -> dict[str, Any]:
    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError as exc:
        return {
            "success": False,
            "message": f"Elsevier API returned invalid XML: {exc}"[:240],
        }

    title = ""
    doi = ""
    pii = ""
    for element in root.iter():
        name = xml_local_name(element.tag)
        if not title and name in {"title", "titletext"}:
            title = element_text(element)
        elif not doi and name == "doi":
            doi = element_text(element)
        elif not pii and name == "pii":
            pii = element_text(element)

    objects = elsevier_api_object_records(root)
    figures = elsevier_api_figure_records(root, objects)

    article_roots = [
        element
        for element in root.iter()
        if xml_local_name(element.tag) in {"body", "originaltext", "rawtext"}
    ]
    if not article_roots:
        return {
            "success": False,
            "message": "Elsevier API response did not contain a full article body",
            "title": title,
            "doi": doi,
            "pii": pii,
            "objects": objects,
            "api_figures": figures,
        }

    block_names = {
        "acknowledgment",
        "caption",
        "label",
        "list-item",
        "para",
        "rawtext",
        "section-title",
        "simple-para",
        "title",
    }
    blocks: list[str] = []
    seen: set[str] = set()
    for root_element in article_roots:
        for element in root_element.iter():
            if xml_local_name(element.tag) not in block_names:
                continue
            text = element_text(element)
            if not text or text in seen:
                continue
            seen.add(text)
            blocks.append(text)
    if not blocks:
        text = element_text(article_roots[0])
    else:
        text = "\n\n".join(blocks)
    full_text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(full_text) < 800:
        return {
            "success": False,
            "message": "Elsevier API article body was too short for full-text evidence",
            "text_chars": len(full_text),
            "title": title,
            "doi": doi,
            "pii": pii,
            "objects": objects,
            "api_figures": figures,
        }
    trimmed, truncated = trim_text(full_text, max_chars)
    return {
        "success": True,
        "text": trimmed,
        "text_chars": len(full_text),
        "text_truncated": truncated,
        "title": title,
        "doi": doi,
        "pii": pii,
        "objects": objects,
        "api_figures": figures,
        "section_headings": section_heading_count(full_text),
    }


def fetch_elsevier_article_api(
    original_url: str,
    final_url: str,
    metadata: dict[str, Any] | None,
    max_chars: int,
) -> dict[str, Any]:
    if not elsevier_api_configured():
        return {"attempted": False, "success": False}
    identifiers = elsevier_api_identifiers(original_url, final_url, metadata)
    if not identifiers:
        return {"attempted": False, "success": False}
    attempts: list[dict[str, Any]] = []
    for kind, identifier in identifiers[:3]:
        endpoint = (
            f"https://api.elsevier.com/content/article/{kind}/"
            f"{quote(identifier, safe='')}"
        )
        query = urlencode({"view": "FULL", "httpAccept": "text/xml"})
        request = urllib.request.Request(
            f"{endpoint}?{query}",
            headers={
                "Accept": "text/xml",
                "User-Agent": "scientist-literature-browser-mcp/0.4",
                "X-ELS-APIKey": ELSEVIER_API_KEY,
                "X-ELS-Insttoken": ELSEVIER_INSTTOKEN,
            },
            method="GET",
        )
        attempt: dict[str, Any] = {
            "identifier_type": kind,
            "identifier": identifier,
        }
        attempts.append(attempt)
        try:
            with urllib.request.urlopen(
                request,
                timeout=ELSEVIER_API_TIMEOUT_SECONDS,
            ) as response:
                raw = response.read(MAX_ELSEVIER_API_RESPONSE_BYTES + 1)
                status = int(response.status)
        except urllib.error.HTTPError as exc:
            attempt.update(
                {
                    "status": int(exc.code),
                    "success": False,
                    "message": f"Elsevier API returned HTTP {int(exc.code)}",
                }
            )
            continue
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            attempt.update(
                {
                    "success": False,
                    "message": f"Elsevier API request failed: {type(exc).__name__}",
                }
            )
            continue
        if len(raw) > MAX_ELSEVIER_API_RESPONSE_BYTES:
            attempt.update(
                {
                    "status": status,
                    "success": False,
                    "message": "Elsevier API response exceeded the size limit",
                }
            )
            continue
        extracted = extract_elsevier_api_xml(raw, max_chars=max_chars)
        attempt.update(
            {
                "status": status,
                "success": bool(extracted.get("success")),
                "text_chars": int(extracted.get("text_chars") or 0),
                "message": extracted.get("message", ""),
            }
        )
        if extracted.get("success"):
            return {
                "attempted": True,
                "success": True,
                "source": "elsevier_article_retrieval_api",
                "identifier_type": kind,
                "identifier": identifier,
                "status": status,
                "attempts": attempts,
                **extracted,
            }
    return {
        "attempted": True,
        "success": False,
        "source": "elsevier_article_retrieval_api",
        "attempts": attempts,
        "message": attempts[-1].get("message", "Elsevier API did not return full text"),
    }


def elsevier_api_failure_message(
    extraction: dict[str, Any],
    *,
    default: str,
) -> str:
    attempts = extraction.get("attempts")
    if isinstance(attempts, list):
        for attempt in reversed(attempts):
            if isinstance(attempt, dict) and attempt.get("message"):
                return str(attempt.get("message"))[:400]
    return str(extraction.get("message") or default)[:400]


def selected_elsevier_api_figures(
    extraction: dict[str, Any],
    *,
    figure_offset: int,
    max_figures: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    all_figures = [
        dict(item)
        for item in extraction.get("api_figures") or []
        if isinstance(item, dict)
    ]
    total = len(all_figures)
    start = max(0, int(figure_offset))
    limit = max(1, min(int(max_figures), MAX_FIGURES_PER_READ))
    selected = [dict(item) for item in all_figures[start:start + limit]]
    for offset, item in enumerate(selected, start=start):
        item["index"] = offset
    return selected, {
        "total": total,
        "offset": start,
        "returned": len(selected),
        "has_more": start + len(selected) < total,
    }


def attach_elsevier_api_figure_images(figures: list[dict[str, Any]]) -> None:
    for figure in figures:
        source = str(figure.get("src") or "").strip()
        ref = str(figure.get("ref") or "").strip() or source
        fetched = fetch_elsevier_api_url(
            source,
            accept="image/*,*/*;q=0.1",
            max_bytes=MAX_FIGURE_IMAGE_BYTES,
        )
        if not fetched.get("success"):
            raise LiteratureBrowserError(
                "Elsevier API figure retrieval failed for "
                f"{ref}: {fetched.get('message') or 'unknown error'}"
            )
        raw = fetched.get("raw")
        if not isinstance(raw, bytes) or not raw:
            raise LiteratureBrowserError(
                f"Elsevier API figure retrieval returned an empty payload for {ref}"
            )
        content_type = str(fetched.get("content_type") or "").split(";", 1)[0].lower()
        if not content_type.startswith("image/"):
            raise LiteratureBrowserError(
                "Elsevier API figure retrieval did not return image bytes for "
                f"{ref}: {content_type or 'unknown content type'}"
            )
        figure["image_base64"] = base64.b64encode(raw).decode("ascii")
        figure["mime_type"] = content_type
        figure["image_bytes"] = len(raw)
        figure["image_extraction_method"] = "elsevier_object_retrieval_api"


def elsevier_api_pdf_objects(extraction: dict[str, Any]) -> dict[str, dict[str, Any]]:
    objects = [
        item
        for item in extraction.get("objects") or []
        if isinstance(item, dict)
    ]
    return preferred_elsevier_objects(objects, mimetype_prefix="application/pdf")


def elsevier_api_pdf_links(extraction: dict[str, Any]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for ref, item in sorted(
        elsevier_api_pdf_objects(extraction).items(),
        key=lambda pair: natural_object_ref_key(pair[0]),
    ):
        href = str(item.get("url") or "")
        if not href:
            continue
        links.append(
            {
                "href": href,
                "text": ref,
                "source": "elsevier_object_retrieval_api",
            }
        )
    return links


def extract_elsevier_api_pdf_object(
    target_url: str,
    object_url: str,
    max_chars: int,
    *,
    render_page_index: int | None = None,
) -> dict[str, Any]:
    fetched = fetch_elsevier_api_url(
        object_url,
        accept="application/pdf,*/*;q=0.1",
        max_bytes=MAX_PDF_BYTES,
    )
    if not fetched.get("success"):
        raise LiteratureBrowserError(
            "Elsevier API PDF retrieval failed: "
            f"{fetched.get('message') or 'unknown error'}"
        )
    raw = fetched.get("raw")
    if not isinstance(raw, bytes):
        raw = b""
    content_type = str(fetched.get("content_type") or "")
    status = int(fetched.get("status") or 0)
    validate_pdf_payload(raw, content_type, status)
    pdf_text, pdf_truncated, pdf_pages = extract_pdf_text(raw, max_chars)
    if not pdf_text:
        raise LiteratureBrowserError(
            "Elsevier API PDF contained no extractable text"
        )
    return {
        "text": pdf_text,
        "text_truncated": pdf_truncated,
        "pages": pdf_pages,
        "source_url": str(fetched.get("final_url") or object_url),
        "strategy": "elsevier_object_retrieval_api",
        "bytes": len(raw),
        "content_type": content_type,
        "status": status,
        **pdf_page_payload(raw, pdf_pages, render_page_index),
    }


def extract_elsevier_api_supplement_pdf(
    target_url: str,
    extraction: dict[str, Any],
    max_chars: int,
    *,
    render_page_index: int | None = None,
) -> dict[str, Any]:
    pdf_objects = elsevier_api_pdf_objects(extraction)
    requested_ref = elsevier_object_ref_from_url(target_url)
    if requested_ref:
        item = pdf_objects.get(requested_ref)
        if item is None:
            raise LiteratureBrowserError(
                "Elsevier API article metadata did not expose supplementary "
                f"object {requested_ref}"
            )
    elif len(pdf_objects) == 1:
        item = next(iter(pdf_objects.values()))
    else:
        raise LiteratureBrowserError(
            "Elsevier API supplementary PDF request requires a specific object ref"
        )
    return extract_elsevier_api_pdf_object(
        target_url,
        str(item.get("url") or ""),
        max_chars,
        render_page_index=render_page_index,
    )


def fetch_elsevier_article_pdf_api(
    original_url: str,
    final_url: str,
    metadata: dict[str, Any] | None,
    max_chars: int,
    *,
    render_page_index: int | None = None,
) -> dict[str, Any]:
    if not elsevier_api_configured():
        return {
            "attempted": False,
            "success": False,
            "message": "Elsevier API credentials are not configured",
        }
    identifiers = elsevier_api_identifiers(original_url, final_url, metadata)
    if not identifiers:
        return {
            "attempted": False,
            "success": False,
            "message": "Elsevier API article identifiers were not found",
        }
    attempts: list[dict[str, Any]] = []
    for kind, identifier in identifiers[:3]:
        endpoint = (
            f"https://api.elsevier.com/content/article/{kind}/"
            f"{quote(identifier, safe='')}"
        )
        target = f"{endpoint}?{urlencode({'httpAccept': 'application/pdf'})}"
        attempt: dict[str, Any] = {
            "identifier_type": kind,
            "identifier": identifier,
        }
        attempts.append(attempt)
        fetched = fetch_elsevier_api_url(
            target,
            accept="application/pdf,*/*;q=0.1",
            max_bytes=MAX_PDF_BYTES,
        )
        attempt.update(
            {
                "success": bool(fetched.get("success")),
                "status": fetched.get("status"),
                "message": fetched.get("message", ""),
            }
        )
        if not fetched.get("success"):
            continue
        raw = fetched.get("raw")
        if not isinstance(raw, bytes):
            raw = b""
        content_type = str(fetched.get("content_type") or "")
        status = int(fetched.get("status") or 0)
        try:
            validate_pdf_payload(raw, content_type, status)
            pdf_text, pdf_truncated, pdf_pages = extract_pdf_text(raw, max_chars)
            if not pdf_text:
                raise LiteratureBrowserError(
                    "Elsevier API article PDF contained no extractable text"
                )
        except Exception as exc:
            attempt.update(
                {
                    "success": False,
                    "message": f"Elsevier API article PDF rejected: {exc}"[:400],
                }
            )
            continue
        return {
            "attempted": True,
            "success": True,
            "source": "elsevier_article_retrieval_api_pdf",
            "identifier_type": kind,
            "identifier": identifier,
            "status": status,
            "source_url": str(fetched.get("final_url") or target),
            "content_type": content_type,
            "bytes": len(raw),
            "pages": pdf_pages,
            "text": pdf_text,
            "text_truncated": pdf_truncated,
            "text_chars": len(pdf_text),
            "strategy": "elsevier_article_retrieval_api_pdf",
            "attempts": attempts,
            **pdf_page_payload(raw, pdf_pages, render_page_index),
        }
    return {
        "attempted": True,
        "success": False,
        "source": "elsevier_article_retrieval_api_pdf",
        "attempts": attempts,
        "message": attempts[-1].get(
            "message",
            "Elsevier API did not return an article PDF",
        ),
    }


def elsevier_api_pdf_result(
    target: str,
    extraction: dict[str, Any],
    article_extraction: dict[str, Any] | None = None,
) -> dict[str, Any]:
    details = dict(extraction)
    text = str(details.pop("text"))
    truncated = bool(details.pop("text_truncated"))
    figures = list(details.pop("figures", []))
    figure_extraction = dict(details.pop("figure_extraction", {}))
    references = extract_numbered_references(text)
    return {
        "success": True,
        "url": target,
        "final_url": str(details.get("source_url") or target),
        "status": int(details.get("status") or 200),
        "page_state": "content",
        "title": str((article_extraction or {}).get("title") or "Elsevier PDF"),
        "text": text,
        "text_truncated": truncated,
        "text_source": "pdf",
        "html_text_chars": 0,
        "full_text_dom_visible": True,
        "challenge_bypass": {"attempted": False, "success": False},
        "html_extraction": {
            "attempted": False,
            "success": False,
            "candidates": [],
        },
        "pdf_extraction": {
            "attempted": True,
            "success": True,
            "text_chars": len(text),
            **details,
        },
        "metadata": {},
        "figures": figures,
        "figure_extraction": figure_extraction,
        "pdf_links": elsevier_api_pdf_links(article_extraction or {}),
        "content_routes": elsevier_content_routes(article_extraction or {}),
        "references": references,
        "access_state": "institutional_full_text",
        "access_signals": ["Institutional access"],
        "institution_markers": ["Elsevier Article Retrieval API PDF"],
    }


def elsevier_api_article_result(
    target: str,
    extraction: dict[str, Any],
    *,
    figures: list[dict[str, Any]] | None = None,
    figure_extraction: dict[str, Any] | None = None,
) -> dict[str, Any]:
    text = str(extraction.get("text") or "")
    title = str(extraction.get("title") or "").strip() or "Elsevier article"
    metadata = {
        "citation_doi": [str(extraction.get("doi") or "")],
        "citation_pii": [str(extraction.get("pii") or "")],
        "source": ["elsevier_article_retrieval_api"],
    }
    metadata = {
        key: [item for item in values if item]
        for key, values in metadata.items()
        if any(values)
    }
    references = extract_numbered_references(text)
    return {
        "success": True,
        "url": target,
        "final_url": target,
        "status": int(extraction.get("status") or 200),
        "page_state": "content",
        "title": title,
        "text": text,
        "text_truncated": bool(extraction.get("text_truncated")),
        "text_source": "elsevier_api_xml",
        "html_text_chars": 0,
        "full_text_dom_visible": True,
        "challenge_bypass": {"attempted": False, "success": False},
        "html_extraction": {
            "attempted": True,
            "success": True,
            "selector": "elsevier_article_retrieval_api",
            "text_chars": int(extraction.get("text_chars") or len(text)),
        },
        "pdf_extraction": {"attempted": False, "success": False, "attempts": []},
        "metadata": metadata,
        "figures": figures or [],
        "figure_extraction": figure_extraction
        or {
            "total": len(extraction.get("api_figures") or []),
            "offset": 0,
            "returned": 0,
            "has_more": bool(extraction.get("api_figures")),
        },
        "pdf_links": elsevier_api_pdf_links(extraction),
        "content_routes": elsevier_content_routes(extraction),
        "references": references,
        "access_state": "institutional_full_text",
        "access_signals": ["Institutional access"],
        "institution_markers": ["Elsevier Article Retrieval API FULL"],
    }


def elsevier_api_only_result(
    original_url: str,
    target: str,
    *,
    max_chars: int,
    include_figure_images: bool,
    max_figures: int,
    figure_offset: int,
    target_is_supplementary_pdf: bool,
    target_is_article_pdf_request: bool,
) -> dict[str, Any]:
    if not elsevier_api_configured():
        raise LiteratureBrowserError(
            "Elsevier API-only route requires configured Elsevier API credentials"
        )
    article_extraction = fetch_elsevier_article_api(
        original_url,
        target,
        None,
        max_chars,
    )
    if not article_extraction.get("success"):
        raise LiteratureBrowserError(
            "Elsevier API-only article retrieval failed: "
            + elsevier_api_failure_message(
                article_extraction,
                default="Elsevier API did not return full article metadata",
            )
        )
    if target_is_supplementary_pdf:
        extracted = extract_elsevier_api_supplement_pdf(
            target,
            article_extraction,
            max_chars,
            render_page_index=(figure_offset if include_figure_images else None),
        )
        return supplementary_pdf_result(target, extracted)
    if target_is_article_pdf_request:
        pdf_extraction = fetch_elsevier_article_pdf_api(
            original_url,
            target,
            None,
            max_chars,
            render_page_index=(figure_offset if include_figure_images else None),
        )
        if not pdf_extraction.get("success"):
            raise LiteratureBrowserError(
                "Elsevier API-only article PDF retrieval failed: "
                + elsevier_api_failure_message(
                    pdf_extraction,
                    default="Elsevier API did not return an article PDF",
                )
            )
        return elsevier_api_pdf_result(
            target,
            pdf_extraction,
            article_extraction=article_extraction,
        )
    figures: list[dict[str, Any]] | None = None
    figure_extraction: dict[str, Any] | None = None
    if include_figure_images:
        figures, figure_extraction = selected_elsevier_api_figures(
            article_extraction,
            figure_offset=figure_offset,
            max_figures=max_figures,
        )
        object_error = ""
        if figures:
            try:
                attach_elsevier_api_figure_images(figures)
            except LiteratureBrowserError as exc:
                object_error = str(exc)[:240]
                figures = []
        if not figures:
            pdf_page = fetch_elsevier_article_pdf_api(
                original_url,
                target,
                None,
                max_chars,
                render_page_index=figure_offset,
            )
            if not pdf_page.get("success"):
                detail = elsevier_api_failure_message(
                    pdf_page,
                    default="Elsevier API did not return an article PDF",
                )
                if object_error:
                    detail = f"{object_error}; {detail}"[:400]
                raise LiteratureBrowserError(
                    "Elsevier API-only figure retrieval failed: " + detail
                )
            figures = [
                dict(item)
                for item in pdf_page.get("figures") or []
                if isinstance(item, dict)
            ]
            if not figures:
                raise LiteratureBrowserError(
                    "Elsevier API article PDF returned no rendered page"
                )
            for figure in figures:
                figure["extraction_method"] = (
                    "elsevier_article_retrieval_api_pdf_page"
                )
                figure["image_extraction_method"] = (
                    "elsevier_article_retrieval_api_pdf_page"
                )
            figure_extraction = {
                **dict(pdf_page.get("figure_extraction") or {}),
                "fallback": "article_pdf_page",
                "source": "elsevier_article_retrieval_api_pdf",
            }
    return elsevier_api_article_result(
        target,
        article_extraction,
        figures=figures,
        figure_extraction=figure_extraction,
    )


REFERENCE_HEADING_RE = re.compile(
    r"(?im)^\s*(?:references|bibliography)\s*$"
)
REFERENCE_ENTRY_RE = re.compile(
    r"(?m)^\s*(?:(\d{1,4})\.|\[(\d{1,4})\])\s*"
)
REFERENCE_DOI_RE = re.compile(
    r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b",
    re.IGNORECASE,
)
MAX_STRUCTURED_REFERENCES = 300
MAX_REFERENCE_TEXT_CHARS = 1000


def extract_numbered_references(text: str) -> dict[str, Any]:
    headings = list(REFERENCE_HEADING_RE.finditer(text))
    if not headings:
        return {
            "state": "not_found",
            "count": 0,
            "truncated": False,
            "items": [],
        }

    section = text[headings[-1].end():]
    matches = list(REFERENCE_ENTRY_RE.finditer(section))
    items: list[dict[str, Any]] = []
    for index, match in enumerate(matches[:MAX_STRUCTURED_REFERENCES]):
        number = int(match.group(1) or match.group(2))
        end = matches[index + 1].start() if index + 1 < len(matches) else len(section)
        citation = section[match.end():end]
        citation = re.sub(
            r"(?:\n\s*(?:Google Scholar|Crossref)\s*)+$",
            "",
            citation,
            flags=re.IGNORECASE,
        )
        citation = re.sub(r"\s+", " ", citation).strip()
        if not citation:
            continue
        doi_match = REFERENCE_DOI_RE.search(citation)
        doi = doi_match.group(0) if doi_match else ""
        doi = re.sub(r"(?i)\.?crossref$", "", doi).rstrip(".,;)")
        items.append(
            {
                "number": number,
                "doi": doi,
                "citation": citation[:MAX_REFERENCE_TEXT_CHARS],
                "citation_truncated": len(citation) > MAX_REFERENCE_TEXT_CHARS,
            }
        )
    return {
        "state": "available" if items else "not_found",
        "count": len(items),
        "truncated": len(matches) > MAX_STRUCTURED_REFERENCES,
        "items": items,
    }


def queued_read_timeout(
    wait_ms: int,
    jobs_ahead: int,
    request_budget_seconds: int | None = None,
) -> float:
    del wait_ms
    del jobs_ahead
    budget = (
        READ_BUDGET_SECONDS
        if request_budget_seconds is None
        else max(45, min(int(request_budget_seconds), READ_BUDGET_SECONDS))
    )
    return min(
        MAX_QUEUED_READ_TIMEOUT_SECONDS,
        max(50.0, budget + 5.0),
    )


class BrowserManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._pending_count = 0
        self._completed_reads = 0
        self._closed_pages = 0
        self._open_pages_after_cleanup = 0
        self._last_read_finished_at = 0.0
        self._active_read_started_at = 0.0
        self._active_read_started_monotonic = 0.0
        self._active_read_budget_seconds = 0
        self._cancelled_queued_reads = 0
        self._expired_queued_reads = 0
        self._rejected_reads = 0
        self._active_wait_timeouts = 0
        self._publisher_landing_urls: dict[str, str] = {}
        self._requests: queue.Queue[
            tuple[
                str,
                int,
                int,
                bool,
                int,
                int,
                int,
                float,
                str,
                bool,
                Future[dict[str, Any]],
            ]
        ] = queue.Queue()
        self._playwright: Any = None
        self._browser: Any = None
        self._context: Any = None
        self._thread = threading.Thread(
            target=self._serve,
            name="literature-browser",
            daemon=True,
        )
        self._thread.start()

    @staticmethod
    def _context_pages(context: Any) -> list[Any]:
        try:
            return list(context.pages)
        except Exception:
            return []

    def _record_page_state(self, context: Any) -> None:
        with self._state_lock:
            self._open_pages_after_cleanup = len(self._context_pages(context))

    def _close_page(self, page: Any) -> None:
        try:
            page.close()
        except Exception:
            return
        with self._state_lock:
            self._closed_pages += 1

    def _enforce_page_limit(self, new_page: Any) -> None:
        context = self._context
        if context is None:
            return
        pages = self._context_pages(context)
        if len(pages) > MAX_OPEN_PAGES:
            self._close_page(new_page)
        self._record_page_state(context)

    def _configure_context(self, context: Any) -> Any:
        try:
            context.route(
                f"https://{SCIENCEDIRECT_CHINA_STATIC_HOST}/**",
                proxy_sciencedirect_static_asset,
            )
        except Exception:
            pass
        try:
            context.on("page", self._enforce_page_limit)
        except Exception:
            pass
        self._record_page_state(context)
        return context

    def _acquire_task_page(self, context: Any) -> Any:
        pages = self._context_pages(context)
        if pages:
            task_page = pages[0]
            for stale_page in pages[1:]:
                self._close_page(stale_page)
        else:
            task_page = context.new_page()
        self._record_page_state(context)
        return task_page

    def _trim_task_pages(self, context: Any, task_page: Any) -> None:
        for candidate in self._context_pages(context):
            if candidate is not task_page:
                self._close_page(candidate)
        self._record_page_state(context)

    def _reset_after_read(self, context: Any, task_page: Any) -> None:
        keepalive = None
        try:
            keepalive = context.new_page()
        except Exception:
            pass
        for candidate in self._context_pages(context):
            if candidate is not keepalive:
                self._close_page(candidate)
        if not self._context_pages(context):
            try:
                keepalive = context.new_page()
            except Exception:
                pass
        self._record_page_state(context)
        with self._state_lock:
            self._completed_reads += 1
            self._last_read_finished_at = time.time()

    def status(self) -> dict[str, Any]:
        with self._pending_lock:
            pending = self._pending_count
        with self._state_lock:
            active_age_seconds = (
                max(0.0, time.monotonic() - self._active_read_started_monotonic)
                if self._active_read_started_monotonic
                else 0.0
            )
            stalled = bool(
                self._active_read_started_monotonic
                and active_age_seconds
                > self._active_read_budget_seconds + ACTIVE_READ_STALL_GRACE_SECONDS
            )
            return {
                "pending_reads": pending,
                "max_pending_reads": MAX_PENDING_READS,
                "completed_reads": self._completed_reads,
                "cancelled_queued_reads": self._cancelled_queued_reads,
                "expired_queued_reads": self._expired_queued_reads,
                "rejected_reads": self._rejected_reads,
                "active_wait_timeouts": self._active_wait_timeouts,
                "closed_pages": self._closed_pages,
                "open_pages_after_cleanup": self._open_pages_after_cleanup,
                "max_open_pages": MAX_OPEN_PAGES,
                "idle_page_limit": 1,
                "close_task_pages_after_read": True,
                "persistent_profile_history": True,
                "challenge_solver": {
                    "mode": "ephemeral_browser_per_request",
                    **_FLARESOLVERR_REQUEST_SLOTS.status(),
                },
                "last_read_finished_at": self._last_read_finished_at or None,
                "active_read_started_at": self._active_read_started_at or None,
                "active_read_age_seconds": round(active_age_seconds, 3),
                "active_read_budget_seconds": self._active_read_budget_seconds or None,
                "worker_alive": self._thread.is_alive(),
                "stalled": stalled,
            }

    def _serve(self) -> None:
        while True:
            (
                url,
                wait_ms,
                max_chars,
                include_figure_images,
                max_figures,
                figure_offset,
                timeout_seconds,
                deadline,
                landing_url_hint,
                maintenance_session_refresh,
                request_priority,
                future,
            ) = self._requests.get()
            try:
                if future.cancelled():
                    with self._state_lock:
                        self._cancelled_queued_reads += 1
                    continue
                remaining_seconds = int(max(0.0, deadline - time.monotonic()))
                if remaining_seconds < 5:
                    if future.set_running_or_notify_cancel():
                        future.set_exception(
                            LiteratureBrowserTimeoutError(
                                "browser read expired before it reached the worker"
                            )
                        )
                        with self._state_lock:
                            self._expired_queued_reads += 1
                    else:
                        with self._state_lock:
                            self._cancelled_queued_reads += 1
                    continue
                if not future.set_running_or_notify_cancel():
                    with self._state_lock:
                        self._cancelled_queued_reads += 1
                    continue
                active_budget = min(timeout_seconds, remaining_seconds)
                with self._state_lock:
                    self._active_read_started_at = time.time()
                    self._active_read_started_monotonic = time.monotonic()
                    self._active_read_budget_seconds = active_budget
                had_priority = hasattr(
                    _FLARESOLVERR_REQUEST_CONTEXT,
                    "maintenance",
                )
                previous_priority = getattr(
                    _FLARESOLVERR_REQUEST_CONTEXT,
                    "maintenance",
                    False,
                )
                _FLARESOLVERR_REQUEST_CONTEXT.maintenance = (
                    request_priority == "maintenance"
                )
                try:
                    result = self._read_in_worker(
                        url,
                        wait_ms=wait_ms,
                        max_chars=max_chars,
                        include_figure_images=include_figure_images,
                        max_figures=max_figures,
                        figure_offset=figure_offset,
                        timeout_seconds=active_budget,
                        landing_url_hint=landing_url_hint,
                        maintenance_session_refresh=maintenance_session_refresh,
                    )
                finally:
                    if had_priority:
                        _FLARESOLVERR_REQUEST_CONTEXT.maintenance = previous_priority
                    else:
                        delattr(_FLARESOLVERR_REQUEST_CONTEXT, "maintenance")
                future.set_result(result)
            except BaseException as exc:
                if future.running():
                    future.set_exception(exc)
            finally:
                with self._state_lock:
                    self._active_read_started_at = 0.0
                    self._active_read_started_monotonic = 0.0
                    self._active_read_budget_seconds = 0
                with self._pending_lock:
                    self._pending_count = max(0, self._pending_count - 1)
                self._requests.task_done()

    def _ensure_context(self) -> Any:
        if self._context is not None:
            if not CDP_URL:
                return self._context
            try:
                if self._browser is None or not self._browser.is_connected():
                    raise LiteratureBrowserError("headed Chromium disconnected")
                # Accessing pages forces Playwright to validate the cached CDP
                # context after an externally managed Chrome process restarts.
                list(self._context.pages)
                return self._context
            except Exception:
                self._browser = None
                self._context = None
        PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        if self._playwright is None:
            from playwright.sync_api import sync_playwright

            self._playwright = sync_playwright().start()
        if CDP_URL:
            self._browser = self._playwright.chromium.connect_over_cdp(CDP_URL)
            contexts = self._browser.contexts
            if not contexts:
                raise LiteratureBrowserError(
                    "headed Chromium did not expose its persistent context"
                )
            self._context = contexts[0]
            return self._configure_context(self._context)
        launch_options: dict[str, Any] = {
            "user_data_dir": str(PROFILE_DIR),
            "executable_path": CHROMIUM_PATH,
            "headless": HEADLESS,
            # Publisher SI links can be true browser downloads. They are read
            # from Playwright's temporary path, validated, and deleted by the
            # bounded download helper rather than persisted in the profile.
            "accept_downloads": True,
            "locale": "en-US",
            "timezone_id": "Asia/Shanghai",
            "viewport": {"width": 1440, "height": 1000},
            "args": chromium_launch_args(),
        }
        self._context = self._playwright.chromium.launch_persistent_context(
            **launch_options,
        )
        return self._configure_context(self._context)

    def read(
        self,
        url: str,
        *,
        wait_ms: int,
        max_chars: int,
        include_figure_images: bool = False,
        max_figures: int = 12,
        figure_offset: int = 0,
        timeout_seconds: int = READ_BUDGET_SECONDS,
        landing_url_hint: str = "",
        maintenance_session_refresh: bool = False,
        request_priority: str = "production",
    ) -> dict[str, Any]:
        target = validate_url(url)
        if request_priority not in {"production", "maintenance"}:
            raise LiteratureBrowserError(
                "request_priority must be production or maintenance"
            )
        validated_landing_url_hint = ""
        if landing_url_hint:
            candidate = validate_url(landing_url_hint)
            target_host = (urlparse(target).hostname or "").lower()
            candidate_host = (urlparse(candidate).hostname or "").lower()
            if not landing_hint_allowed_for_target(target_host, candidate_host):
                raise LiteratureBrowserError(
                    "landing_url_hint must use the literature target host"
                )
            validated_landing_url_hint = candidate
        request_budget = max(
            45,
            min(int(timeout_seconds), READ_BUDGET_SECONDS),
        )
        future: Future[dict[str, Any]] = Future()
        queue_full = False
        with self._pending_lock:
            jobs_ahead = self._pending_count
            if jobs_ahead >= MAX_PENDING_READS:
                queue_full = True
            else:
                self._pending_count += 1
        if queue_full:
            with self._state_lock:
                self._rejected_reads += 1
            raise LiteratureBrowserBusyError(
                "browser read queue is full; retry after pending reads complete"
            )
        deadline = time.monotonic() + request_budget
        self._requests.put(
            (
                target,
                wait_ms,
                max_chars,
                include_figure_images,
                max(1, min(max_figures, MAX_FIGURES_PER_READ)),
                max(0, figure_offset),
                request_budget,
                deadline,
                validated_landing_url_hint,
                maintenance_session_refresh,
                request_priority,
                future,
            )
        )
        try:
            return future.result(
                timeout=queued_read_timeout(
                    wait_ms,
                    jobs_ahead,
                    request_budget,
                )
            )
        except FutureTimeoutError as exc:
            cancelled = future.cancel()
            if not cancelled:
                with self._state_lock:
                    self._active_wait_timeouts += 1
            raise LiteratureBrowserTimeoutError(
                "browser read exceeded its total queue and execution budget"
            ) from exc

    def _read_in_worker(
        self,
        url: str,
        *,
        wait_ms: int,
        max_chars: int,
        include_figure_images: bool = False,
        max_figures: int = 12,
        figure_offset: int = 0,
        timeout_seconds: int = READ_BUDGET_SECONDS,
        landing_url_hint: str = "",
        maintenance_session_refresh: bool = False,
    ) -> dict[str, Any]:
        original_is_article_pdf_request = publisher_pdf_request_url(url)
        target = canonical_navigation_target(url)
        deadline = time.monotonic() + max(
            5,
            min(int(timeout_seconds), READ_BUDGET_SECONDS),
        )
        target_is_supplementary_pdf = is_supplementary_pdf_url(target) or (
            elsevier_api_only_target(url, target)
            and elsevier_object_ref_from_url(target).startswith("mmc")
            and urlparse(target).path.lower().endswith(".pdf")
        )
        pnas_doi = pnas_article_doi(url) or pnas_article_doi(target)
        if (
            pnas_doi
            and not target_is_supplementary_pdf
            and not original_is_article_pdf_request
        ):
            try:
                return extract_pnas_article_via_europe_pmc(
                    f"https://doi.org/{pnas_doi}",
                    max_chars,
                    timeout_ms=min(
                        90000,
                        int(max(5.0, deadline - time.monotonic()) * 1000),
                    ),
                )
            except LiteratureBrowserError:
                # Preserve the publisher browser path when the exact open record
                # is absent or temporarily unavailable.
                pass
        rsc_article_source = ""
        if RSC_OPEN_REPOSITORY_FIRST:
            for candidate in (url, target):
                if rsc_article_code_from_url(candidate):
                    rsc_article_source = candidate
                    break
        if (
            rsc_article_source
            and not target_is_supplementary_pdf
            and not original_is_article_pdf_request
        ):
            try:
                return extract_rsc_article_via_europe_pmc(
                    rsc_article_source,
                    max_chars,
                    timeout_ms=min(
                        90000,
                        int(max(5.0, deadline - time.monotonic()) * 1000),
                    ),
                    include_figure_images=include_figure_images,
                    max_figures=max_figures,
                    figure_offset=figure_offset,
                )
            except LiteratureBrowserError:
                # Preserve the RSC/FlareSolverr path when there is no unique,
                # DOI-verified open repository record.
                pass
        if elsevier_api_only_target(url, target):
            return elsevier_api_only_result(
                url,
                target,
                max_chars=max_chars,
                include_figure_images=include_figure_images,
                max_figures=max_figures,
                figure_offset=figure_offset,
                target_is_supplementary_pdf=target_is_supplementary_pdf,
                target_is_article_pdf_request=(
                    original_is_article_pdf_request
                    and not target_is_supplementary_pdf
                ),
            )
        with self._lock:
            context = self._ensure_context()
            try:
                page = self._acquire_task_page(context)
            except Exception as exc:
                message = str(exc).casefold()
                cdp_context_closed = CDP_URL and any(
                    marker in message
                    for marker in (
                        "browser has been closed",
                        "context or browser has been closed",
                        "target page, context or browser has been closed",
                    )
                )
                if not cdp_context_closed:
                    raise
                self._browser = None
                self._context = None
                context = self._ensure_context()
                page = self._acquire_task_page(context)
            try:
                supplementary_fast_error = ""
                if target_is_supplementary_pdf:
                    try:
                        remaining_ms = int(
                            max(0.0, deadline - time.monotonic()) * 1000
                        )
                        if remaining_ms < 5000:
                            raise LiteratureBrowserError(
                                "supplementary PDF fetch reached the request time budget"
                            )
                        effective_landing_url_hint = self._publisher_landing_urls.get(
                            urlparse(target).hostname or "",
                            landing_url_hint,
                        ) or landing_url_hint
                        render_page_index = (
                            figure_offset if include_figure_images else None
                        )
                        pnas_doi, _pnas_filename = pnas_supplement_identity(target)
                        if pnas_doi:
                            extracted = extract_pnas_supplement_pdf_via_europe_pmc(
                                target,
                                max_chars,
                                timeout_ms=min(PDF_FETCH_TIMEOUT_MS, remaining_ms),
                                render_page_index=render_page_index,
                            )
                        else:
                            extracted = extract_governed_supplement_fallback(
                                target,
                                max_chars,
                                timeout_ms=min(PDF_FETCH_TIMEOUT_MS, remaining_ms),
                                render_page_index=render_page_index,
                            )
                        if extracted is None:
                            if rsc_supplement_doi(target):
                                extracted = extract_rsc_supplement_pdf_from_profile(
                                    context,
                                    target,
                                    max_chars,
                                    timeout_ms=min(PDF_FETCH_TIMEOUT_MS, remaining_ms),
                                    landing_url_hint=effective_landing_url_hint,
                                    page=page,
                                    render_page_index=render_page_index,
                                )
                            else:
                                extracted = extract_supplementary_pdf_from_origin(
                                    page,
                                    target,
                                    max_chars,
                                    timeout_ms=min(PDF_FETCH_TIMEOUT_MS, remaining_ms),
                                    landing_url_hint=effective_landing_url_hint,
                                    render_page_index=render_page_index,
                                )
                        return supplementary_pdf_result(target, extracted)
                    except SameSessionDownloadError:
                        raise
                    except Exception as exc:
                        supplementary_fast_error = (
                            f"{type(exc).__name__}: {exc}"[:400]
                        )
                        if (
                            rsc_supplement_doi(target)
                            or acs_supplement_identity(target)[0]
                        ):
                            raise
                try:
                    response = goto_with_dns_retry(
                        page,
                        target,
                        deadline=deadline,
                    )
                except Exception as exc:
                    if (
                        target_is_supplementary_pdf
                        and "download is starting" in str(exc).lower()
                    ):
                        raise LiteratureBrowserError(
                            "direct supplementary PDF fetch failed before "
                            f"browser download: {supplementary_fast_error}"
                        ) from exc
                    raise
                try:
                    page.wait_for_load_state(
                        "domcontentloaded",
                        timeout=DOM_READY_TIMEOUT_MS,
                    )
                except Exception:
                    pass
                try:
                    page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
                if wait_ms:
                    page.wait_for_timeout(wait_ms)
                self._trim_task_pages(context, page)

                challenge_bypass: dict[str, Any] = {
                    "attempted": False,
                    "success": False,
                }
                profile_warm: dict[str, Any] = {"attempted": False}
                initial_sample = settle_publisher_shell(
                    page,
                    page_access_sample(page, body_timeout_ms=10000),
                )
                if not target_is_supplementary_pdf:
                    initial_sample, profile_warm = warm_rsc_article_profile(
                        page,
                        initial_sample,
                        deadline=deadline,
                    )
                initial_title = str(initial_sample.get("title") or "")
                initial_url = str(initial_sample.get("url") or page.url)
                initial_text = str(initial_sample.get("text") or "")
                initial_state = str(initial_sample.get("state") or "content")
                initial_full_text_dom = bool(initial_sample.get("full_text_dom"))
                if should_try_flaresolverr(
                    initial_url,
                    initial_state,
                    initial_text,
                    full_text_dom=initial_full_text_dom,
                ) or (
                    target_is_supplementary_pdf
                    and should_retry_supplement_with_flaresolverr(
                        target,
                        supplementary_fast_error,
                    )
                ):
                    challenge_bypass["attempted"] = True
                    try:
                        solver_url = (
                            supplementary_pdf_landing_url(target)
                            if target_is_supplementary_pdf
                            else initial_url
                        )
                        solved = request_publisher_flaresolverr(
                            solver_url,
                            allow_retry=True,
                            include_figure_images=False,
                            max_timeout_ms=max(
                                10000,
                                int((deadline - time.monotonic() - 15.0) * 1000),
                            ),
                        )
                        solver_publisher = flaresolverr_publisher_slug(solver_url)
                        apply_flaresolverr_handoff(
                            page,
                            solved,
                            publisher=solver_publisher,
                        )
                        handoff_url = validate_url(
                            str(solved.get("final_url") or solver_url)
                        )
                        response = goto_with_dns_retry(
                            page,
                            handoff_url,
                            deadline=deadline,
                        )
                        try:
                            page.wait_for_load_state(
                                "domcontentloaded",
                                timeout=DOM_READY_TIMEOUT_MS,
                            )
                        except Exception:
                            pass
                        try:
                            page.wait_for_load_state("networkidle", timeout=10000)
                        except Exception:
                            pass
                        if wait_ms:
                            page.wait_for_timeout(wait_ms)
                        handoff_sample = settle_publisher_shell(
                            page,
                            page_access_sample(page, body_timeout_ms=10000),
                        )
                        handoff_content = bool(
                            str(handoff_sample.get("state") or "") == "content"
                            and (
                                target_is_supplementary_pdf
                                or bool(handoff_sample.get("full_text_dom"))
                                or full_text_visible(
                                    str(handoff_sample.get("text") or "")
                                )
                            )
                        )
                        if not handoff_content:
                            raise LiteratureBrowserError(
                                "Playwright did not verify publisher content after "
                                "the solver profile handoff"
                            )
                        challenge_bypass.update(
                            {
                                "success": True,
                                "solver_status": solved["status"],
                                "profile_handoff": True,
                                "playwright_verified": True,
                            }
                        )
                    except Exception as exc:
                        challenge_bypass["message"] = str(exc)[:400]

                title = safe_page_title(page)
                final_url = page.url
                final_host = urlparse(final_url).hostname or ""
                if not host_allowed(final_host):
                    raise LiteratureBrowserError(
                        "publisher redirected outside the literature allowlist"
                    )
                try:
                    html_text = page.locator("body").inner_text(timeout=10000)
                except Exception:
                    html_text = ""
                html_text, html_truncated = trim_text(html_text, max_chars)
                metadata = page.evaluate(
                    """() => {
                      const values = {};
                      for (const node of document.querySelectorAll('meta[name], meta[property]')) {
                        const key = node.getAttribute('name') || node.getAttribute('property');
                        const content = node.getAttribute('content');
                        if (!key || !content) continue;
                        if (!values[key]) values[key] = [];
                        if (values[key].length < 20) values[key].push(content);
                      }
                      return values;
                    }"""
                )
                figure_extraction = extract_figure_manifest(
                    page,
                    figure_offset=figure_offset,
                    max_figures=max_figures,
                )
                figures = figure_extraction["items"]
                if include_figure_images:
                    attach_figure_images(page, figures)
                pdf_links = page.evaluate(
                    """() => {
                      const items = [];
                      for (const node of document.querySelectorAll('meta[name], meta[property]')) {
                        const key = (
                          node.getAttribute('name') ||
                          node.getAttribute('property') ||
                          ''
                        ).toLowerCase();
                        if (!key.includes('citation_pdf_url')) continue;
                        items.push({
                          href: node.getAttribute('content') || '',
                          text: key,
                          source: 'meta'
                        });
                      }
                      for (const node of document.querySelectorAll(
                        'a[href], iframe[src], embed[src], object[data], '
                        + '[data-url], [data-href], [data-download-url], [data-file-url]'
                      )) {
                        const href = (
                          node.href ||
                          node.src ||
                          node.data ||
                          node.getAttribute('href') ||
                          node.getAttribute('src') ||
                          node.getAttribute('data') ||
                          node.getAttribute('data-url') ||
                          node.getAttribute('data-href') ||
                          node.getAttribute('data-download-url') ||
                          node.getAttribute('data-file-url') ||
                          ''
                        );
                        const text = (
                          node.innerText ||
                          node.getAttribute('aria-label') ||
                          node.getAttribute('title') ||
                          ''
                        ).trim();
                        if (!/pdf|supporting information|supplement/i.test(
                          href + ' ' + text
                        )) continue;
                        items.push({
                          href,
                          text,
                          source: node.tagName.toLowerCase()
                        });
                      }
                      return items.slice(0, 60);
                    }"""
                )
                page_markup = page.content()
                pdf_links.extend(
                    extract_sciencedirect_pdf_links(
                        page_markup,
                        final_url,
                    )
                )
                pdf_links.extend(
                    extract_supplementary_artifact_links(
                        page_markup,
                        final_url,
                    )
                )
                pdf_links = deduplicate_resource_links(pdf_links)
                full_text_dom = bool(
                    page.evaluate(
                        """() => Array.from(document.querySelectorAll(
                          '.widget-ArticleFulltext, .article-body, .article__body, '
                          + '.article-section__content, .Body, #body, '
                          + '.c-article-body, #main-content, main article, '
                          + '[data-aa-name="articleBody"]'
                        )).some((node) => (node.innerText || '').trim().length >= 8000)"""
                    )
                )
                state = classify_page(final_url, title, html_text)
                text = html_text
                truncated = html_truncated
                text_source = "html"
                html_extraction: dict[str, Any] = {
                    "attempted": False,
                    "success": False,
                    "candidates": [],
                }
                if (
                    state == "content"
                    and host_in_domains(
                        final_host,
                        (
                            "wiley.com",
                            "springer.com",
                            "springernature.com",
                            "nature.com",
                            "rsc.org",
                            "acs.org",
                        ),
                    )
                ):
                    try:
                        html_extraction = extract_structured_html_article(
                            page.content(),
                            final_url,
                            max_chars,
                        )
                        if html_extraction.get("success"):
                            text = str(html_extraction.pop("text"))
                            truncated = bool(
                                max_chars > 0 and len(text) >= max_chars
                            )
                            text_source = "publisher_html"
                            full_text_dom = True
                    except Exception as exc:
                        html_extraction["message"] = (
                            f"structured HTML extraction failed: {exc}"[:400]
                        )
                if (
                    not include_figure_images
                    and not target_is_supplementary_pdf
                    and not publisher_pdf_request_url(target)
                    and host_in_domains(
                        final_host,
                        ("sciencedirect.com", "elsevier.com"),
                    )
                    and not full_text_dom
                    and not full_text_visible(text)
                ):
                    elsevier_api_extraction = fetch_elsevier_article_api(
                        url,
                        final_url,
                        metadata,
                        max_chars,
                    )
                    if elsevier_api_extraction.get("success"):
                        text = str(elsevier_api_extraction.get("text") or "")
                        truncated = bool(
                            elsevier_api_extraction.get("text_truncated")
                        )
                        text_source = "elsevier_api_xml"
                        full_text_dom = True
                        state = "content"
                        title = (
                            str(elsevier_api_extraction.get("title") or "").strip()
                            or title
                        )
                        html_extraction = {
                            "attempted": True,
                            "success": True,
                            "selector": "elsevier_article_retrieval_api",
                            "text_chars": int(
                                elsevier_api_extraction.get("text_chars")
                                or len(text)
                            ),
                        }
                if unresolved_rsc_solver(
                    final_url,
                    challenge_bypass,
                    text,
                    full_text_dom=full_text_dom,
                    pdf_links=pdf_links,
                ):
                    state = "challenge"
                    challenge_bypass["success"] = False
                    challenge_bypass.setdefault(
                        "message",
                        "RSC challenge solver did not yield full article text",
                    )
                pdf_extraction: dict[str, Any] = {
                    "attempted": False,
                    "success": False,
                    "attempts": [],
                }
                response_headers = response.headers if response is not None else {}
                response_content_type = str(
                    response_headers.get("content-type") or ""
                ).lower()
                result_status = response.status if response is not None else None
                direct_pdf = (
                    response is not None
                    and (
                        (
                            response.status < 400
                            and (
                                "application/pdf" in response_content_type
                                or urlparse(final_url).path.lower().endswith(".pdf")
                            )
                        )
                        or urlparse(target).path.lower().endswith(".pdf")
                        or target_is_supplementary_pdf
                    )
                )
                if direct_pdf and (
                    state == "content" or target_is_supplementary_pdf
                ):
                    pdf_extraction["attempted"] = True
                    try:
                        remaining_ms = int(
                            max(0.0, deadline - time.monotonic()) * 1000
                        )
                        if remaining_ms < 5000:
                            raise LiteratureBrowserError(
                                "direct PDF fallback reached the request time budget"
                            )
                        extracted = extract_direct_pdf_response(
                            page,
                            response,
                            target,
                            final_url,
                            max_chars,
                            timeout_ms=min(
                                PDF_FETCH_TIMEOUT_MS,
                                remaining_ms,
                            ),
                            render_page_index=(
                                figure_offset if include_figure_images else None
                            ),
                        )
                        rendered_figures = extracted.pop("figures", None)
                        rendered_figure_extraction = extracted.pop(
                            "figure_extraction",
                            None,
                        )
                        if rendered_figures is not None:
                            figures = rendered_figures
                        if rendered_figure_extraction is not None:
                            figure_extraction = rendered_figure_extraction
                        text = str(extracted.pop("text"))
                        truncated = bool(extracted.pop("text_truncated"))
                        result_status = int(extracted.get("status") or 200)
                        final_url = str(extracted.get("source_url") or final_url)
                        state = "content"
                        text_source = "pdf"
                        pdf_extraction.update(
                            {
                                "success": True,
                                "text_chars": len(text),
                                **extracted,
                            }
                        )
                    except Exception as exc:
                        pdf_extraction["message"] = (
                            f"direct PDF extraction failed: {exc}"[:400]
                        )
                if (
                    state == "content"
                    and text_source in {"html", "publisher_html"}
                    and not full_text_dom
                    and not full_text_visible(text)
                ):
                    candidates = select_pdf_candidates(pdf_links)
                    maximum_candidates = MAX_PDF_CANDIDATES
                    if candidates:
                        pdf_extraction["attempted"] = True
                    for candidate in candidates:
                        remaining_ms = int(
                            max(0.0, deadline - time.monotonic()) * 1000
                        )
                        if remaining_ms < 5000:
                            pdf_extraction["message"] = (
                                "PDF fallback stopped at the per-article time budget"
                            )
                            break
                        parsed_candidate = urlparse(candidate)
                        attempt: dict[str, Any] = {
                            "source_host": parsed_candidate.hostname or "",
                            "source_path": parsed_candidate.path,
                        }
                        pdf_extraction["attempts"].append(attempt)
                        try:
                            candidate_host = (
                                urlparse(candidate).hostname or ""
                            )
                            if browser_pdf_fetch_host_allowed(candidate_host):
                                (
                                    raw_pdf,
                                    pdf_content_type,
                                    pdf_final_url,
                                    pdf_status,
                                ) = fetch_pdf_in_browser(
                                    page,
                                    candidate,
                                    timeout_ms=min(
                                        PDF_FETCH_TIMEOUT_MS,
                                        remaining_ms,
                                    ),
                                )
                                pdf_strategy = "browser_fetch"
                            else:
                                pdf_response = context.request.get(
                                    candidate,
                                    headers={
                                        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.1",
                                        "Referer": final_url,
                                    },
                                    fail_on_status_code=False,
                                    max_redirects=5,
                                    timeout=min(
                                        NAVIGATION_TIMEOUT_MS,
                                        remaining_ms,
                                    ),
                                )
                                pdf_final_url = pdf_response.url
                                pdf_host = (
                                    urlparse(pdf_final_url).hostname or ""
                                )
                                if not host_allowed(pdf_host):
                                    raise LiteratureBrowserError(
                                        "PDF redirected outside the literature allowlist"
                                    )
                                pdf_status = pdf_response.status
                                pdf_content_type = str(
                                    pdf_response.headers.get("content-type")
                                    or ""
                                ).lower()
                                raw_pdf = pdf_response.body()
                                pdf_strategy = "request_context"
                            attempt.update(
                                {
                                    "strategy": pdf_strategy,
                                    "status": pdf_status,
                                    "content_type": pdf_content_type[:120],
                                    "bytes": len(raw_pdf),
                                }
                            )
                            if pdf_status >= 400:
                                raise LiteratureBrowserError(
                                    f"publisher PDF returned HTTP {pdf_status}"
                                )
                            if (
                                "application/pdf" not in pdf_content_type
                                and not raw_pdf.startswith(b"%PDF")
                            ):
                                viewer_candidates = extract_pdf_urls_from_viewer(
                                    raw_pdf,
                                    pdf_final_url,
                                )
                                attempt["viewer_candidates"] = [
                                    urlparse(value).path
                                    for value in viewer_candidates
                                ]
                                for value in viewer_candidates:
                                    if (
                                        value not in candidates
                                        and len(candidates) < maximum_candidates
                                    ):
                                        candidates.append(value)
                                raise LiteratureBrowserError(
                                    "publisher PDF link did not return a PDF"
                                )
                            pdf_text, pdf_truncated, pdf_pages = extract_pdf_text(
                                raw_pdf,
                                max_chars,
                            )
                            usable_article = pdf_is_usable_article(
                                pdf_text,
                                html_text,
                                pages=pdf_pages,
                                source_url=pdf_final_url,
                                browser_originated=(
                                    pdf_strategy == "browser_fetch"
                                ),
                            )
                            attempt.update(
                                {
                                    "pages": pdf_pages,
                                    "text_chars": len(pdf_text),
                                    "structured_full_text": full_text_visible(
                                        pdf_text
                                    ),
                                    "usable_article": usable_article,
                                }
                            )
                            if not usable_article:
                                raise LiteratureBrowserError(
                                    "publisher PDF did not add extractable article text"
                                )
                            text = pdf_text
                            truncated = pdf_truncated
                            text_source = "pdf"
                            pdf_extraction.update(
                                {
                                    "success": True,
                                    "source_url": pdf_final_url,
                                    "pages": pdf_pages,
                                    "text_chars": len(pdf_text),
                                    "strategy": pdf_strategy,
                                }
                            )
                            attempt["accepted"] = True
                            break
                        except Exception as exc:
                            attempt["error_type"] = type(exc).__name__
                            pdf_extraction["message"] = (
                                f"PDF fallback failed: {exc}"[:400]
                            )
                if target_is_supplementary_pdf and not pdf_extraction.get("success"):
                    details = []
                    for value in (
                        supplementary_fast_error,
                        str(pdf_extraction.get("message") or "").strip(),
                    ):
                        if value and value not in details:
                            details.append(value)
                    detail = "; ".join(details) or (
                        "publisher supplementary PDF did not return a validated PDF"
                    )
                    raise LiteratureBrowserError(
                        "publisher supplementary PDF fetch failed: " + detail[:500]
                    )
                signal_text = f"{html_text}\n{text}"
                signals = access_signals(signal_text) if state == "content" else []
                if text_source == "elsevier_api_xml":
                    signals = sorted(set(signals + ["Institutional access"]))
                institution_markers = [
                    marker
                    for marker in (
                        "Xiamen University",
                        "厦门大学",
                        "Access through your institution",
                        "Institutional access",
                    )
                    if marker.lower() in signal_text.lower()
                ]
                if text_source == "elsevier_api_xml":
                    institution_markers.append("Elsevier Article Retrieval API FULL")
                references = extract_numbered_references(text)
                result = {
                    "success": page_read_succeeded(state),
                    "url": target,
                    "final_url": final_url,
                    "status": result_status,
                    "page_state": state,
                    "title": title,
                    "text": text,
                    "text_truncated": truncated,
                    "text_source": text_source,
                    "html_text_chars": len(html_text),
                    "full_text_dom_visible": full_text_dom,
                    "challenge_bypass": challenge_bypass,
                    "profile_warm": profile_warm,
                    "html_extraction": html_extraction,
                    "pdf_extraction": pdf_extraction,
                    "metadata": metadata,
                    "figures": figures,
                    "figure_extraction": {
                        key: figure_extraction[key]
                        for key in ("total", "offset", "returned", "has_more")
                        if key in figure_extraction
                    },
                    "pdf_links": pdf_links,
                    "references": references,
                    "access_state": classify_access(
                        state,
                        text,
                        signals,
                        full_text_dom=(
                            full_text_dom
                            or (
                                text_source == "pdf"
                                and bool(pdf_extraction.get("success"))
                            )
                        ),
                    ),
                    "access_signals": signals,
                    "institution_markers": institution_markers,
                }
                if state == "content" and not target_is_supplementary_pdf:
                    self._publisher_landing_urls[final_host] = final_url
                return result
            finally:
                self._reset_after_read(context, page)


BROWSER = BrowserManager()


def browser_lifecycle_ready(lifecycle: dict[str, Any]) -> bool:
    return bool(lifecycle.get("worker_alive")) and not bool(lifecycle.get("stalled"))


def utf8_safe_json(value: Any) -> Any:
    if isinstance(value, str):
        if not any(0xD800 <= ord(char) <= 0xDFFF for char in value):
            return value
        return "".join(
            "\ufffd" if 0xD800 <= ord(char) <= 0xDFFF else char for char in value
        )
    if isinstance(value, dict):
        return {utf8_safe_json(key): utf8_safe_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [utf8_safe_json(item) for item in value]
    return value


def json_response(
    handler: BaseHTTPRequestHandler,
    status: int,
    payload: dict[str, Any],
) -> None:
    raw = json.dumps(utf8_safe_json(payload), ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        if urlparse(self.path).path == "/ready":
            lifecycle = BROWSER.status()
            ready = browser_lifecycle_ready(lifecycle)
            json_response(
                self,
                200 if ready else 503,
                {
                    "ready": ready,
                    "headless": HEADLESS,
                    "browser_mode": "cdp-headed" if CDP_URL else "playwright",
                    "profile": "dedicated",
                    "allowed_domain_count": len(ALLOWED_DOMAINS),
                    "lifecycle": lifecycle,
                },
            )
            return
        json_response(self, 404, {"success": False, "message": "not found"})

    def do_POST(self) -> None:
        if urlparse(self.path).path != "/v1/literature/read":
            json_response(self, 404, {"success": False, "message": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0 or length > 65536:
                raise LiteratureBrowserError("invalid request body size")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(payload, dict):
                raise LiteratureBrowserError("request body must be an object")
            wait_ms = max(
                0,
                min(int(payload.get("wait_ms") or DEFAULT_WAIT_MS), MAX_WAIT_MS),
            )
            raw_max_chars = payload.get("max_chars", DEFAULT_MAX_CHARS)
            max_chars = int(raw_max_chars)
            if max_chars != 0 and max_chars < 1000:
                raise LiteratureBrowserError(
                    "max_chars must be 0 for full text or at least 1000"
                )
            include_figure_images = payload.get("include_figure_images", False)
            if not isinstance(include_figure_images, bool):
                raise LiteratureBrowserError(
                    "include_figure_images must be a boolean"
                )
            maintenance_session_refresh = payload.get(
                "maintenance_session_refresh",
                False,
            )
            if not isinstance(maintenance_session_refresh, bool):
                raise LiteratureBrowserError(
                    "maintenance_session_refresh must be a boolean"
                )
            request_priority = str(
                payload.get("request_priority") or "production"
            ).strip().lower()
            if request_priority not in {"production", "maintenance"}:
                raise LiteratureBrowserError(
                    "request_priority must be production or maintenance"
                )
            max_figures = max(
                1,
                min(
                    int(payload.get("max_figures") or 12),
                    MAX_FIGURES_PER_READ,
                ),
            )
            figure_offset = int(payload.get("figure_offset") or 0)
            if not 0 <= figure_offset <= 1000:
                raise LiteratureBrowserError(
                    "figure_offset must be between 0 and 1000"
                )
            result = BROWSER.read(
                str(payload.get("url") or ""),
                wait_ms=wait_ms,
                max_chars=max_chars,
                include_figure_images=include_figure_images,
                max_figures=max_figures,
                figure_offset=figure_offset,
                landing_url_hint=str(payload.get("landing_url_hint") or ""),
                maintenance_session_refresh=maintenance_session_refresh,
                request_priority=request_priority,
                timeout_seconds=max(
                    45,
                    min(
                        int(payload.get("timeout_seconds") or READ_BUDGET_SECONDS),
                        READ_BUDGET_SECONDS,
                    ),
                ),
            )
            json_response(self, 200, result)
        except LiteratureBrowserBusyError as exc:
            json_response(self, 429, {"success": False, "message": str(exc)})
        except LiteratureBrowserTimeoutError as exc:
            json_response(self, 504, {"success": False, "message": str(exc)})
        except LiteratureBrowserError as exc:
            json_response(self, 403, {"success": False, "message": str(exc)})
        except Exception as exc:
            json_response(
                self,
                500,
                {"success": False, "message": f"browser read failed: {exc}"[:1600]},
            )


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(
        "[literature-browser] "
        f"listen={HOST}:{PORT} headless={HEADLESS} "
        f"allowed_domains={len(ALLOWED_DOMAINS)}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
