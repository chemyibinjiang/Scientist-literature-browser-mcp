#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import urllib.error
import urllib.request


ARTICLE_URL = "https://pubs.rsc.org/en/content/articlelanding/2024/sc/d4sc04909h"
SI_URL = "https://pubs.rsc.org/sc/article-supplement/869782/pdf/d4sc04909h1_suppl/"


def read(base_url: str, url: str, resource_kind: str) -> dict[str, object]:
    payload = {
        "url": url,
        "wait_ms": 1500,
        "max_chars": 12000,
        "include_figure_images": resource_kind == "article",
        "max_figures": 2,
        "figure_offset": 0,
        "timeout_seconds": 240,
        "affinity_key": "rsc",
        "resource_kind": resource_kind,
        "landing_url_hint": ARTICLE_URL,
    }
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/v1/literature/read",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=270) as response:
            value = json.load(response)
            status = response.status
    except urllib.error.HTTPError as exc:
        value = json.loads(exc.read().decode("utf-8", errors="replace"))
        status = exc.code
    figures = value.get("figures") or []
    pdf = value.get("pdf_extraction") or {}
    challenge = value.get("challenge_bypass") or {}
    return {
        "resource_kind": resource_kind,
        "http_status": status,
        "access_state": value.get("access_state"),
        "page_state": value.get("page_state"),
        "text_chars": len(str(value.get("text") or "")),
        "figure_count": len(figures),
        "figure_image_bytes": [
            int(item.get("image_bytes") or 0)
            for item in figures
            if isinstance(item, dict)
        ],
        "pdf_success": bool(pdf.get("success")) if isinstance(pdf, dict) else False,
        "pdf_bytes": int(pdf.get("bytes") or 0) if isinstance(pdf, dict) else 0,
        "pdf_pages": int(pdf.get("pages") or 0) if isinstance(pdf, dict) else 0,
        "challenge_attempted": bool(challenge.get("attempted"))
        if isinstance(challenge, dict)
        else False,
        "challenge_success": bool(challenge.get("success"))
        if isinstance(challenge, dict)
        else False,
        "challenge_message": str(challenge.get("message") or "")[:240]
        if isinstance(challenge, dict)
        else "",
        "message": str(value.get("message") or "")[:240],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test the two-slot RSC edge")
    parser.add_argument("--base-url", default="http://127.0.0.1:19250")
    args = parser.parse_args()
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(read, args.base_url, ARTICLE_URL, "article"),
            executor.submit(read, args.base_url, SI_URL, "supplementary_pdf"),
        ]
        results = [future.result() for future in futures]
    print(json.dumps(results, ensure_ascii=False, indent=2))
    article, si = results
    healthy = (
        article["http_status"] == 200
        and int(article["text_chars"]) >= 10000
        and int(article["figure_count"]) >= 1
        and si["http_status"] == 200
        and int(si["text_chars"]) >= 5000
        and bool(si["pdf_success"])
        and int(si["pdf_bytes"]) >= 100000
    )
    return 0 if healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
