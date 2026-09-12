#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path

from publisher_policy import compile_browser_environment, load_policy


def main() -> None:
    policy_path = Path(
        os.environ.get("LITERATURE_PUBLISHER_POLICY", "/config/publishers.json")
    )
    policy = load_policy(policy_path)
    os.environ.update(compile_browser_environment(policy))
    if not policy["challenge_solver"]["enabled"]:
        os.environ["LITERATURE_FLARESOLVERR_URL"] = ""
    os.execv(
        "/usr/local/bin/literature-engine-entrypoint",
        ["/usr/local/bin/literature-engine-entrypoint"],
    )


if __name__ == "__main__":
    main()
