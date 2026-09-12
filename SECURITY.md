# Security Model

The package has three boundaries:

1. `config/publishers.json` is the human-reviewed network allowlist.
2. The browser API is bound to loopback on the host and is reachable internally
   by the MCP and monitor containers.
3. Browser session material remains in a Docker named volume and is never part
   of MCP results, logs, reports, archives, or Git.

FlareSolverr is enabled by the reviewed policy. It is internal-only, has no host
port, and is called only after the normal browser identifies a challenge or an
approved publisher shell without usable article text. Solver HTML is never delivered as
article evidence. The engine and solver use the same pinned Chromium build. Allowlisted
cookies and the exact solver user agent may be applied only in memory to the same
`PROFILE_ID` and publisher, under an exclusive pool lease. That Playwright profile must
then navigate to the publisher again and verify content before it extracts article
text, figures, references, PDFs, or SI. Session state is never exported, logged, written
as a separate artifact, copied to another profile, or replayed by another HTTP client.
The named solver browser is destroyed immediately after its handoff state is captured.
Failure is terminal: the
request is not retried in a second solver browser or reduced to an unverified image URL.

The review records a SHA-256 over the governed publisher, solver, and session
handoff sections. Any policy edit invalidates startup until a human explicitly
approves the new digest.

In Clash, VPN, or system-proxy environments, campus and publisher domains
should route as direct traffic. `config/clash-direct-rules.example.yaml`
contains a reviewed starter snippet, and the Compose services pass
`LITERATURE_NO_PROXY` into `NO_PROXY`/`no_proxy` for proxy-aware clients. A
Clash TUN/global proxy still requires Clash-side `DIRECT` rules because
process-level `NO_PROXY` cannot override packet interception.

The MCP server returns selected fields rather than forwarding arbitrary browser
JSON. Publisher metadata keys that resemble cookies, tokens, authorization, or
user-agent state are removed. Health reports never contain article text.

Do not expose browser ports 9020/9030/19020 to a LAN or the internet. Remote
clients use the authenticated Research Gateway on `/mcp/research` or
`/api/research/v1/`; the gateway is reverse-proxied through TLS and keeps its
own port 9040 on host loopback.

Research Gateway API keys are independent per client. The registry stores only
key digests. Never commit the registry or plaintext token output. Student keys
should receive publisher, read, and health scopes only; active probes and
session refresh are separate operational privileges.
