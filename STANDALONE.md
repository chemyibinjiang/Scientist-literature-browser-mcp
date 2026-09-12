# Standalone Distribution

This repository can be used in two ways:

1. Connect a client to a Research Gateway already operated by the lab. Follow
   `client/README.md`; each user must receive an individual API key through the
   administrator's credential-delivery process.
2. Operate a private local browser service on a machine that has legitimate
   institutional or open-access entitlement. Follow the local setup below.

## Validate The Checkout

64-bit Python 3.11 or newer and Docker Desktop with Compose are required. A
32-bit Python installation may try to compile `cryptography` locally and is not
supported by this package.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[test]"
.\.venv\Scripts\python.exe -m pytest -q
docker compose config --quiet
```

The tests do not contact publishers, consume credentials, or create browser
profiles. They validate the package contracts, safety policy, and generic
Compose topology.

## Review Before Local Use

The standalone export deliberately marks `config/publishers.json` as requiring
review. Inspect the enabled domains and access basis, then approve that exact
policy for the machine:

```powershell
.\scripts\approve-policy.ps1 -ReviewedBy "Your name"
Copy-Item .env.example .env
```

Set local paths and any optional publisher API credentials in `.env`. Never
commit `.env`, API keys, browser profiles, cookies, health reports, or downloaded
publisher content. Start the local stack only after the policy review:

```powershell
.\scripts\start.ps1
```

## Distribution Boundary

`STANDALONE-MANIFEST.json` records the canonical source commit and SHA-256 of
every exported file. The export omits production-specific SSH, edge-routing,
systemd router configuration, recovery overlays, profiles, cookies, secrets,
and runtime reports.

No project license is granted by this package. Keep a shared repository private
unless the project owner has selected and added an appropriate source license.
