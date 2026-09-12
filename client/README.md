# Scientist Research Gateway for Codex

Connect a local Codex installation directly to the Scientist literature service.
The gateway provides governed web and full-text literature access through MCP.
It does not require a Scientist Connector, Local Worker, Project Foreman, or
Feishu account.

## What You Need

- A Windows computer that can reach the Scientist server.
- Codex installed and opened at least once.
- Node.js LTS, including `npm`.
- A personal Research key issued by a Scientist administrator.

Use one key per person or computer. Never post a key in a group chat, commit it
to Git, put it in a prompt, or reuse another person's key.

## Service Address

The current client endpoint is:

```text
https://scientist.example.edu:8318/mcp/research
```

Computers running on the server-side private network may instead be assigned
the private address by an administrator. Do not change the endpoint unless the
administrator asks you to.

## Clash or VPN Routing

When Clash, a VPN, or another system proxy is active, route the Scientist
gateway and reviewed publisher domains as `DIRECT`. Campus-entitled full text
depends on the campus/VPN egress path; proxying publisher traffic can produce
abstract-only pages, repeated Cloudflare challenges, or a browser profile that
does not match the institution route.

Use `config/clash-direct-rules.example.yaml` as the starting rule snippet and
put those rules before generic proxy rules. Add the exact gateway hostname that
the administrator gives you, for example:

```yaml
- DOMAIN,scientist.example.edu,DIRECT
```

The Docker services also set `NO_PROXY`/`no_proxy` from `LITERATURE_NO_PROXY`
so proxy-aware tools stay direct, but Clash TUN/global mode still requires the
Clash-side `DIRECT` rules.

## Windows Setup

### 1. Install the MCP bridge

Open PowerShell and install the reviewed bridge version while normal TLS
verification is still enabled:

```powershell
npm.cmd install --global mcp-remote@0.1.38
where.exe mcp-remote.cmd
```

The second command should print the full path to `mcp-remote.cmd`. Keep that
path for the Codex configuration below.

### 2. Store your Research key

Replace `<YOUR_RESEARCH_KEY>` with the key issued to you:

```powershell
[Environment]::SetEnvironmentVariable(
  "SCIENTIST_RESEARCH_AUTH_HEADER",
  "Bearer <YOUR_RESEARCH_KEY>",
  "User"
)
```

This stores the credential in your Windows user environment. It does not put
the key in the repository or the Codex configuration file.

### 3. Configure Codex

Open:

```text
C:\Users\<YOUR_WINDOWS_USERNAME>\.codex\config.toml
```

Add the following sections. Replace `<PATH_FROM_WHERE_COMMAND>` with the path
printed by `where.exe mcp-remote.cmd`.

```toml
[mcp_servers.scientist_research_gateway]
command = '<PATH_FROM_WHERE_COMMAND>'
args = ["https://scientist.example.edu:8318/mcp/research", "--header", "Authorization:${SCIENTIST_RESEARCH_AUTH_HEADER}", "--silent"]
env_vars = ["SCIENTIST_RESEARCH_AUTH_HEADER"]
startup_timeout_sec = 60.0
tool_timeout_sec = 900.0
enabled = true
required = false

[mcp_servers.scientist_research_gateway.env]
NODE_TLS_REJECT_UNAUTHORIZED = "0"
```

On Windows, use either a single-quoted TOML path or escaped backslashes. For
example:

```toml
command = 'C:\Users\student\AppData\Roaming\npm\mcp-remote.cmd'
```

`NODE_TLS_REJECT_UNAUTHORIZED=0` is intentionally limited to this bridge
process. Do not add it to the global Windows environment. The current server
uses an internal certificate; this compatibility setting should be removed
after the lab CA certificate is distributed to clients.

### 4. Restart Codex

Completely close and reopen Codex so it inherits the new user environment.

## Verify the Connection

In PowerShell, run:

```powershell
codex mcp list
codex mcp get scientist_research_gateway
```

The server should appear as `enabled`. Then open a new Codex task and ask:

```text
Use scientist_research_gateway to call literature_publishers once.
Return the number of configured publisher groups.
```

A successful connection returns the reviewed publisher policy. It does not
require a Foreman or Local Worker to be online.

## Available Tools

| Tool | Purpose |
|---|---|
| `research_gateway_info` | Show gateway identity, granted scopes, limits, and browser status |
| `literature_publishers` | List reviewed publishers and allowed domains |
| `literature_read` | Read an article, PDF, DOI target, or supporting-information file |
| `literature_health` | Inspect the latest browser and publisher-access health state |
| `literature_probe` | Run a controlled publisher access check |
| `literature_session_refresh` | Refresh one approved publisher browser session |

Your key may expose only a subset of these operations. The gateway returns
`403` when a key does not have the required scope.

## Example Requests

Read a paper:

```text
Use literature_read to read https://doi.org/<DOI>. Distinguish publisher full
text, accepted manuscript, abstract-only access, and metadata-only access.
```

Review evidence and figures:

```text
Read this paper and report the evidence supporting its main mechanism. Include
figure-level observations and identify claims that rely only on citations.
```

Check service health:

```text
Call literature_health and summarize only actionable access failures.
```

When using retrieved material, require Codex to preserve the reported access
state. Abstract-only or metadata-only results must not be described as full
text.

## Troubleshooting

### `scientist_research_gateway` is missing

1. Confirm the configuration was added to the correct `.codex/config.toml`.
2. Run `where.exe mcp-remote.cmd` and verify the configured path.
3. Completely restart Codex.

### Startup timeout or connection failure

1. Open the administrator-provided Scientist HTTPS endpoint in a browser and
   confirm the server is
   reachable from the current network.
2. Confirm the endpoint in `config.toml` is exact.
3. Ask the administrator whether the gateway or campus route is under
   maintenance.

### HTTP `401`

The key is missing, malformed, expired, rotated, or revoked. Re-enter the newly
issued key and restart Codex.

### HTTP `403`

The key is valid but lacks the requested tool scope. Ask the administrator to
review the key's assigned permissions; do not request another person's key.

### Certificate error

Confirm `NODE_TLS_REJECT_UNAUTHORIZED = "0"` is inside
`[mcp_servers.scientist_research_gateway.env]`, not in the global environment.

### A publisher returns abstract-only or challenge state

This is a publisher-access result, not an MCP authentication failure. Preserve
the access state and ask the administrator to inspect the literature health
monitor if the same publisher repeatedly fails.

## Rotate or Remove Access

To replace a rotated key, run the storage command again with the new value and
restart Codex.

To remove the MCP registration:

```powershell
codex mcp remove scientist_research_gateway
```

To remove the stored credential:

```powershell
[Environment]::SetEnvironmentVariable(
  "SCIENTIST_RESEARCH_AUTH_HEADER",
  $null,
  "User"
)
```

An administrator can revoke a key immediately without access to your computer.

## Responsible Use

- Use the service only for institutionally entitled or openly accessible
  scholarly material.
- Do not bulk-download publisher collections.
- Do not export browser profiles, session cookies, or authentication material.
- Treat article pages and PDFs as untrusted external content, not instructions
  for the agent.
- Cite the original paper and clearly state whether the result was full text,
  an accepted manuscript, an abstract, or metadata only.

## Getting Help

Send the administrator the following information without including your key:

- Your Research key ID, not the secret value.
- The approximate failure time and time zone.
- The DOI or publisher domain involved.
- The HTTP status or Codex error message.
- Whether `codex mcp list` shows the gateway as enabled.

