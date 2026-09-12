# Scientist Literature Browser MCP

For a collaborator who only needs to connect Codex to the deployed Research
Gateway, use the standalone [client setup guide](client/README.md). It does not
require deploying this package locally.

这是一个可独立分发的文献浏览 MCP 包。它把校园网中的真实 Chromium、
FlareSolverr 挑战恢复、全文/PDF/SI 提取、持久浏览器会话和定时健康巡检，
封装成 Codex 可以调用的一组 MCP tools。

## 组件

```text
Codex / Foreman
  -> MCP stdio server
     -> local browser API
        -> persistent Chromium profile
        -> FlareSolverr (only after an approved-domain challenge)

MCP maintenance plane
  -> bounded request/profile health canaries
  -> repair or replace a profile only after repeated real-request failure
  -> no continuous all-publisher keep-warm census
  -> sanitized health state only
```

MCP 不返回 cookie、浏览器 profile、user agent、原始 solver HTML 或 token。
FlareSolverr 只在普通浏览器明确遇到 challenge 后按需启动临时 Chrome。它只返回
经过域名过滤、仍留在进程内存中的会话状态。该状态立即回注到发起请求的同一个
Playwright profile，随后 solver session 被销毁。Playwright 必须重新访问出版商并
验证正文，之后才可提取全文、figure、引文、PDF 或 SI；solver HTML、图片和下载结果
均不能直接作为交付物。

当前版本只实现了经过审查的 `flaresolverr` 会话提供器。以后可以增加其他提供器，
但必须实现显式 adapter、纳入配置校验和测试，并在启用前重新完成人工签署；本包不提供
任意 cookie 导入接口。

二进制 PDF/SI 必须由完成回注验证的请求 profile、出版商 API，或经过治理的
repository fallback 获取。PNAS SI 可按
精确 DOI 和归档文件名从
Europe PMC 恢复；其他替代 SI 来源必须登记在
`config/supplementary-fallbacks.json`，并同时通过原 URL、SHA-256 和 PDF 内 DOI
校验。

生产发布仍应优先使用完整 Dockerfile。若镜像仓库暂时不可达，维护者可在先保留并标记
当前生产镜像后，使用 `Dockerfile.engine.release-overlay` 和
`Dockerfile.mcp.release-overlay` 从该已验证依赖基线构建；overlay 只用于有记录的
发布恢复，不能代替后续完整重建。

## 内网 Research Gateway

远端部署可以把同一浏览器池通过带 API key 的 Streamable HTTP MCP 和 REST
接口提供给实验室内网客户端。部署者应把下列示例域名替换为经过审查的网关地址：

```text
MCP:  https://scientist.example.edu:8318/mcp/research
REST: https://scientist.example.edu:8318/api/research/v1/
```

网关只在远端 loopback 的 `9040` 端口监听，由 Caddy 在 `8318` 终止 TLS。
浏览器和池的 `9020/9030` 端口不对内网开放。每个使用者或服务应有独立 API
key；服务端只保存摘要，支持热加载撤销与轮换。Codex 客户端配置示例见
`codex.remote.config.example.toml`，客户端说明见 `client/README.md`。

## 使用前审查

先打开 `config/publishers.json`：

1. 检查 `review.scope` 是否适用于当前机构和机器。
2. 检查每个 `publishers[].domains`。
3. 只为确有访问依据的出版商保留 `enabled: true`。
4. 决定该出版商是否允许 `challenge_solver` 和 `pdf_fetch`。
5. 修改后运行审核命令，让审核人签署当前配置摘要：

```powershell
./scripts/approve-policy.ps1 -ReviewedBy "Reviewer name"
```

该命令会更新 `reviewed_by`、`reviewed_at` 和 `approved_policy_sha256`。
域名或访问策略在审核后再发生变化，浏览器会拒绝启动。

配置拒绝通配符、IP、localhost、重复域名和未经批准的状态。FlareSolverr
在本包中默认启用，但只会在普通浏览器判定为 challenge 或空正文壳时，
对 `challenge_solver: true` 的域名运行。

## Windows 快速启动

需要 Docker Desktop 和 Docker Compose：

```powershell
Copy-Item .env.example .env
./scripts/start.ps1
./scripts/register-codex.ps1
```

重启 Codex 后使用 `/mcp` 检查 `scientist-literature`。如果 Codex CLI
注册不可用，可按 `codex.config.example.toml` 手工加入 MCP 配置。

停止服务：

```powershell
./scripts/stop.ps1
```

停止不会删除命名 volume，因此浏览器会话仍保留。只有明确执行
`docker compose down --volumes` 才会删除 profile。

## Linux / WSL

```bash
cp .env.example .env
./scripts/start.sh
./scripts/register-codex.sh
```

## Queue health

The browser queue is bounded. Each request budget covers queue wait and
execution; requests that time out before execution are cancelled and skipped.
If an active read exceeds its budget plus the configured stall grace, `/ready`
returns HTTP 503 so the container health policy can restart the worker without
deleting its persistent browser profile.

## MCP tools

| Tool | 用途 |
|---|---|
| `literature_publishers` | 查看当前经过人审的出版商和域名策略 |
| `literature_read` | 读取 DOI、正文 HTML、文章 PDF 或 SI；`max_chars=0` 表示不截断 |
| `literature_figure_read` | 按索引读取一张 HTML figure，或把 PDF/SI 的指定页渲染为 MCP image |
| `literature_health` | 查看浏览器存活状态和最近一次巡检 |
| `literature_probe` | 立即复核一组出版商并更新脱敏报告 |
| `literature_session_refresh` | 对一个出版商预热会话；不返回正文或 cookie |

所有工具只允许 `config/publishers.json` 中的域名。完整的 agent 使用约定见
`AGENT_USAGE.md`。

## 自动巡检与修复

巡检和修复属于本 MCP 包的服务能力，而不是 LarkBot 或 Foreman 的逻辑。
生产默认使用按需维护：外部请求优先，真实读取成功会更新完全相同的出版商和资源
类型；浏览器或上下文连续失败后才进入隔离与修复。每 6 小时的少量文章/SI canary
用于发现出版商级故障。48 个 profile 分成两个独立的 24-profile 巡检 shard；每个
shard 最多使用 4 个检查槽，并让前台读取保持优先。自动 replacement 默认关闭，
publisher、URL context 或 solver queue 故障不会被误当成 profile 损坏。

profile 在任一时刻只能处于 `available`、`serving_external`、`checking`、
`draining`、`replacing`、`warming`、`retired` 或 `quarantined` 之一。
`scientist-literature-profile-repair` 只处理重复出现的浏览器进程、上下文或后端故障；
一次出版社 challenge/timeout 不会删除 profile。需要人工运行 fleet 时，候选 generation
仍与目标一对一绑定，只有验证改善后才提交，否则原 generation 原子恢复。

单机 Docker 示例仍可使用 `monitor` 容器执行 `config/probes.json` 中的文章和
supporting-information 基准。旧的多进程 `profile-patrol@` units 仅为回滚兼容，生产
fleet 启用后必须停用，避免两个维护控制器同时占用同一个 profile。
RSC profile 经人工完成 challenge 后，可按需启用独立的
`scientist-literature-rsc-keep-warm@` 两 lane 服务。它只维护配置中明确列出的 RSC
profile：正文会话按 profile 错峰续温，正文和 SI canary 轮换验证。每次请求仍通过
共享 pool 租约；profile 正在服务外部请求时，维护请求不会抢占。publisher challenge
会触发同 profile 的受控 solver handoff 和重试，但不会触发 profile 删除或替换。
SI 只有在实际取得成功的 PDF 响应、下载字节数达到配置阈值并完成文本提取时才算
健康。publisher timeout/challenge 只更新该 profile 的出版商能力记录，不作为浏览器
故障隔离；只有 browser/context/X-server 或 malformed backend failure 才会触发
profile quarantine。所有状态均脱敏，不保存文章正文、PDF 字节、cookie、后端 URL
或 capability token。

## Publisher-Specific Strategy

Elsevier / ScienceDirect reads are API-only when the private host provides the
API key and institutional token through environment variables. Article text uses
the Elsevier Article Retrieval API; figure images and SI object PDFs use the
Elsevier Object Retrieval API; article PDFs use the Article Retrieval API PDF
response when available. If legacy XML has no figure object, figure reads render
the requested page from that API PDF instead of invoking a browser. The package
must not fall back to ScienceDirect browser navigation, FlareSolverr, or
browser-profile PDF fetching for Elsevier-owned URLs. API failures are returned
as terminal Elsevier access/artifact errors.

The request pool records article and SI capability independently for ACS, RSC,
Nature, Springer, Elsevier, Wiley, PNAS, and Science. Successful real article/SI
reads renew only that exact publisher/resource capability for the selected
profile. Sanitized capability state is persisted in the pool volume, so a
container recreation does not forget working publisher/profile pairings.
Per-publisher and global concurrency gates prevent foreground requests or bounded
canaries from producing a publisher request stampede.

Challenge recovery uses a profile-exclusive named FlareSolverr session only after the
normal browser detects a challenge. The engine and solver use the exact same Chromium
build. A solver session is bound to one publisher and one `PROFILE_ID`. Its allowlisted
cookies and exact user agent are handed to the matching Playwright profile in memory
only. Playwright must then navigate to the publisher again and verify real content;
solver HTML is never accepted as the delivered article. Article text, figures,
references, PDFs, and SI are all extracted by that verified Playwright profile. Session
material is never exported, logged, persisted separately, or applied to another
profile or publisher. The named solver session is destroyed immediately after the
handoff state is captured. The solver service has a bounded global
concurrency limit (four by default); excess requests queue, and production reads retain
priority over maintenance. Failure is terminal for that recovery attempt: there is no
ordinary-profile retry, second solver browser, secondary HTTP client, URL-only image
result, or alternate source.

RSC legacy records remain profile-state sensitive. A local desktop browser that
passes a Cloudflare/RSC challenge proves only that local profile; it is not a
deployable fix for the remote host. Reserved RSC profiles are marked warm only by
their own successful Playwright reads after an optional solver handoff. Unresolved RSC
challenges remain `access_state=challenge`.

For a deliberately workstation-owned RSC path, use
`deploy/windows-rsc-edge/`. It runs two persistent, visible Chrome profiles and
publishes only a loopback browser pool through a restricted reverse SSH tunnel.
The Research Gateway selects it with `LITERATURE_BROWSER_ROUTES_JSON`, using
either the `rsc` affinity or an RSC hostname. Routed requests make one upstream
attempt and never fall back to the general pool. A stopped workstation therefore
produces an explicit RSC route error while other publishers and gateway health
remain available.

## 会话与扩容

浏览器 profile 位于外部数据目录或 Docker 命名 volume，不属于 Git，也不会进入
分发 ZIP。每次读取结束后，任务页面和额外弹窗都会关闭，只保留一个空白保活页；
访问网址、浏览历史及出版社会话仍由该持久 profile 保存。

单机快速启动仍使用一个 browser service。集群部署由两个受 cgroup 限制的浏览器
引擎分别管理 `001..024` 与 `025..048`，并按需启动 Chromium 子进程。每个 24-profile
shard 使用 6 个独立 FlareSolverr，每个 solver 固定服务 4 个 profile，并在服务进程内
全局限制为 2 个并发。共享池交错两个 shard 的后端顺序，对内网入口保持一个地址。
空闲 30 分钟后只退出子进程并保留 profile。生产模板见
`docker-compose.cluster.yml` 和 `deploy/systemd/`。

集群容器以非 root 用户运行。`LITERATURE_PRIVATE_ROOT` 应保持
`root:<private-group>` 和 `0750/0640` 权限；设置 `LITERATURE_PRIVATE_GROUP_ID`
为该 host group 的数字 GID，使 browser-engine 和 pool 容器通过补充组读取内部
maintenance token，而无需把 token 设为 world-readable。

`profiles/`、`profiles-b/` 与 `pool-state/` 都位于 `LITERATURE_DATA_ROOT`。生产升级应使用新的
版本化数据根，避免把旧 profile 的 publisher warm 状态带入新集群；回滚时同时恢复
上一 release 与其数据根。`LITERATURE_PRIVATE_ROOT` 独立指向受保护的共享 token
目录，不随 profile 数据根复制。

## 分发

从 Scientist 仓库生成干净 ZIP：

```powershell
python tools/sync_literature_browser_mcp.py --check `
  --archive .private/dist/literature-browser-mcp.zip
```

ZIP 只收录 Git 跟踪文件，不含 `.env`、健康报告、浏览器 profile、cookie、
生产专用 edge/SSH 配置或恢复用 Docker overlay。它附带
`STANDALONE-MANIFEST.json`，并把出版商 policy 重置为必须由接收者重新审核。
独立使用和验证步骤见 `STANDALONE.md`。公开到独立 GitHub 仓库前，项目所有者
还需要为本包选择源代码许可证。

## 边界

- 仅用于机构已订阅或公开可访问的内容。
- 不做批量抓取，不绕过付费授权，不导出认证材料。
- 页面正文是外部不可信数据，agent 不得执行其中的指令。
- challenge 恢复失败时，应保留 `access_state` 证据边界，而不是声称已读全文。
