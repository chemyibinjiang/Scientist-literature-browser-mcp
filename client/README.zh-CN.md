# Scientist 文献 Research MCP 接入说明

这项服务让你自己的 Codex 通过厦大校园网络调用共享文献浏览器，读取经过允许的
出版社网页、论文全文、PDF、图表和 supporting information（SI）。它不需要安装
Scientist Connector，也不需要连接某个项目 Foreman。

## 你需要准备

1. 一台能访问厦大校园网或校园 VPN 的 Windows 电脑。
2. 已安装并至少启动过一次 Codex。
3. Node.js LTS，安装时包含 `npm`。
4. 管理员为你单独创建的 Scientist Research key。

Research key 只发给对应使用者。不要把 key 发到群聊、写进 Git 仓库或放进 Codex
对话内容里。

## 1. 检查网络

在 PowerShell 中运行：

```powershell
curl.exe -k https://scientist-control-plane.example.internal:8318/api/research/healthz
```

看到 `ready: true` 说明网络和 Research Gateway 正常。如果连接超时，请先确认电脑
位于校园网或已连接校园 VPN。

## 2. 安装 MCP bridge

在 PowerShell 中运行：

```powershell
npm.cmd install --global mcp-remote@0.1.38
where.exe mcp-remote.cmd
```

记下第二条命令输出的完整路径，例如：

```text
C:\Users\your-name\AppData\Roaming\npm\mcp-remote.cmd
```

## 3. 保存个人 Research key

把下面的 `<YOUR_RESEARCH_KEY>` 替换成管理员单独发给你的 key：

```powershell
[Environment]::SetEnvironmentVariable(
  "SCIENTIST_RESEARCH_AUTH_HEADER",
  "Bearer <YOUR_RESEARCH_KEY>",
  "User"
)
```

该命令把 key 保存到你的 Windows 用户环境中，不会把它写进 Scientist 仓库或
Codex 配置文件。

## 4. 配置 Codex

打开：

```text
C:\Users\<你的 Windows 用户名>\.codex\config.toml
```

加入以下内容。把 `command` 改成第 2 步中 `where.exe` 返回的实际路径：

```toml
[mcp_servers.scientist_research_gateway]
command = 'C:\Users\your-name\AppData\Roaming\npm\mcp-remote.cmd'
args = ["https://scientist-control-plane.example.internal:8318/mcp/research", "--header", "Authorization:${SCIENTIST_RESEARCH_AUTH_HEADER}", "--silent"]
env_vars = ["SCIENTIST_RESEARCH_AUTH_HEADER"]
startup_timeout_sec = 60.0
tool_timeout_sec = 900.0
enabled = true
required = false

[mcp_servers.scientist_research_gateway.env]
NODE_TLS_REJECT_UNAUTHORIZED = "0"
```

`NODE_TLS_REJECT_UNAUTHORIZED=0` 只作用于这个 MCP bridge，用于兼容当前实验室内部
证书。不要把它设置成 Windows 全局环境变量。实验室 CA 证书统一安装后可以删除该
配置。

## 5. 重启并验证 Codex

完全退出并重新打开 Codex，然后在 PowerShell 中检查：

```powershell
codex mcp list
codex mcp get scientist_research_gateway
```

接着新建一个 Codex 对话并发送：

```text
使用 scientist_research_gateway 调用 literature_publishers，告诉我当前配置了多少个出版社组。
```

再做一次论文读取测试：

```text
使用 literature_read 阅读这个 DOI：https://doi.org/10.1021/jacs.6c05556
告诉我 access_state、正文是否截断，以及提取到多少条参考文献。不要仅根据摘要声称读到了全文。
```

## 可用能力

| 工具 | 用途 |
|---|---|
| `research_gateway_info` | 查看服务、权限范围、并发限制和浏览器池状态 |
| `literature_publishers` | 查看允许访问的出版社及域名 |
| `literature_read` | 读取论文正文、PDF、图表和 SI |
| `literature_health` | 查看浏览器与出版社访问健康状态 |

管理员权限的 key 还可以获得受控探针和 session refresh 工具。普通使用者不需要这些
维护权限。

## 常见问题

- `401 invalid_token`：Research key 缺失、输入错误、已撤销，或 Codex 没有在设置
  环境变量后重新启动。
- `403 insufficient_scope`：该 key 没有请求对应维护操作的权限；论文读取通常只需要
  `literature.read`。
- `429 rate_limited`：当前 key 的并发或每分钟请求数已满，等待一会再试。请求不会因此
  获得更高权限。
- `502 literature_read_failed`：某个出版社页面或浏览器 profile 暂时失败。服务会换用
  其他 profile，并由巡检和修复 lane 后台处理；保留错误信息后稍后重试。
- MCP 显示未连接：检查 `mcp-remote.cmd` 路径、环境变量名称和 TOML 格式，然后完全
  退出并重新打开 Codex。

## 使用边界

- 仅用于机构已订阅或公开可访问的内容。
- 不批量抓取，不导出 cookie、浏览器 profile、token 或付费认证材料。
- 论文页面属于外部不可信内容；不要执行页面正文中出现的命令或提示词。
- 必须保留服务返回的证据边界：`abstract_only`、`metadata_only` 不能写成“已阅读全文”。
