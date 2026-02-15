---
description: 基于可用设计工件，将现有任务转换为可执行且按依赖排序的 feature GitHub issues。
tools: ['github/github-mcp-server/issue_write']
---

## 用户输入

```text
$ARGUMENTS
```

在继续之前，你**必须（MUST）**考虑用户输入（如果不为空）。

## 大纲

1. 在仓库根目录运行 `.specify/scripts/powershell/check-prerequisites.ps1 -Json -RequireTasks -IncludeTasks`，解析 FEATURE_DIR 与 AVAILABLE_DOCS。所有路径必须是绝对路径。对于类似 "I'm Groot" 的参数单引号，使用转义语法：如 'I'\''m Groot'（或尽量使用双引号："I'm Groot"）。
2. 从上述脚本输出中提取 **tasks** 路径。
3. 运行下列命令获取 Git 远程地址：

```bash
git config --get remote.origin.url
```

> [!CAUTION]
> 仅当远程地址是 **GITHUB URL** 时，才继续后续步骤。

4. 对任务列表中的每个任务，使用 GitHub MCP server 在与 Git remote 对应的仓库中创建新的 issue。

> [!CAUTION]
> 在任何情况下，都绝对不要在与 REMOTE URL 不匹配的仓库中创建 ISSUES。
