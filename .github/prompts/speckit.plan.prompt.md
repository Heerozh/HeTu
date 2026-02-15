---
description: 使用 plan template 执行实现规划 workflow，生成 design 工件。
handoffs:
  - label: 创建任务
    agent: speckit.tasks
    prompt: 将计划拆解为任务
    send: true
  - label: 创建检查清单
    agent: speckit.checklist
    prompt: 为以下领域创建 checklist...
---

## 用户输入

```text
$ARGUMENTS
```

在继续之前，你**必须（MUST）**考虑用户输入（如果不为空）。

## 大纲

1. **Setup**：在仓库根目录运行 `.specify/scripts/powershell/setup-plan.ps1 -Json`，解析 JSON 中的 FEATURE_SPEC、IMPL_PLAN、SPECS_DIR、BRANCH。对于类似 "I'm Groot" 的参数单引号，使用转义语法：如 'I'\''m Groot'（或尽量使用双引号："I'm Groot"）。

2. **Load context**：读取 FEATURE_SPEC 与 `.specify/memory/constitution.md`。加载 IMPL_PLAN template（已复制）。

3. **执行 plan workflow**：按 IMPL_PLAN template 结构完成：
   - 填写 Technical Context（未知项标记为 `NEEDS CLARIFICATION`）
   - 基于 constitution 填写 Constitution Check
   - 评估 gates（若违规且无合理理由则报 `ERROR`）
   - Phase 0：生成 `research.md`（解决所有 `NEEDS CLARIFICATION`）
   - Phase 1：生成 `data-model.md`、`contracts/`、`quickstart.md`
   - Phase 1：通过运行 agent 脚本更新 agent context
   - design 后重新评估 Constitution Check

4. **停止并报告**：命令在 Phase 2 planning 完成后结束。报告 branch、IMPL_PLAN 路径和已生成工件。

## Phases

### Phase 0: Outline & Research

1. 从上方 Technical Context 提取未知项：
   - 每个 `NEEDS CLARIFICATION` → 一个 research task
   - 每个 dependency → 一个 best practices task
   - 每个 integration → 一个 patterns task

2. **生成并分发 research agents**：

   ```text
   For each unknown in Technical Context:
     Task: "Research {unknown} for {feature context}"
   For each technology choice:
     Task: "Find best practices for {tech} in {domain}"
   ```

3. 将结论汇总到 `research.md`，格式：
   - Decision: [选择了什么]
   - Rationale: [为什么这样选]
   - Alternatives considered: [评估过哪些替代方案]

**输出**：`research.md`，且所有 `NEEDS CLARIFICATION` 已解决

### Phase 1: Design & Contracts

**前置条件：** `research.md` 已完成

1. **从 feature spec 提取实体** → `data-model.md`：
   - 实体名称、字段、关系
   - 来自 requirements 的校验规则
   - 如适用，包含状态迁移

2. **基于 functional requirements 生成 API contracts**：
   - 每个用户动作 → 一个 endpoint
   - 使用标准 REST/GraphQL 模式
   - 将 OpenAPI/GraphQL schema 输出到 `/contracts/`

3. **Agent context update**：
   - 运行 `.specify/scripts/powershell/update-agent-context.ps1 -AgentType claude`
   - 脚本会检测当前使用的 AI agent
   - 更新相应的 agent-specific context 文件
   - 仅追加当前计划中的新技术
   - 保留标记区间之间的手工补充内容

**输出**：`data-model.md`、`/contracts/*`、`quickstart.md`、agent-specific 文件

## Key rules

- 使用绝对路径
- 若 gate 失败或澄清未解决，报 `ERROR`
