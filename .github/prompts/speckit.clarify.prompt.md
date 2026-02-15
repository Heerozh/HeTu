---
description: 通过最多 5 个高针对性澄清问题识别当前 feature spec 中欠规格区域，并将答案回写到 spec。
handoffs:
  - label: Build Technical Plan
    agent: speckit.plan
    prompt: Create a plan for the spec. I am building with...
---

## 用户输入

```text
$ARGUMENTS
```

在继续之前，你**必须（MUST）**考虑用户输入（如果不为空）。

## 大纲

目标：识别并降低当前 feature specification 中的歧义与缺失决策点，并将澄清内容直接记录到 spec 文件。

注意：该澄清流程应在调用 `/speckit.plan` **之前**运行并完成。若用户明确表示跳过澄清（例如 exploratory spike），可以继续，但必须提示下游返工风险会上升。

执行步骤：

1. 在仓库根目录**只运行一次** `.specify/scripts/powershell/check-prerequisites.ps1 -Json -PathsOnly`（组合模式 `--json --paths-only` / `-Json -PathsOnly`）。解析最小 JSON 字段：
   - `FEATURE_DIR`
   - `FEATURE_SPEC`
   - （可选捕获 `IMPL_PLAN`、`TASKS` 以便后续串联流程）
   - 若 JSON 解析失败，终止并提示用户重新运行 `/speckit.specify` 或检查 feature branch 环境。
   - 对类似 "I'm Groot" 的参数单引号，使用转义语法：如 'I'\''m Groot'（或尽量使用双引号："I'm Groot"）。

2. 读取当前 spec 文件。按以下分类执行结构化“歧义与覆盖”扫描。每个类别标记为：Clear / Partial / Missing。构建内部 coverage map 用于排序（除非最终不提问，否则不要输出原始 map）。

   Functional Scope & Behavior:
   - 核心用户目标与成功标准
   - 明确的 out-of-scope 声明
   - 用户角色 / personas 区分

   Domain & Data Model:
   - 实体、属性、关系
   - 身份与唯一性规则
   - 生命周期/状态迁移
   - 数据量/规模假设

   Interaction & UX Flow:
   - 关键用户旅程/步骤序列
   - 错误/空态/加载态
   - 可访问性或本地化说明

   Non-Functional Quality Attributes:
   - 性能（延迟、吞吐目标）
   - 可扩展性（水平/垂直、上限）
   - 可靠性与可用性（uptime、恢复预期）
   - 可观测性（日志、指标、追踪信号）
   - 安全与隐私（authN/Z、数据保护、威胁假设）
   - 合规/监管约束（如有）

   Integration & External Dependencies:
   - 外部服务/API 及故障模式
   - 数据导入/导出格式
   - 协议/版本假设

   Edge Cases & Failure Handling:
   - 负向场景
   - 限流/节流
   - 冲突解决（例如并发编辑）

   Constraints & Tradeoffs:
   - 技术约束（语言、存储、托管）
   - 明确权衡或被拒绝的替代方案

   Terminology & Consistency:
   - 规范术语（canonical glossary）
   - 避免使用的同义词/废弃术语

   Completion Signals:
   - acceptance criteria 的可测试性
   - 可度量 Definition of Done 风格指标

   Misc / Placeholders:
   - TODO 标记 / 未决决策
   - 缺乏量化的模糊形容词（"robust"、"intuitive"）

   对每个标记为 Partial 或 Missing 的类别，加入一个候选澄清问题，除非：
   - 澄清不会实质改变实现或验证策略
   - 信息更适合延后到 planning 阶段（内部记录）

3. （内部）生成优先级澄清问题队列（最多 5 个）。不要一次性全部输出。约束如下：
   - 整个会话中最多 10 个问题。
   - 每个问题必须可通过以下之一回答：
     - 简短多选（2–5 个互斥选项），或
     - 一词/短语回答（显式约束："Answer in <=5 words"）。
   - 仅纳入那些答案会实质影响 architecture、data modeling、task 拆解、test 设计、UX 行为、运维准备度或合规验证的问题。
   - 兼顾类别覆盖：优先覆盖高影响未决项；避免在高影响领域（如安全姿态）未解时去问两个低影响问题。
   - 排除已回答问题、纯风格偏好、或计划层执行细节（除非阻塞正确性）。
   - 优先选择能减少下游返工或避免验收错配的问题。
   - 若未决类别超过 5 个，按（Impact * Uncertainty）选前 5。

4. 顺序提问循环（交互）：
   - 一次只呈现 **一个**问题。
   - 对多选题：
     - **分析所有选项**并基于以下维度选出**最合适选项**：
       - 项目类型最佳实践
       - 相似实现常见模式
       - 风险降低（安全、性能、可维护性）
       - 与 spec 中显式目标/约束的一致性
     - 在顶部突出展示**推荐项**及 1–2 句理由。
     - 格式：`**Recommended:** Option [X] - <reasoning>`
     - 然后用 Markdown 表格展示全部选项：

       | Option | Description |
       |--------|-------------|
       | A | <Option A description> |
       | B | <Option B description> |
       | C | <Option C description> |
       | Short | Provide a different short answer (<=5 words) |

       - 表后附：`You can reply with the option letter (e.g., "A"), accept the recommendation by saying "yes" or "recommended", or provide your own short answer.`
   - 对短答题（无有意义离散选项）：
     - 基于上下文与最佳实践给出**建议答案**。
     - 格式：`**Suggested:** <your proposed answer> - <brief reasoning>`
     - 然后输出：`Format: Short answer (<=5 words). You can accept the suggestion by saying "yes" or "suggested", or provide your own answer.`
   - 用户回答后：
     - 若用户回复 "yes"、"recommended" 或 "suggested"，采用此前推荐/建议答案。
     - 否则校验其是否映射到选项，或满足 <=5 词约束。
     - 若歧义，进行快速消歧（仍算同一问题，不前进计数）。
     - 一旦答案有效，记录到工作内存（暂不落盘）并进入下一个问题。
   - 在以下情况停止继续提问：
     - 关键歧义已提前解决（剩余队列变得不必要），或
     - 用户发出结束信号（"done"、"good"、"no more"），或
     - 已问满 5 个问题。
   - 不要提前透露后续队列问题。
   - 若一开始就无有效问题，立即报告无关键歧义。

5. 每个已接受答案后的集成（增量更新）：
   - 维护 spec 的内存表示（启动时加载一次）+ 原始文本。
   - 本会话首次集成答案时：
     - 确保存在 `## Clarifications` 章节（若缺失，插入到最高层上下文/概览章节之后）。
     - 在其下创建（若不存在）`### Session YYYY-MM-DD` 子标题（今天日期）。
   - 每次接受后立即追加 bullet：`- Q: <question> → A: <final answer>`。
   - 然后立刻将澄清应用到最合适章节：
     - 功能歧义 → 更新/新增到 Functional Requirements
     - 用户交互/角色区分 → 更新 User Stories 或 Actors 子节
     - 数据形态/实体 → 更新 Data Model（字段、类型、关系，保持顺序）
     - 非功能约束 → 在 Non-Functional / Quality Attributes 中新增/修改可度量标准
     - 边界/负向流 → 在 Edge Cases / Error Handling 下新增 bullet（或创建对应子节）
     - 术语冲突 → 全文统一 canonical term；必要时仅保留一次 `(formerly referred to as "X")`
   - 若新澄清使旧表述失效，替换旧表述而非并列重复；不得留下相互矛盾文本。
   - 每次集成后都要保存 spec（原子覆盖），降低上下文丢失风险。
   - 保持格式：不要重排无关章节；保持标题层级。
   - 插入内容应最小化且可测试（避免叙述漂移）。

6. 验证（每次写入后 + 最终通检）：
   - Clarifications 会话中每个已接受答案恰好一条 bullet（无重复）
   - 已问（已接受）问题总数 ≤ 5
   - 被更新章节不应残留本应由新答案消除的模糊占位
   - 不应保留过时且矛盾的旧陈述
   - Markdown 结构有效；仅允许新增标题：`## Clarifications`、`### Session YYYY-MM-DD`
   - 术语一致：更新段落中使用同一 canonical term

7. 将更新后的 spec 写回 `FEATURE_SPEC`。

8. 提问循环结束（或提前终止）后输出完成报告：
   - 已提问并已回答数量
   - 更新后的 spec 路径
   - 触达章节列表
   - 覆盖汇总表：每个分类的状态为 Resolved / Deferred / Clear / Outstanding
   - 若仍有 Outstanding/Deferred，建议是否继续 `/speckit.plan` 或稍后再次运行 `/speckit.clarify`
   - 建议下一条命令

行为规则：

- 若未发现有意义歧义（或都属低影响），回复："No critical ambiguities detected worth formal clarification." 并建议继续。
- 若 spec 文件缺失，提示先运行 `/speckit.specify`（不要在此创建新 spec）。
- 总提问数绝不超过 5（同一问题的重试不计新问题）。
- 避免推测性 tech stack 问题，除非缺失会阻塞功能清晰度。
- 尊重用户提前终止信号（"stop"、"done"、"proceed"）。
- 若因覆盖充分而未提问，输出紧凑覆盖摘要（全部 Clear）并建议进入下一阶段。
- 若到达配额且仍有高影响未决项，在 Deferred 中明确标注并说明原因。

用于优先级判断的上下文：$ARGUMENTS
