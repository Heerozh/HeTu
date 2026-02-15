---
description: 根据交互式或提供的原则输入创建或更新项目 Constitution，并确保所有依赖模板保持同步。
handoffs:
  - label: 构建规格说明
    agent: speckit.specify
    prompt: 基于更新后的 Constitution 生成功能规格。我想构建...
---

## 用户输入

```text
$ARGUMENTS
```

在继续之前，你**必须（MUST）**考虑用户输入（如果不为空）。

## 大纲

你正在更新位于 `.specify/memory/constitution.md` 的项目 Constitution。该文件是一个 TEMPLATE，包含方括号占位符（例如 `[PROJECT_NAME]`、`[PRINCIPLE_1_NAME]`）。你的任务是：
(a) 收集/推导具体值，(b) 精确填充模板，(c) 将任何修订传播到依赖工件。

**注意**：如果 `.specify/memory/constitution.md` 不存在，应在项目初始化时由 `.specify/templates/constitution-template.md` 初始化。若缺失，请先复制模板。

按如下流程执行：

1. 加载现有 `.specify/memory/constitution.md`。
   - 识别所有形如 `[ALL_CAPS_IDENTIFIER]` 的占位符。
   **重要**：用户要求的原则数量可能少于或多于模板默认数量。若指定了数量，请按该数量更新通用模板。

2. 收集/推导占位符值：
   - 若用户输入（对话）提供值，则直接使用。
   - 否则从仓库上下文推断（README、docs、旧版 Constitution）。
   - 治理日期规则：`RATIFICATION_DATE` 为首次批准日期（未知则询问或标记 TODO）；`LAST_AMENDED_DATE` 若有修改则为今天，否则保持原值。
   - `CONSTITUTION_VERSION` 必须按语义化版本递增：
     - MAJOR：治理/原则被不兼容删除或重定义。
     - MINOR：新增原则/章节，或实质扩展指导。
     - PATCH：澄清、措辞、拼写、非语义优化。
   - 若版本提升级别不明确，先给出理由再定稿。

3. 起草更新后的 Constitution 内容：
   - 用具体文本替换每个占位符（除非项目明确决定暂时保留某槽位，否则不得残留方括号标记；若保留必须说明原因）。
   - 保持标题层级；替换后可删除注释，除非其仍有澄清价值。
   - 每个 Principle 章节需包含：
     - 简洁命名行
     - 不可妥协规则的段落或列表
     - 必要时给出明确 rationale
   - Governance 章节需列出：修订流程、版本策略、合规审查预期。

4. 一致性传播检查（把清单转为主动验证）：
   - 读取 `.specify/templates/plan-template.md`，确保其中 “Constitution Check” 或规则与更新后的原则一致。
   - 读取 `.specify/templates/spec-template.md`，若 Constitution 新增/移除了强制章节或约束，则同步更新。
   - 读取 `.specify/templates/tasks-template.md`，确保任务分类反映新增/移除的原则驱动任务类型（如 observability、versioning、testing discipline）。
   - 读取 `.specify/templates/commands/*.md` 下所有命令（包括当前文件），检查是否存在过时的通用指导引用（例如仅绑定特定 agent 名称 CLAUDE）。
   - 读取运行时指导文档（如 `README.md`、`docs/quickstart.md`、或 agent-specific 指南文件），更新其中已变更原则的引用。

5. 生成同步影响报告（以 HTML 注释追加到 Constitution 文件顶部）：
   - 版本变更：旧 → 新
   - 修改的原则列表（重命名时写旧名 → 新名）
   - 新增章节
   - 删除章节
   - 需更新模板清单（✅ updated / ⚠ pending）及文件路径
   - 若有刻意延期的占位符，列出后续 TODO

6. 最终输出前验证：
   - 不应存在未解释的方括号占位符。
   - 版本行与影响报告一致。
   - 日期使用 ISO 格式 `YYYY-MM-DD`。
   - 原则应声明式、可验证，避免模糊措辞（必要时将 should 升级为 MUST/SHOULD 并给出理由）。

7. 将完成后的 Constitution 覆写回 `.specify/memory/constitution.md`。

8. 向用户输出最终总结，包含：
   - 新版本与升级理由。
   - 标记为需人工后续处理的文件。
   - 建议提交信息（例如：`docs: amend constitution to vX.Y.Z (principle additions + governance update)`）。

Formatting & Style Requirements:

- 严格使用模板原有 Markdown 标题层级（不要升降级）。
- 较长理由适度换行（理想 <100 字符），避免生硬断句。
- 各章节之间保持一个空行。
- 避免行尾空白。

如果用户只提供局部更新（例如只改一个 principle），仍需执行验证与版本决策步骤。

若关键信息确实缺失（如 ratification date 未知），插入 `TODO(<FIELD_NAME>): explanation`，并在同步影响报告的延期项中记录。

不要创建新模板；始终在现有 `.specify/memory/constitution.md` 文件上操作。
