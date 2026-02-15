---
description: 基于用户需求为当前 feature 生成自定义 checklist。
---

## Checklist Purpose: "Unit Tests for English"

**CRITICAL CONCEPT**：checklist 是**需求写作的单元测试（UNIT TESTS FOR REQUIREMENTS WRITING）**——用于验证某一领域内需求的质量、清晰度和完整性。

**不是用于 verification/testing**：

- ❌ 不是 "Verify the button clicks correctly"
- ❌ 不是 "Test error handling works"
- ❌ 不是 "Confirm the API returns 200"
- ❌ 不是检查代码/实现是否符合 spec

**是用于需求质量校验**：

- ✅ "Are visual hierarchy requirements defined for all card types?"（完整性）
- ✅ "Is 'prominent display' quantified with specific sizing/positioning?"（清晰性）
- ✅ "Are hover state requirements consistent across all interactive elements?"（一致性）
- ✅ "Are accessibility requirements defined for keyboard navigation?"（覆盖性）
- ✅ "Does the spec define what happens when logo image fails to load?"（边界场景）

**隐喻**：如果你的 spec 是用英文写的代码，那么 checklist 就是它的单元测试套件。你测试的是需求是否写得好、完整、无歧义、可落地实现——而不是实现是否运行正确。

## 用户输入

```text
$ARGUMENTS
```

在继续之前，你**必须（MUST）**考虑用户输入（如果不为空）。

## 执行步骤

1. **Setup**：在仓库根目录运行 `.specify/scripts/powershell/check-prerequisites.ps1 -Json`，解析 JSON 中的 FEATURE_DIR 和 AVAILABLE_DOCS。
   - 所有路径都必须是绝对路径。
   - 对类似 "I'm Groot" 的参数单引号，使用转义语法：如 'I'\''m Groot'（或尽量用双引号："I'm Groot"）。

2. **Clarify intent（动态）**：最多生成三个初始澄清问题（不可使用预置题库）。这些问题必须：
   - 来自用户措辞 + 从 spec/plan/tasks 提取的信号
   - 只询问会实质影响 checklist 内容的信息
   - 若 `$ARGUMENTS` 已明确，则逐条跳过
   - 重精度，轻广度

   生成算法：
   1. 提取信号：feature 领域关键词（如 auth、latency、UX、API）、风险指示词（"critical"、"must"、"compliance"）、角色提示（"QA"、"review"、"security team"）、显式交付物（"a11y"、"rollback"、"contracts"）。
   2. 将信号聚类成候选 focus areas（最多 4 个），按相关性排序。
   3. 若未明确，则识别可能 audience 与 timing（author、reviewer、QA、release）。
   4. 检测缺失维度：范围宽度、深度/严谨度、风险侧重、排除边界、可度量 acceptance criteria。
   5. 从以下原型构建问题：
      - Scope refinement
      - Risk prioritization
      - Depth calibration
      - Audience framing
      - Boundary exclusion
      - Scenario class gap

   问题格式规则：
   - 若提供选项，使用紧凑表格：Option | Candidate | Why It Matters
   - 选项最多 A–E；若自由回答更清晰可省略表格
   - 不要让用户重复已说明内容
   - 避免臆测分类（不要 hallucination）。不确定时明确询问："Confirm whether X belongs in scope."

   无法交互时默认：
   - Depth: Standard
   - Audience: Reviewer（PR，若代码相关）否则 Author
   - Focus: 相关性前 2 的 clusters

   输出问题并标注 Q1/Q2/Q3。收到回答后：若仍有 ≥2 个 scenario class（Alternate / Exception / Recovery / Non-Functional）不清晰，可追加最多两个定向追问（Q4/Q5），并给一行理由（如 "Unresolved recovery path risk"）。总问题数不得超过 5。若用户明确拒绝追加问题，则停止升级。

3. **理解用户请求**：结合 `$ARGUMENTS` + 澄清回答：
   - 推导 checklist 主题（如 security、review、deploy、ux）
   - 汇总用户明确提出的 must-have 项
   - 将 focus 映射到分类脚手架
   - 从 spec/plan/tasks 推断缺失上下文（不得 hallucinate）

4. **加载 feature 上下文**：从 FEATURE_DIR 读取：
   - spec.md：feature 需求与范围
   - plan.md（若存在）：技术细节、依赖
   - tasks.md（若存在）：实现任务

   **Context Loading Strategy**：
   - 仅加载与当前 focus area 相关的必要部分（避免整文件倾倒）
   - 长段落优先摘要为简洁的 scenario/requirement bullets
   - 渐进披露：发现缺口再补充读取
   - 若源文档很大，生成中间摘要项，不嵌入原始大段文本

5. **生成 checklist**——创建“需求的单元测试”：
   - 如不存在则创建 `FEATURE_DIR/checklists/` 目录
   - 生成唯一 checklist 文件名：
     - 使用基于领域的短描述名（如 `ux.md`、`api.md`、`security.md`）
     - 格式：`[domain].md`
     - 若文件已存在，追加内容
   - 条目编号从 CHK001 顺序递增
   - 每次 `/speckit.checklist` 运行都创建**新文件**（绝不覆盖既有 checklist）

   **核心原则（CORE PRINCIPLE）——测试需求，不测试实现**：
   每一项必须评估需求文本本身：
   - **Completeness**
   - **Clarity**
   - **Consistency**
   - **Measurability**
   - **Coverage**

   **分类结构（Category Structure）**：
   - Requirement Completeness
   - Requirement Clarity
   - Requirement Consistency
   - Acceptance Criteria Quality
   - Scenario Coverage
   - Edge Case Coverage
   - Non-Functional Requirements
   - Dependencies & Assumptions
   - Ambiguities & Conflicts

   **如何写条目（Unit Tests for English）**：

   ❌ **错误（测实现）**：
   - "Verify landing page displays 3 episode cards"
   - "Test hover states work on desktop"
   - "Confirm logo click navigates home"

   ✅ **正确（测需求质量）**：
   - "Are the exact number and layout of featured episodes specified?" [Completeness]
   - "Is 'prominent display' quantified with specific sizing/positioning?" [Clarity]
   - "Are hover state requirements consistent across all interactive elements?" [Consistency]
   - "Are keyboard navigation requirements defined for all interactive UI?" [Coverage]
   - "Is the fallback behavior specified when logo image fails to load?" [Edge Cases]
   - "Are loading states defined for asynchronous episode data?" [Completeness]
   - "Does the spec define visual hierarchy for competing UI elements?" [Clarity]

   **条目结构（ITEM STRUCTURE）**：
   - 使用问题句式评估需求质量
   - 聚焦 spec/plan 中“写了什么/没写什么”
   - 包含质量标签 [Completeness/Clarity/Consistency/etc.]
   - 检查现有需求时引用 `[Spec §X.Y]`
   - 检查缺失需求时使用 `[Gap]`

   **按质量维度示例**：

   Completeness:
   - "Are error handling requirements defined for all API failure modes? [Gap]"
   - "Are accessibility requirements specified for all interactive elements? [Completeness]"
   - "Are mobile breakpoint requirements defined for responsive layouts? [Gap]"

   Clarity:
   - "Is 'fast loading' quantified with specific timing thresholds? [Clarity, Spec §NFR-2]"
   - "Are 'related episodes' selection criteria explicitly defined? [Clarity, Spec §FR-5]"
   - "Is 'prominent' defined with measurable visual properties? [Ambiguity, Spec §FR-4]"

   Consistency:
   - "Do navigation requirements align across all pages? [Consistency, Spec §FR-10]"
   - "Are card component requirements consistent between landing and detail pages? [Consistency]"

   Coverage:
   - "Are requirements defined for zero-state scenarios (no episodes)? [Coverage, Edge Case]"
   - "Are concurrent user interaction scenarios addressed? [Coverage, Gap]"
   - "Are requirements specified for partial data loading failures? [Coverage, Exception Flow]"

   Measurability:
   - "Are visual hierarchy requirements measurable/testable? [Acceptance Criteria, Spec §FR-1]"
   - "Can 'balanced visual weight' be objectively verified? [Measurability, Spec §FR-2]"

   **Scenario 分类与覆盖**（仍是需求质量视角）：
   - 检查是否存在 Primary、Alternate、Exception/Error、Recovery、Non-Functional 场景需求
   - 每类都问："Are [scenario type] requirements complete, clear, and consistent?"
   - 若某类缺失："Are [scenario type] requirements intentionally excluded or missing? [Gap]"
   - 若涉及状态变更，要包含 resilience/rollback："Are rollback requirements defined for migration failures? [Gap]"

   **Traceability 要求**：
   - 最低要求：≥80% 条目包含至少一个可追溯引用
   - 每项应引用：`[Spec §X.Y]` 或标记 `[Gap]`、`[Ambiguity]`、`[Conflict]`、`[Assumption]`
   - 若无 ID 体系："Is a requirement & acceptance criteria ID scheme established? [Traceability]"

   **暴露并解决问题（面向需求质量）**：
   - Ambiguities："Is the term 'fast' quantified with specific metrics? [Ambiguity, Spec §NFR-1]"
   - Conflicts："Do navigation requirements conflict between §FR-10 and §FR-10a? [Conflict]"
   - Assumptions："Is the assumption of 'always available podcast API' validated? [Assumption]"
   - Dependencies："Are external podcast API requirements documented? [Dependency, Gap]"
   - Missing definitions："Is 'visual hierarchy' defined with measurable criteria? [Gap]"

   **内容收敛（Content Consolidation）**：
   - 软上限：候选项 > 40 时按风险/影响排序
   - 合并检查同一方面的近重复条目
   - 若低影响边界项 > 5，合并成一条："Are edge cases X, Y, Z addressed in requirements? [Coverage]"

   **🚫 绝对禁止（ABSOLUTELY PROHIBITED）**：会把它变成实现测试，而不是需求测试：
   - ❌ 任何以 "Verify"、"Test"、"Confirm"、"Check" + 实现行为开头的条目
   - ❌ 引用代码执行、用户操作、系统运行行为
   - ❌ "Displays correctly"、"works properly"、"functions as expected"
   - ❌ "Click"、"navigate"、"render"、"load"、"execute"
   - ❌ 测试用例、测试计划、QA 流程
   - ❌ 实现细节（framework、API、algorithm）

   **✅ 必需模式（REQUIRED PATTERNS）**：
   - ✅ "Are [requirement type] defined/specified/documented for [scenario]?"
   - ✅ "Is [vague term] quantified/clarified with specific criteria?"
   - ✅ "Are requirements consistent between [section A] and [section B]?"
   - ✅ "Can [requirement] be objectively measured/verified?"
   - ✅ "Are [edge cases/scenarios] addressed in requirements?"
   - ✅ "Does the spec define [missing aspect]?"

6. **结构参考**：按 `.specify/templates/checklist-template.md` 规范生成 checklist（标题、meta、分类标题、ID 格式）。若模板不可用，使用：H1 标题、purpose/created meta 行、`##` 分类段，每段使用 `- [ ] CHK### <requirement item>`，全局 ID 从 CHK001 递增。

7. **报告**：输出新建 checklist 的完整路径、条目数量，并提醒“每次运行都会创建新文件”。同时总结：
   - 选定的 focus areas
   - Depth 级别
   - Actor/timing
   - 已纳入的用户 must-have 项

**重要**：每次 `/speckit.checklist` 调用都会使用简短描述性文件名创建 checklist（除非文件已存在）。这使得：

- 能并存多类 checklist（如 `ux.md`、`test.md`、`security.md`）
- 文件名直观可记，便于表达用途
- 易于在 `checklists/` 中检索定位

为避免杂乱，请使用清晰的类型命名，并在完成后清理过时 checklist。

## 示例 checklist 类型与样例条目

**UX Requirements Quality：** `ux.md`

样例条目（测试需求，不测实现）：

- "Are visual hierarchy requirements defined with measurable criteria? [Clarity, Spec §FR-1]"
- "Is the number and positioning of UI elements explicitly specified? [Completeness, Spec §FR-1]"
- "Are interaction state requirements (hover, focus, active) consistently defined? [Consistency]"
- "Are accessibility requirements specified for all interactive elements? [Coverage, Gap]"
- "Is fallback behavior defined when images fail to load? [Edge Case, Gap]"
- "Can 'prominent display' be objectively measured? [Measurability, Spec §FR-4]"

**API Requirements Quality：** `api.md`

样例条目：

- "Are error response formats specified for all failure scenarios? [Completeness]"
- "Are rate limiting requirements quantified with specific thresholds? [Clarity]"
- "Are authentication requirements consistent across all endpoints? [Consistency]"
- "Are retry/timeout requirements defined for external dependencies? [Coverage, Gap]"
- "Is versioning strategy documented in requirements? [Gap]"

**Performance Requirements Quality：** `performance.md`

样例条目：

- "Are performance requirements quantified with specific metrics? [Clarity]"
- "Are performance targets defined for all critical user journeys? [Coverage]"
- "Are performance requirements under different load conditions specified? [Completeness]"
- "Can performance requirements be objectively measured? [Measurability]"
- "Are degradation requirements defined for high-load scenarios? [Edge Case, Gap]"

**Security Requirements Quality：** `security.md`

样例条目：

- "Are authentication requirements specified for all protected resources? [Coverage]"
- "Are data protection requirements defined for sensitive information? [Completeness]"
- "Is the threat model documented and requirements aligned to it? [Traceability]"
- "Are security requirements consistent with compliance obligations? [Consistency]"
- "Are security failure/breach response requirements defined? [Gap, Exception Flow]"

## 反例：不要这样做

**❌ 错误——这是在测实现，不是测需求：**

```markdown
- [ ] CHK001 - Verify landing page displays 3 episode cards [Spec §FR-001]
- [ ] CHK002 - Test hover states work correctly on desktop [Spec §FR-003]
- [ ] CHK003 - Confirm logo click navigates to home page [Spec §FR-010]
- [ ] CHK004 - Check that related episodes section shows 3-5 items [Spec §FR-005]
```

**✅ 正确——这是在测需求质量：**

```markdown
- [ ] CHK001 - Are the number and layout of featured episodes explicitly specified? [Completeness, Spec §FR-001]
- [ ] CHK002 - Are hover state requirements consistently defined for all interactive elements? [Consistency, Spec §FR-003]
- [ ] CHK003 - Are navigation requirements clear for all clickable brand elements? [Clarity, Spec §FR-010]
- [ ] CHK004 - Is the selection criteria for related episodes documented? [Gap, Spec §FR-005]
- [ ] CHK005 - Are loading state requirements defined for asynchronous episode data? [Gap]
- [ ] CHK006 - Can "visual hierarchy" requirements be objectively measured? [Measurability, Spec §FR-001]
```

**关键差异：**

- 错误：测试系统是否工作正确
- 正确：测试需求是否写得正确
- 错误：验证行为
- 正确：验证需求质量
- 错误："它会不会做 X？"
- 正确："X 是否被清晰地定义了？"
