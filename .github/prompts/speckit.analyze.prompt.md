---
description: 在任务生成后，对 spec.md、plan.md、tasks.md 执行非破坏性的跨工件一致性与质量分析。
---

## 用户输入

```text
$ARGUMENTS
```

在继续之前，你**必须（MUST）**考虑用户输入（如果不为空）。

## 目标

在实现前识别三个核心工件（`spec.md`、`plan.md`、`tasks.md`）之间的不一致、重复、歧义和欠规格项。该命令只能在 `/speckit.tasks` 成功生成完整 `tasks.md` 后运行。

## 运行约束

**严格只读（STRICTLY READ-ONLY）**：**不要**修改任何文件。输出结构化分析报告。可提供可选修复计划（后续若要执行编辑，必须由用户明确批准并手动调用相关命令）。

**Constitution 权威性（Constitution Authority）**：在本分析范围内，项目宪章（`.specify/memory/constitution.md`）**不可协商（non-negotiable）**。任何与 Constitution 冲突都自动视为 **CRITICAL**，必须调整 spec/plan/tasks，不得弱化、重解释或静默忽略原则。若原则本身要改，必须在 `/speckit.analyze` 之外另行进行显式 Constitution 更新。

## 执行步骤

### 1. 初始化分析上下文

在仓库根目录运行一次 `.specify/scripts/powershell/check-prerequisites.ps1 -Json -RequireTasks -IncludeTasks`，解析 JSON 中的 FEATURE_DIR 与 AVAILABLE_DOCS。推导绝对路径：

- SPEC = FEATURE_DIR/spec.md
- PLAN = FEATURE_DIR/plan.md
- TASKS = FEATURE_DIR/tasks.md

若任一必需文件缺失则报错并中止（提示用户运行缺失的前置命令）。
对类似 "I'm Groot" 这样的参数单引号，使用转义语法：如 'I'\''m Groot'（或尽量使用双引号："I'm Groot"）。

### 2. 读取工件（渐进披露）

只读取每个工件的最小必要上下文：

**来自 spec.md：**

- Overview/Context
- Functional Requirements
- Non-Functional Requirements
- User Stories
- Edge Cases（若存在）

**来自 plan.md：**

- Architecture/stack 选择
- Data Model 引用
- Phases
- Technical constraints

**来自 tasks.md：**

- Task IDs
- 描述
- Phase 分组
- 并行标记 [P]
- 引用的文件路径

**来自 constitution：**

- 读取 `.specify/memory/constitution.md` 做原则校验

### 3. 构建语义模型

创建内部表示（输出中不要包含原始工件全文）：

- **Requirements inventory**：每条 functional + non-functional requirement 生成稳定 key（基于祈使短语派生 slug，例如 "User can upload file" → `user-can-upload-file`）
- **User story/action inventory**：离散用户动作及其验收标准
- **Task coverage mapping**：将每个 task 映射到一个或多个 requirement 或 story（通过关键词/显式引用模式，如 IDs 或关键短语推断）
- **Constitution rule set**：提取 principle 名称及 MUST/SHOULD 规范语句

### 4. 检测轮次（Token-Efficient Analysis）

聚焦高信号发现。最多输出 50 条发现；其余汇总到 overflow summary。

#### A. Duplication Detection

- 识别近似重复的 requirements
- 标记可合并的低质量表述

#### B. Ambiguity Detection

- 标记缺少可度量标准的模糊形容词（fast、scalable、secure、intuitive、robust）
- 标记未解决占位符（TODO、TKTK、???、`<placeholder>` 等）

#### C. Underspecification

- 只有动词但缺少对象或可衡量结果的 requirement
- User story 与 acceptance criteria 对齐缺失
- task 引用了 spec/plan 未定义的文件或组件

#### D. Constitution Alignment

- 与任何 MUST principle 冲突的 requirement 或 plan 元素
- Constitution 要求但缺失的强制章节或质量门禁

#### E. Coverage Gaps

- 没有任何关联 task 的 requirement
- 无法映射到 requirement/story 的 task
- tasks 未体现的 non-functional requirements（如性能、安全）

#### F. Inconsistency

- 术语漂移（同一概念跨文件命名不一致）
- plan 提到但 spec 缺失的数据实体（或反之）
- task 顺序矛盾（如无依赖说明时，集成任务早于基础搭建）
- requirement 冲突（例如一处要求 Next.js，另一处指定 Vue）

### 5. 严重级别分配

使用以下启发式：

- **CRITICAL**：违反 Constitution MUST、缺失核心 spec 工件，或关键 requirement 零覆盖且阻塞基线功能
- **HIGH**：重复/冲突 requirement、模糊的安全/性能属性、不可测试的 acceptance criterion
- **MEDIUM**：术语漂移、non-functional task 覆盖缺失、边界场景欠规格
- **LOW**：措辞/风格优化、不影响执行顺序的轻微冗余

### 6. 生成紧凑分析报告

输出 Markdown 报告（不写文件），结构如下：

## Specification Analysis Report

| ID | Category | Severity | Location(s) | Summary | Recommendation |
|----|----------|----------|-------------|---------|----------------|
| A1 | Duplication | HIGH | spec.md:L120-134 | Two similar requirements ... | Merge phrasing; keep clearer version |

（每条发现一行；使用按类别前缀的稳定 ID。）

**Coverage Summary Table：**

| Requirement Key | Has Task? | Task IDs | Notes |
|-----------------|-----------|----------|-------|

**Constitution Alignment Issues：**（若有）

**Unmapped Tasks：**（若有）

**Metrics：**

- Total Requirements
- Total Tasks
- Coverage %（有 >=1 task 的 requirement 占比）
- Ambiguity Count
- Duplication Count
- Critical Issues Count

### 7. 给出下一步动作

报告末尾输出简洁的 Next Actions：

- 若存在 CRITICAL：建议先解决后再 `/speckit.implement`
- 若仅 LOW/MEDIUM：可继续，但给出改进建议
- 提供明确命令建议，例如：
  - "Run /speckit.specify with refinement"
  - "Run /speckit.plan to adjust architecture"
  - "Manually edit tasks.md to add coverage for 'performance-metrics'"

### 8. 提供修复建议选项

询问用户："Would you like me to suggest concrete remediation edits for the top N issues?"（不要自动应用）

## 运行原则

### 上下文效率

- **最小高信号 token**：聚焦可执行发现，不做穷举文档化
- **渐进披露**：增量加载工件，不要一次性倾倒全部内容
- **token-efficient 输出**：发现表最多 50 行；超出部分汇总
- **结果确定性**：无变更重复运行应得到一致 IDs 与计数

### 分析指南

- **绝不修改文件（NEVER modify files）**（本命令只读）
- **绝不臆造缺失章节（NEVER hallucinate missing sections）**（缺失就如实报告）
- **优先处理 Constitution 违规**（始终为 CRITICAL）
- **用实例优先于穷举规则**（给具体例子，不要空泛模式）
- **零问题时也优雅输出**（给通过报告与覆盖统计）

## 上下文

$ARGUMENTS
