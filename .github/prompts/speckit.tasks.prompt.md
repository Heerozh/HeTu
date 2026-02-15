---
description: 基于可用设计工件，为该 feature 生成可执行、按依赖排序的 tasks.md。
handoffs:
  - label: Analyze For Consistency
    agent: speckit.analyze
    prompt: Run a project analysis for consistency
    send: true
  - label: Implement Project
    agent: speckit.implement
    prompt: Start the implementation in phases
    send: true
---

## 用户输入

```text
$ARGUMENTS
```

在继续之前，你**必须（MUST）**考虑用户输入（如果不为空）。

## 大纲

1. **Setup**：在仓库根目录运行 `.specify/scripts/powershell/check-prerequisites.ps1 -Json`，解析 FEATURE_DIR 与 AVAILABLE_DOCS。所有路径必须是绝对路径。对于类似 "I'm Groot" 的参数单引号，使用转义语法：如 'I'\''m Groot'（或尽量使用双引号："I'm Groot"）。

2. **加载设计文档**：从 FEATURE_DIR 读取：
   - **Required**：`plan.md`（tech stack、libraries、结构）、`spec.md`（带优先级的 user stories）
   - **Optional**：`data-model.md`（entities）、`contracts/`（API endpoints）、`research.md`（决策）、`quickstart.md`（测试场景）
   - 注意：不是所有项目都有全部文档。请基于可用文档生成任务。

3. **执行任务生成 workflow**：
   - 读取 `plan.md` 并提取 tech stack、libraries、项目结构
   - 读取 `spec.md` 并提取 user stories 与优先级（P1、P2、P3...）
   - 若存在 `data-model.md`：提取 entities 并映射到 user stories
   - 若存在 `contracts/`：将 endpoints 映射到 user stories
   - 若存在 `research.md`：提取决策用于 setup 任务
   - 按 user story 组织生成任务（见下方 Task Generation Rules）
   - 生成 user story 完成顺序的 dependency graph
   - 为每个 user story 生成并行执行示例
   - 验证任务完整性（每个 story 任务完整且可独立测试）

4. **生成 tasks.md**：以 `.specify/templates/tasks-template.md` 为结构，填充：
   - 来自 `plan.md` 的正确 feature 名称
   - Phase 1：Setup 任务（项目初始化）
   - Phase 2：Foundational 任务（所有 user story 的阻塞前置）
   - Phase 3+：按优先级一个 user story 一个 phase
   - 每个 phase 包含：story 目标、独立测试标准、测试任务（如要求）、实现任务
   - Final Phase：Polish 与 cross-cutting concerns
   - 所有任务必须符合严格 checklist 格式（见下方规则）
   - 每个任务带明确 file path
   - Dependencies 章节展示 story 完成顺序
   - 每个 story 给出 parallel execution 示例
   - Implementation strategy（MVP first，增量交付）

5. **报告**：输出生成的 tasks.md 路径与摘要：
   - 任务总数
   - 每个 user story 的任务数
   - 识别出的并行机会
   - 每个 story 的独立测试标准
   - 建议 MVP 范围（通常仅 User Story 1）
   - 格式校验：确认所有任务均符合 checklist 格式（checkbox、ID、labels、file paths）

用于任务生成的上下文：$ARGUMENTS

生成的 tasks.md 应可直接执行——每个任务都要具体到 LLM 无需额外上下文即可完成。

## Task Generation Rules

**CRITICAL**：任务必须按 user story 组织，以支持独立实现与测试。

**Tests 是可选项**：仅在 feature spec 明确要求，或用户要求 TDD 时生成测试任务。

### Checklist 格式（必须）

每个任务必须严格使用：

```text
- [ ] [TaskID] [P?] [Story?] Description with file path
```

**格式组成**：

1. **Checkbox**：始终以 `- [ ]` 开头
2. **Task ID**：按执行顺序递增（T001、T002、T003...）
3. **[P] 标记**：仅当任务可并行时添加（不同文件、且不依赖未完成任务）
4. **[Story] 标签**：仅 user story phase 任务必须有
   - 格式：[US1]、[US2]、[US3]...（映射 spec.md 的 user stories）
   - Setup phase：无 story 标签
   - Foundational phase：无 story 标签
   - User Story phases：必须有 story 标签
   - Polish phase：无 story 标签
5. **Description**：清晰动作 + 精确 file path

**示例**：

- ✅ 正确：`- [ ] T001 Create project structure per implementation plan`
- ✅ 正确：`- [ ] T005 [P] Implement authentication middleware in src/middleware/auth.py`
- ✅ 正确：`- [ ] T012 [P] [US1] Create User model in src/models/user.py`
- ✅ 正确：`- [ ] T014 [US1] Implement UserService in src/services/user_service.py`
- ❌ 错误：`- [ ] Create User model`（缺少 ID 和 Story 标签）
- ❌ 错误：`T001 [US1] Create model`（缺少 checkbox）
- ❌ 错误：`- [ ] [US1] Create User model`（缺少 Task ID）
- ❌ 错误：`- [ ] T001 [US1] Create model`（缺少 file path）

### 任务组织方式

1. **来自 User Stories（spec.md）**——主要组织轴：
   - 每个 user story（P1、P2、P3...）对应一个 phase
   - 将相关组件映射到对应 story：
     - 该 story 所需模型
     - 该 story 所需服务
     - 该 story 所需 endpoint/UI
     - 若要求测试：该 story 的测试任务
   - 标注 story 依赖关系（大多数 story 应可独立）

2. **来自 Contracts**：
   - 每个 contract/endpoint 映射到其服务的 user story
   - 若要求测试：每个 contract 在该 story phase 先放 contract test 任务 [P]，再放实现任务

3. **来自 Data Model**：
   - 将每个 entity 映射到需要它的 user story
   - 若实体服务多个 story：放入最早 story 或 Setup phase
   - 关系相关任务放到对应 story 的 service 层

4. **来自 Setup/Infrastructure**：
   - 共享基础设施 → Setup phase（Phase 1）
   - 阻塞性前置 → Foundational phase（Phase 2）
   - story 专属 setup → 放入对应 story phase

### Phase 结构

- **Phase 1**：Setup（项目初始化）
- **Phase 2**：Foundational（阻塞性前置，必须先完成）
- **Phase 3+**：按优先级的 User Stories（P1、P2、P3...）
  - 每个 story 内顺序：Tests（如要求）→ Models → Services → Endpoints → Integration
  - 每个 phase 应是完整且可独立测试的增量
- **Final Phase**：Polish & Cross-Cutting Concerns
