---

description: "功能实现任务清单模板"
---

# 任务清单（Tasks）：[FEATURE NAME]

**输入**：`/specs/[###-feature-name]/` 下的设计文档
**前置条件**：plan.md（必需）、spec.md（用户故事必需）、research.md、data-model.md、contracts/

**测试说明**：下方示例包含测试任务。是否生成测试任务由功能规格决定；若需求涉及行为变更或缺陷修复，MUST 包含对应测试任务。

**组织方式**：任务按用户故事分组，确保每个故事可独立实现与验证。

## 格式：`[ID] [P?] [Story] 描述`

- **[P]**：可并行执行（不同文件、无依赖）
- **[Story]**：任务所属用户故事（如 US1、US2、US3）
- 描述中必须包含准确文件路径

## 路径约定

- **Single project**: `src/`, `tests/` at repository root
- **Web app**: `backend/src/`, `frontend/src/`
- **Mobile**: `api/src/`, `ios/src/` or `android/src/`
- Paths shown below assume single project - adjust based on plan.md structure

<!-- 
  ============================================================================
  IMPORTANT: The tasks below are SAMPLE TASKS for illustration purposes only.
  
  The /speckit.tasks command MUST replace these with actual tasks based on:
  - User stories from spec.md (with their priorities P1, P2, P3...)
  - Feature requirements from plan.md
  - Entities from data-model.md
  - Endpoints from contracts/
  
  Tasks MUST be organized by user story so each story can be:
  - Implemented independently
  - Tested independently
  - Delivered as an MVP increment
  
  DO NOT keep these sample tasks in the generated tasks.md file.
  ============================================================================
-->

## Phase 1：准备阶段（共享基础设施）

**目的**：项目初始化与基础结构准备

- [ ] T001 按实施计划创建项目结构
- [ ] T002 初始化 [language] 项目并安装 [framework] 依赖
- [ ] T003 [P] 配置 lint/format/type-check 工具链

---

## Phase 2：基础阶段（阻塞性前置）

**目的**：任何用户故事开始前 MUST 完成的核心基础设施

**⚠️ 关键**：本阶段完成前不得开始任何用户故事开发

Examples of foundational tasks (adjust based on your project):

- [ ] T004 Setup database schema and migrations framework
- [ ] T005 [P] Implement authentication/authorization framework
- [ ] T006 [P] Setup API routing and middleware structure
- [ ] T007 Create base models/entities that all stories depend on
- [ ] T008 Configure error handling and logging infrastructure
- [ ] T009 Setup environment configuration management

**检查点**：基础设施就绪，可并行推进用户故事

---

## Phase 3：用户故事 1 - [标题]（优先级：P1）🎯 MVP

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### 用户故事 1 的测试任务（按规格要求生成）⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T010 [P] [US1] 为 [endpoint] 编写契约测试：`tests/contract/test_[name].py`
- [ ] T011 [P] [US1] 为 [用户旅程] 编写集成测试：`tests/integration/test_[name].py`

### 用户故事 1 的实现任务

- [ ] T012 [P] [US1] Create [Entity1] model in src/models/[entity1].py
- [ ] T013 [P] [US1] Create [Entity2] model in src/models/[entity2].py
- [ ] T014 [US1] Implement [Service] in src/services/[service].py (depends on T012, T013)
- [ ] T015 [US1] Implement [endpoint/feature] in src/[location]/[file].py
- [ ] T016 [US1] Add validation and error handling
- [ ] T017 [US1] 为用户故事 1 添加日志与可观测性信息
- [ ] T018 [US1] 执行并记录代码质量门禁（ruff/basedpyright）
- [ ] T019 [US1] 评估性能影响（核心路径变更时提供基线对比）

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4：用户故事 2 - [标题]（优先级：P2）

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 2 (OPTIONAL - only if tests requested) ⚠️

- [ ] T018 [P] [US2] Contract test for [endpoint] in tests/contract/test_[name].py
- [ ] T019 [P] [US2] Integration test for [user journey] in tests/integration/test_[name].py

### Implementation for User Story 2

- [ ] T020 [P] [US2] Create [Entity] model in src/models/[entity].py
- [ ] T021 [US2] Implement [Service] in src/services/[service].py
- [ ] T022 [US2] Implement [endpoint/feature] in src/[location]/[file].py
- [ ] T023 [US2] Integrate with User Story 1 components (if needed)

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5：用户故事 3 - [标题]（优先级：P3）

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 3 (OPTIONAL - only if tests requested) ⚠️

- [ ] T024 [P] [US3] Contract test for [endpoint] in tests/contract/test_[name].py
- [ ] T025 [P] [US3] Integration test for [user journey] in tests/integration/test_[name].py

### Implementation for User Story 3

- [ ] T026 [P] [US3] Create [Entity] model in src/models/[entity].py
- [ ] T027 [US3] Implement [Service] in src/services/[service].py
- [ ] T028 [US3] Implement [endpoint/feature] in src/[location]/[file].py

**Checkpoint**: All user stories should now be independently functional

---

[Add more user story phases as needed, following the same pattern]

---

## Phase N：收尾与横切关注点

**Purpose**: Improvements that affect multiple user stories

- [ ] TXXX [P] 更新文档（`docs/`）
- [ ] TXXX 代码清理与重构
- [ ] TXXX 跨故事性能优化与回归对比
- [ ] TXXX [P] 补充单元测试（如规格要求）
- [ ] TXXX 安全加固
- [ ] TXXX 验证 `quickstart.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Final Phase)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - May integrate with US1 but should be independently testable
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - May integrate with US1/US2 but should be independently testable

### Within Each User Story

- Tests (if included) MUST be written and FAIL before implementation
- Models before services
- Services before endpoints
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- All tests for a user story marked [P] can run in parallel
- Models within a story marked [P] can run in parallel
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Launch all tests for User Story 1 together (if tests requested):
Task: "Contract test for [endpoint] in tests/contract/test_[name].py"
Task: "Integration test for [user journey] in tests/integration/test_[name].py"

# Launch all models for User Story 1 together:
Task: "Create [Entity1] model in src/models/[entity1].py"
Task: "Create [Entity2] model in src/models/[entity2].py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test User Story 1 independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 → Test independently → Deploy/Demo
4. Add User Story 3 → Test independently → Deploy/Demo
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1
   - Developer B: User Story 2
   - Developer C: User Story 3
3. Stories complete and integrate independently

---

## Notes

- [P] 任务 = 不同文件、无依赖，可并行执行
- [Story] 标签用于任务到用户故事的可追溯映射
- 每个用户故事应可独立完成并独立验证
- 实现前先验证测试可失败（若采用 TDD）
- 每个任务或逻辑分组完成后提交
- 在每个检查点进行独立验收
- 避免：模糊任务、同文件冲突、破坏独立性的跨故事依赖

### 宪章驱动任务类型（必覆盖）

生成任务时应显式覆盖以下类型：

1. **代码质量任务**：格式化、静态检查、类型检查与文档一致性。
2. **测试任务**：缺陷复现测试、行为回归测试、核心链路集成测试。
3. **性能任务**：核心路径性能评估、基线对比、性能退化防护。
