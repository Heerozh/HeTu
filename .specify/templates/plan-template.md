# 实施计划（Implementation Plan）：[FEATURE]

**分支**：`[###-feature-name]` | **日期**：[DATE] | **Spec**：[link]
**输入**：来自 `/specs/[###-feature-name]/spec.md` 的功能规格说明

**说明**：该模板通常由 `/speckit.plan` 命令填充。（若命令模板目录缺失，请以项目脚本/文档为准。）

## 摘要

[从 spec 提取：核心需求 + 研究结论中的技术方案]

## 技术上下文

<!--
  ACTION REQUIRED: Replace the content in this section with the technical details
  for the project. The structure here is presented in advisory capacity to guide
  the iteration process.
-->

**语言/版本**：[例如 Python 3.x 或 NEEDS CLARIFICATION]  
**主要依赖**：[例如 Sanic、Redis client、NumPy 或 NEEDS CLARIFICATION]  
**存储**：[例如 Redis 或 N/A]  
**测试**：[例如 pytest 或 NEEDS CLARIFICATION]  
**目标平台**：[例如 Linux server / Windows dev / Docker 或 NEEDS CLARIFICATION]
**项目类型**：[single/web/mobile - 决定源码结构]  
**性能目标**：[领域目标；若无硬阈值，写“不得低于现有基线”]  
**约束**：[例如 p95 延迟、内存上限、可扩缩容等或 NEEDS CLARIFICATION]  
**规模/范围**：[例如并发连接数、实例数量或 NEEDS CLARIFICATION]

## 宪章检查（Constitution Check）

*门禁（GATE）：Phase 0 研究前必须通过；Phase 1 设计后需复核。*

对照 `.specify/memory/constitution.md`（HeTu 宪章）逐条核对：

- 代码质量：变更计划中是否包含 lint/format/type-check 的执行与修复策略？
- 测试标准：是否定义了可复现验证与对应测试（缺陷先补测试、核心路径优先集成测试）？
- 性能保障：是否评估核心路径的性能影响，并在需要时提供基准/对比方法？

## 项目结构

### 文档（本功能）

```text
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### 源码（仓库根目录）
<!--
  ACTION REQUIRED: Replace the placeholder tree below with the concrete layout
  for this feature. Delete unused options and expand the chosen structure with
  real paths (e.g., apps/admin, packages/something). The delivered plan must
  not include Option labels.
-->

```text
# [REMOVE IF UNUSED] Option 1: Single project (DEFAULT)
src/
├── models/
├── services/
├── cli/
└── lib/

tests/
├── contract/
├── integration/
└── unit/

# [REMOVE IF UNUSED] Option 2: Web application (when "frontend" + "backend" detected)
backend/
├── src/
│   ├── models/
│   ├── services/
│   └── api/
└── tests/

frontend/
├── src/
│   ├── components/
│   ├── pages/
│   └── services/
└── tests/

# [REMOVE IF UNUSED] Option 3: Mobile + API (when "iOS/Android" detected)
api/
└── [same as backend above]

ios/ or android/
└── [platform-specific structure: feature modules, UI flows, platform tests]
```

**结构选择**：[记录所选结构，并引用上方真实目录]

## 复杂度追踪

> **仅当“宪章检查”存在必须豁免/延期的违规项时填写**

| 违规项 | 必要性 | 被拒绝的更简单替代方案与原因 |
|-----------|------------|-------------------------------------|
| [e.g., 4th project] | [current need] | [why 3 projects insufficient] |
| [e.g., Repository pattern] | [specific problem] | [why direct DB access insufficient] |
