# 功能规格说明（Feature Specification）：[FEATURE NAME]

**功能分支**：`[###-feature-name]`  
**创建日期**：[DATE]  
**状态**：Draft  
**输入**：用户描述："$ARGUMENTS"

## 用户场景与测试（必填）

<!--
  IMPORTANT: User stories should be PRIORITIZED as user journeys ordered by importance.
  Each user story/journey must be INDEPENDENTLY TESTABLE - meaning if you implement just ONE of them,
  you should still have a viable MVP (Minimum Viable Product) that delivers value.
  
  Assign priorities (P1, P2, P3, etc.) to each story, where P1 is the most critical.
  Think of each story as a standalone slice of functionality that can be:
  - Developed independently
  - Tested independently
  - Deployed independently
  - Demonstrated to users independently
-->

### 用户故事 1 - [简要标题]（优先级：P1）

[Describe this user journey in plain language]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently - e.g., "Can be fully tested by [specific action] and delivers [specific value]"]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]
2. **Given** [initial state], **When** [action], **Then** [expected outcome]

---

### 用户故事 2 - [简要标题]（优先级：P2）

[Describe this user journey in plain language]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]

---

### 用户故事 3 - [简要标题]（优先级：P3）

[Describe this user journey in plain language]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]

---

[Add more user stories as needed, each with an assigned priority]

### 边界情况

<!--
  ACTION REQUIRED: The content in this section represents placeholders.
  Fill them out with the right edge cases.
-->

- What happens when [boundary condition]?
- How does system handle [error scenario]?

## 需求（必填）

<!--
  ACTION REQUIRED: The content in this section represents placeholders.
  Fill them out with the right functional requirements.
-->

### 功能性需求

- **FR-001**：系统 MUST [具体能力，例如“允许用户创建账户”]
- **FR-002**：系统 MUST [具体能力，例如“校验邮箱地址”]  
- **FR-003**：用户 MUST 能够 [关键交互，例如“重置密码”]
- **FR-004**：系统 MUST [数据要求，例如“持久化用户偏好”]
- **FR-005**：系统 MUST [行为要求，例如“记录关键安全事件”]

*Example of marking unclear requirements:*

- **FR-006**: System MUST authenticate users via [NEEDS CLARIFICATION: auth method not specified - email/password, SSO, OAuth?]
- **FR-007**: System MUST retain user data for [NEEDS CLARIFICATION: retention period not specified]

### 关键实体（若功能涉及数据则必填）

- **[Entity 1]**: [What it represents, key attributes without implementation]
- **[Entity 2]**: [What it represents, relationships to other entities]

## 成功标准（必填）

<!--
  ACTION REQUIRED: Define measurable success criteria.
  These must be technology-agnostic and measurable.
-->

### 可衡量结果

- **SC-001**：[可衡量指标，例如“用户在 2 分钟内完成注册”]
- **SC-002**：[可衡量指标，例如“系统在并发场景下无明显性能退化”]
- **SC-003**：[用户体验指标，例如“90% 用户首次即可完成主流程”]
- **SC-004**：[业务指标，例如“与 [X] 相关工单下降 50%”]

## 宪章一致性补充（必填）

为保证与 HeTu 宪章一致，规格说明 MUST 额外声明：

- **代码质量影响**：本需求是否引入新的编码规范、静态检查或文档要求。
- **测试策略**：至少说明将新增/调整哪些单元测试、集成测试，及其可复现验证方式。
- **性能影响**：说明是否触及核心路径（RPC、订阅、事务、批量读写）；若触及，给出基线对比或评估方法。
