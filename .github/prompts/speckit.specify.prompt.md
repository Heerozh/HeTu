---
description: 根据自然语言功能描述创建或更新 feature specification。
handoffs:
  - label: Build Technical Plan
    agent: speckit.plan
    prompt: Create a plan for the spec. I am building with...
  - label: Clarify Spec Requirements
    agent: speckit.clarify
    prompt: Clarify specification requirements
    send: true
---

## 用户输入

```text
$ARGUMENTS
```

在继续之前，你**必须（MUST）**考虑用户输入（如果不为空）。

## 大纲

触发消息里 `/speckit.specify` 后的文本**就是** feature 描述。即使下面 `$ARGUMENTS` 看起来是字面量，也假设你在当前对话中可获得该描述。除非用户提交的是空命令，否则不要要求其重复输入。

基于该 feature 描述，执行：

1. **生成简洁 short name**（2-4 词）用于 branch：
   - 分析 feature 描述并提取最有意义关键词
   - 生成 2-4 词 short name，准确概括 feature 本质
   - 尽量使用动词-名词格式（如 `add-user-auth`、`fix-payment-bug`）
   - 保留技术术语和缩写（OAuth2、API、JWT 等）
   - 保持简短但可读性足够
   - 示例：
     - "I want to add user authentication" → `user-auth`
     - "Implement OAuth2 integration for the API" → `oauth2-api-integration`
     - "Create a dashboard for analytics" → `analytics-dashboard`
     - "Fix payment processing timeout bug" → `fix-payment-timeout`

2. **创建新 branch 前检查已有分支**：

   a. 先拉取所有远端分支，确保信息最新：

   ```bash
   git fetch --all --prune
   ```

   b. 跨全部来源查找该 short-name 的最大 feature 编号：
   - 远端分支：`git ls-remote --heads origin | grep -E 'refs/heads/[0-9]+-<short-name>$'`
   - 本地分支：`git branch | grep -E '^[* ]*[0-9]+-<short-name>$'`
   - specs 目录：匹配 `specs/[0-9]+-<short-name>`

   c. 计算下一个可用编号：
   - 从三个来源提取全部编号
   - 取最大值 N
   - 新编号使用 N+1

   d. 运行脚本 `.specify/scripts/powershell/create-new-feature.ps1 -Json "$ARGUMENTS"`，并传入计算得到的编号与 short-name：
   - 传递 `--number N+1` 与 `--short-name "your-short-name"` 以及 feature 描述
   - Bash 示例：`.specify/scripts/powershell/create-new-feature.ps1 -Json "$ARGUMENTS" --json --number 5 --short-name "user-auth" "Add user authentication"`
   - PowerShell 示例：`.specify/scripts/powershell/create-new-feature.ps1 -Json "$ARGUMENTS" -Json -Number 5 -ShortName "user-auth" "Add user authentication"`

   **重要（IMPORTANT）**：
   - 必须检查三类来源（remote branches、local branches、specs directories）
   - 只匹配该 short-name 的精确模式
   - 若三处均无匹配，则从 1 开始
   - 每个 feature 该脚本只运行一次
   - 终端会输出 JSON，务必以其内容为准
   - JSON 输出包含 `BRANCH_NAME` 与 `SPEC_FILE`
   - 对类似 "I'm Groot" 的参数单引号，使用转义语法：如 'I'\''m Groot'（或尽量使用双引号："I'm Groot"）

3. 加载 `.specify/templates/spec-template.md`，理解必需章节结构。

4. 按以下流程执行：
   1. 从输入解析用户描述
      若为空：`ERROR "No feature description provided"`
   2. 提取关键概念
      识别：actors、actions、data、constraints
   3. 对不清晰项：
      - 基于上下文与行业惯例做合理推断
      - 仅在以下情况下用 `[NEEDS CLARIFICATION: specific question]` 标注：
        - 该选择显著影响 feature scope 或 user experience
        - 存在多种合理解释且影响不同
        - 不存在合理默认值
      - **限制：最多 3 个 `[NEEDS CLARIFICATION]`**
      - 按影响优先级：scope > security/privacy > user experience > technical details
   4. 填写 User Scenarios & Testing 章节
      若无法明确用户流：`ERROR "Cannot determine user scenarios"`
   5. 生成功能需求（Functional Requirements）
      每条需求必须可测试
      对未指定细节采用合理默认，并在 Assumptions 章节记录
   6. 定义 Success Criteria
      产出可度量、技术无关的结果
      同时包含量化指标（时间、性能、容量）与定性指标（满意度、完成率）
      每条标准必须不依赖实现细节即可验证
   7. 识别 Key Entities（若涉及数据）
   8. 返回：`SUCCESS`（spec 可进入 planning）

5. 按模板结构将 specification 写入 `SPEC_FILE`：用 feature 描述（arguments）推导出的具体内容替换占位符，保持章节顺序与标题不变。

6. **Specification 质量校验**：初稿写入后，按质量标准校验：

   a. **创建 Spec 质量 Checklist**：在 `FEATURE_DIR/checklists/requirements.md` 生成 checklist（使用 checklist 模板结构），包含以下项：

   ```markdown
   # 规格质量检查清单：[FEATURE NAME]

   **Purpose**: 在进入规划前，校验规格的完整性与质量
   **Created**: [DATE]
   **Feature**: [链接到 spec.md]

   ## 内容质量

   - [ ] 不包含实现细节（语言、框架、API）
   - [ ] 聚焦用户价值与业务需求
   - [ ] 面向非技术干系人书写
   - [ ] 所有必填章节均已完成

   ## 需求完整性

   - [ ] 不存在 [NEEDS CLARIFICATION] 标记
   - [ ] 需求可测试且无歧义
   - [ ] 成功标准可度量
   - [ ] 成功标准与技术实现无关（无实现细节）
   - [ ] 所有验收场景均已定义
   - [ ] 已识别边界情况
   - [ ] 范围边界清晰
   - [ ] 依赖与假设已识别

   ## 功能就绪性

   - [ ] 所有功能性需求均有清晰验收标准
   - [ ] 用户场景覆盖主流程
   - [ ] 功能满足成功标准中定义的可度量结果
   - [ ] 规格中未泄露实现细节

   ## 备注

   - 标记为未完成的项，必须先更新 spec，之后才能执行 `/speckit.clarify` 或 `/speckit.plan`
   ```

   b. **运行校验检查**：逐项检查 spec：
   - 判定每项 pass/fail
   - 记录具体问题（引用相应 spec 片段）

   c. **处理校验结果**：
   - **若全部通过**：标记 checklist 完成并进入步骤 6

   - **若存在失败项（不含 `[NEEDS CLARIFICATION]`）**：
     1. 列出失败项与具体问题
     2. 更新 spec 修复问题
     3. 重新校验直至全部通过（最多 3 轮）
     4. 若 3 轮后仍失败，在 checklist notes 中记录剩余问题并提示用户

   - **若仍有 `[NEEDS CLARIFICATION]`**：
     1. 提取 spec 中所有 `[NEEDS CLARIFICATION: ...]`
     2. **上限检查**：若超过 3 个，仅保留最关键 3 个（按 scope/security/UX 影响），其余用合理默认推断
     3. 对每个澄清项（最多 3），按以下格式向用户提供选项：

        ```markdown
        ## 问题 [N]：[主题]

        **Context**: [引用相关 spec 片段]

        **What we need to know**: [来自 NEEDS CLARIFICATION 标记的具体问题]

        **Suggested Answers**:

        | Option | Answer           | Implications             |
        | ------ | ---------------- | ------------------------ |
        | A      | [第一个建议答案] | [对该功能意味着什么]     |
        | B      | [第二个建议答案] | [对该功能意味着什么]     |
        | C      | [第三个建议答案] | [对该功能意味着什么]     |
        | Custom | 提供你自己的答案 | [说明如何提供自定义输入] |

        **Your choice**: _[等待用户回复]_
        ```

     4. **关键（CRITICAL）- 表格格式**：确保 Markdown 表格正确渲染：
        - 使用一致的管道与间距
        - 单元格内容两侧留空格：`| Content |`，不要 `|Content|`
        - 表头分隔至少 3 个短横线：`|--------|`
        - 在 markdown 预览中检查渲染
     5. 问题按顺序编号（Q1、Q2、Q3，最多 3 个）
     6. 一次性展示全部问题后再等待回复
     7. 等待用户一次性回复所有问题（如："Q1: A, Q2: Custom - [details], Q3: B"）
     8. 用用户选择/输入替换对应 `[NEEDS CLARIFICATION]`
     9. 澄清完成后重新执行质量校验

   d. **更新 Checklist**：每轮校验后，更新 checklist 文件中的当前 pass/fail 状态

7. 输出完成报告：branch 名、spec 文件路径、checklist 结果，以及是否可进入下一阶段（`/speckit.clarify` 或 `/speckit.plan`）。

**注意（NOTE）**：脚本会先创建并切换到新 branch，再初始化 spec 文件，然后才写入内容。

## General Guidelines

## 快速指南（Quick Guidelines）

- 聚焦用户**需要什么（WHAT）**和**为什么（WHY）**。
- 避免 HOW（不写 tech stack、API、代码结构）。
- 面向业务相关方，不是开发实现文档。
- 不要在 spec 内嵌 checklist（checklist 由独立命令生成）。

### Section 要求

- **Mandatory sections**：每个 feature 都必须完成
- **Optional sections**：仅在相关时包含
- 不适用章节请直接删除（不要写 "N/A"）

### AI 生成规范

当基于用户 prompt 生成 spec：

1. **做有依据的推断**：基于上下文、行业惯例、通用模式补全信息
2. **记录假设**：在 Assumptions 章节写明合理默认
3. **限制澄清数量**：最多 3 个 `[NEEDS CLARIFICATION]`，仅用于关键决策：
   - 显著影响 feature scope 或 user experience
   - 存在多种合理解释且结果差异大
   - 不存在合理默认值
4. **澄清优先级**：scope > security/privacy > user experience > technical details
5. **以测试者视角审视**：任何模糊需求都应触发“可测试与无歧义”失败
6. **常见需澄清区域**（仅在无合理默认时）：
   - 功能范围与边界（包含/排除用例）
   - 用户类型与权限（存在冲突解释时）
   - 安全/合规要求（涉及法律/财务重大风险时）

**合理默认示例（通常无需追问）**：

- 数据保留：采用该领域常见标准
- 性能目标：默认 Web/Mobile 常规体验预期
- 错误处理：用户友好提示 + 合理兜底
- 认证方式：Web 场景默认 session 或 OAuth2
- 集成模式：默认 RESTful APIs

### Success Criteria 指南

Success criteria 必须满足：

1. **可度量（Measurable）**：包含具体指标（时间、百分比、数量、比率）
2. **技术无关（Technology-agnostic）**：不提 framework、language、database、tool
3. **用户导向（User-focused）**：描述用户/业务结果，而非系统内部细节
4. **可验证（Verifiable）**：不依赖实现细节即可测试/验收

**好例子**：

- "Users can complete checkout in under 3 minutes"
- "System supports 10,000 concurrent users"
- "95% of searches return results in under 1 second"
- "Task completion rate improves by 40%"

**坏例子（实现导向）**：

- "API response time is under 200ms"（太技术化，应改为用户可感知指标）
- "Database can handle 1000 TPS"（实现细节）
- "React components render efficiently"（框架特定）
- "Redis cache hit rate above 80%"（技术特定）
