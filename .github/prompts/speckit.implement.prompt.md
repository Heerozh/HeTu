---
description: 执行 implementation plan，处理并落实 tasks.md 中定义的全部任务。
---

## 用户输入

```text
$ARGUMENTS
```

在继续之前，你**必须（MUST）**考虑用户输入（如果不为空）。

## 大纲

1. 在仓库根目录运行 `.specify/scripts/powershell/check-prerequisites.ps1 -Json -RequireTasks -IncludeTasks`，解析 FEATURE_DIR 与 AVAILABLE_DOCS。所有路径必须是绝对路径。对于类似 "I'm Groot" 的参数单引号，使用转义语法：如 'I'\''m Groot'（或尽量使用双引号："I'm Groot"）。

2. **检查 checklist 状态**（若存在 `FEATURE_DIR/checklists/`）：
   - 扫描 checklists/ 目录下所有 checklist 文件
   - 对每个 checklist 统计：
     - Total items：匹配 `- [ ]`、`- [X]`、`- [x]` 的所有行
     - Completed items：匹配 `- [X]`、`- [x]` 的行
     - Incomplete items：匹配 `- [ ]` 的行
   - 生成状态表：

     ```text
     | Checklist | Total | Completed | Incomplete | Status |
     |-----------|-------|-----------|------------|--------|
     | ux.md     | 12    | 12        | 0          | ✓ PASS |
     | test.md   | 8     | 5         | 3          | ✗ FAIL |
     | security.md | 6   | 6         | 0          | ✓ PASS |
     ```

   - 计算总体状态：
     - **PASS**：所有 checklist 的 incomplete = 0
     - **FAIL**：至少一个 checklist 有 incomplete

   - **若存在未完成 checklist**：
     - 展示表格与未完成项计数
     - **停止**并询问："Some checklists are incomplete. Do you want to proceed with implementation anyway? (yes/no)"
     - 等待用户回复后再继续
     - 若用户回复 "no"、"wait"、"stop"，则终止执行
     - 若用户回复 "yes"、"proceed"、"continue"，则进入步骤 3

   - **若全部 checklist 完成**：
     - 展示全部通过的表格
     - 自动进入步骤 3

3. 加载并分析实现上下文：
   - **REQUIRED**：读取 tasks.md（完整任务列表与执行计划）
   - **REQUIRED**：读取 plan.md（技术栈、架构、文件结构）
   - **IF EXISTS**：读取 data-model.md（实体与关系）
   - **IF EXISTS**：读取 contracts/（API 规格与测试要求）
   - **IF EXISTS**：读取 research.md（技术决策与约束）
   - **IF EXISTS**：读取 quickstart.md（集成场景）

4. **项目设置验证（Project Setup Verification）**：
   - **REQUIRED**：基于实际项目创建/校验 ignore 文件：

   **检测与创建逻辑**：
   - 先运行以下命令判断是否为 git 仓库（若是则创建/校验 `.gitignore`）：

     ```sh
     git rev-parse --git-dir 2>/dev/null
     ```

   - 若存在 Dockerfile* 或 plan.md 提到 Docker → 创建/校验 `.dockerignore`
   - 若存在 `.eslintrc*` → 创建/校验 `.eslintignore`
   - 若存在 `eslint.config.*` → 确保配置中的 `ignores` 包含必要模式
   - 若存在 `.prettierrc*` → 创建/校验 `.prettierignore`
   - 若存在 `.npmrc` 或 `package.json` → 创建/校验 `.npmignore`（若需要发布）
   - 若存在 Terraform 文件（`*.tf`）→ 创建/校验 `.terraformignore`
   - 若存在 Helm chart → 创建/校验 `.helmignore`

   **若 ignore 文件已存在**：校验是否包含关键模式，仅追加缺失的关键项
   **若 ignore 文件缺失**：按检测到技术栈创建完整模式集合

   **按技术栈的通用模式**（来自 plan.md）：
   - **Node.js/JavaScript/TypeScript**：`node_modules/`、`dist/`、`build/`、`*.log`、`.env*`
   - **Python**：`__pycache__/`、`*.pyc`、`.venv/`、`venv/`、`dist/`、`*.egg-info/`
   - **Java**：`target/`、`*.class`、`*.jar`、`.gradle/`、`build/`
   - **C#/.NET**：`bin/`、`obj/`、`*.user`、`*.suo`、`packages/`
   - **Go**：`*.exe`、`*.test`、`vendor/`、`*.out`
   - **Ruby**：`.bundle/`、`log/`、`tmp/`、`*.gem`、`vendor/bundle/`
   - **PHP**：`vendor/`、`*.log`、`*.cache`、`*.env`
   - **Rust**：`target/`、`debug/`、`release/`、`*.rs.bk`、`*.rlib`、`*.prof*`、`.idea/`、`*.log`、`.env*`
   - **Kotlin**：`build/`、`out/`、`.gradle/`、`.idea/`、`*.class`、`*.jar`、`*.iml`、`*.log`、`.env*`
   - **C++**：`build/`、`bin/`、`obj/`、`out/`、`*.o`、`*.so`、`*.a`、`*.exe`、`*.dll`、`.idea/`、`*.log`、`.env*`
   - **C**：`build/`、`bin/`、`obj/`、`out/`、`*.o`、`*.a`、`*.so`、`*.exe`、`Makefile`、`config.log`、`.idea/`、`*.log`、`.env*`
   - **Swift**：`.build/`、`DerivedData/`、`*.swiftpm/`、`Packages/`
   - **R**：`.Rproj.user/`、`.Rhistory`、`.RData`、`.Ruserdata`、`*.Rproj`、`packrat/`、`renv/`
   - **Universal**：`.DS_Store`、`Thumbs.db`、`*.tmp`、`*.swp`、`.vscode/`、`.idea/`

   **工具专项模式**：
   - **Docker**：`node_modules/`、`.git/`、`Dockerfile*`、`.dockerignore`、`*.log*`、`.env*`、`coverage/`
   - **ESLint**：`node_modules/`、`dist/`、`build/`、`coverage/`、`*.min.js`
   - **Prettier**：`node_modules/`、`dist/`、`build/`、`coverage/`、`package-lock.json`、`yarn.lock`、`pnpm-lock.yaml`
   - **Terraform**：`.terraform/`、`*.tfstate*`、`*.tfvars`、`.terraform.lock.hcl`
   - **Kubernetes/k8s**：`*.secret.yaml`、`secrets/`、`.kube/`、`kubeconfig*`、`*.key`、`*.crt`

5. 解析 tasks.md 结构并提取：
   - **Task phases**：Setup、Tests、Core、Integration、Polish
   - **Task dependencies**：串行/并行规则
   - **Task details**：ID、描述、文件路径、并行标记 [P]
   - **Execution flow**：执行顺序与依赖要求

6. 按任务计划执行实现：
   - **按阶段执行**：完成当前 phase 后再进入下一 phase
   - **遵守依赖**：串行任务按顺序，带 [P] 的并行任务可并行
   - **遵循 TDD**：先执行测试任务，再执行对应实现任务
   - **按文件协调**：涉及同一文件的任务必须串行
   - **验证检查点**：每个 phase 完成后校验再前进

7. 实施规则：
   - **先 Setup**：初始化项目结构、依赖、配置
   - **先测试后代码**：若需要为 contracts、entities、集成场景写测试
   - **Core 开发**：实现 models、services、CLI commands、endpoints
   - **Integration 工作**：数据库连接、中间件、日志、外部服务
   - **Polish 与验证**：单元测试、性能优化、文档

8. 进度跟踪与错误处理：
   - 每完成一个任务就报告进度
   - 任一非并行任务失败则停止执行
   - 对并行任务 [P]：继续成功项并报告失败项
   - 提供清晰、可调试的错误上下文
   - 若无法继续实现，给出下一步建议
   - **重要**：已完成任务必须在 tasks 文件中标记为 [X]

9. 完成校验：
   - 验证所有必需任务均完成
   - 检查实现是否符合原始 specification
   - 验证测试通过且覆盖率满足要求
   - 确认实现遵循 technical plan
   - 输出最终状态与完成摘要

注意：该命令假设 tasks.md 已具备完整任务拆解。若任务缺失或不完整，建议先运行 `/speckit.tasks` 重新生成任务列表。
