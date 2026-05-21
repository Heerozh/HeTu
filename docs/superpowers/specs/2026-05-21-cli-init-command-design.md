# Design: `hetu init` — 项目脚手架命令

日期：2026-05-21
分支：`feature/cli-init`

## 1. 目标

为 HeTu CLI 新增 `init` 子命令，帮助用户一条命令快速搭建一个可运行的初始项目：
调用 `uv` 初始化项目、生成包含最简 `login` System 与 `on_disconnect` 回调的
`app.py`、放置启动用 `config.yml`，并提示启动命令。

## 2. 命令接口

```
hetu init [name] [--python <ver>]
```

- `name`（可选位置参数）—— 要创建的项目目录。省略时在当前目录就地初始化。
  行为对齐 `uv init`。
- `--python`（可选）—— 传递给 `uv init` 的 Python 版本。默认取当前运行
  解释器的 `major.minor`（`hetu` 要求 ≥ 3.14）。

实现：新增 `hetu/cli/init.py`，内含 `InitCommand(CommandInterface)` 类，并在
`hetu/cli/__init__.py` 的 `COMMANDS` 列表中注册——与 `start` / `upgrade` /
`build` 完全相同的模式。所有用户可见字符串用 `_("...")` 包裹（i18n 约定）。

## 3. 行为（每一步若已完成则跳过，保证幂等）

| 步骤 | 动作 | 跳过条件 |
|---|---|---|
| 1 | `uv init --lib --python <ver> [name]` | 目标目录已存在 `pyproject.toml` |
| 2 | 写入 `src/<pkg>/app.py`（hello app） | `app.py` 已存在（绝不覆盖用户代码） |
| 3 | 在项目根写入 `config.yml` | `config.yml` 已存在 |
| 4 | `uv add hetudb`（添加依赖，使 `uv run hetu` 可解析） | `hetudb` 已在 `pyproject.toml` 依赖中 |
| 5 | 打印下一步启动命令 | —— |

### 包目录与 namespace 的确定

`uv init --lib` 会做名称规范化（如 `my-game` → `src/my_game/`）。因此步骤 1
之后通过 glob `src/*/` 发现实际包目录，记为 `<pkg>`。`<pkg>` 同时用作：

- `app.py` 与 `config.yml` 中的 HeTu `namespace`；
- `config.yml` 中 `APP_FILE` 的路径片段。

### 执行顺序与回退

1. 解析目标目录（`name` 或当前目录）。
2. 无 `pyproject.toml` → 执行 `uv init --lib`；否则跳过并提示。
3. 发现包目录：存在 `src/*/` 则取之；若不存在（例如在一个非 `--lib` 的既有
   项目里重复运行），回退为把 `app.py` 放在项目根，`APP_FILE` 相应设为
   `app.py`。
4. 写 `app.py`（若已存在则跳过并提示）。
5. 写 `config.yml`（若已存在则跳过并提示）。
6. 若 `hetudb` 不在依赖中 → `uv add hetudb`；否则跳过。
7. 打印成功信息与启动命令。

`uv add hetudb` 放在最后执行，因此即便网络失败，`app.py` 与 `config.yml`
仍已落盘，并附带重试提示。

## 4. 生成的 `src/<pkg>/app.py`

内容为 `init.py` 中的字符串常量模板，渲染时替换 `namespace`：

```python
"""<pkg> — HeTu starter app. 在此定义你的 Component 和 System。"""

import numpy as np

import hetu


@hetu.define_component(namespace="<pkg>", permission=hetu.Permission.EVERYBODY)
class Player(hetu.BaseComponent):
    """玩家数据表。/ The player data table."""

    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", dtype="U32")
    online: bool = hetu.property_field(False)


@hetu.define_system(
    namespace="<pkg>", components=(Player,), permission=hetu.Permission.EVERYBODY
)
async def login(ctx: hetu.SystemContext, user_id: int, name: str):
    """客户端 callSystem('login', user_id, name) 登录。"""
    await hetu.elevate(ctx, int(user_id), kick_logged_in=True)
    async with ctx.repo[Player].upsert(owner=ctx.caller) as row:
        row.name = name
        row.online = True


@hetu.define_system(namespace="<pkg>", components=(Player,), permission=None)
async def on_disconnect(ctx: hetu.SystemContext):
    """连接断开时由引擎自动调用，客户端无法直接调用。"""
    if row := await ctx.repo[Player].get(owner=ctx.caller):
        row.online = False
        await ctx.repo[Player].update(row)
```

该 API 模式取自 `examples/chat/server/src/app.py`，已验证：`login` 是客户端可调用
的 System（`elevate` 提权 + 写入），`on_disconnect`（`permission=None`）由引擎在
连接断开时自动调用、客户端不可直接调用。

## 5. 生成的 `config.yml`

将打包后的 `CONFIG_TEMPLATE.yml` 文本读入，做**两处精确的整行替换**（保留所有
注释，保留模板的教学价值），其余逐字照搬：

- `NAMESPACE: game_short_name` → `NAMESPACE: <pkg>`
- `APP_FILE: app.py` → `APP_FILE: <第 3 节确定的 app.py 相对路径>`
  （`src/<pkg>/` 布局下为 `src/<pkg>/app.py`；回退到项目根时仍为 `app.py`，
  即此行无变化）

不解析再 dump YAML（那样会丢失注释）。其余字段（`INSTANCES`、`BACKENDS`、
`auth_key` 占位符等）保持模板原样，由用户后续按需修改。

## 6. 配置模板的打包（Approach A）

`CONFIG_TEMPLATE.yml` 当前位于仓库根目录，**未**包含在 `hetu` 包内
（`[tool.setuptools.packages.find]` 只收 `hetu*`），因此 `pip install hetudb`
后 `init` 无法读取它。采用 Approach A 修复：

1. `git mv CONFIG_TEMPLATE.yml hetu/CONFIG_TEMPLATE.yml`。
2. 在 `pyproject.toml` 增加 `[tool.setuptools.package-data]`，包含
   `hetu/CONFIG_TEMPLATE.yml`（如 `"hetu" = ["CONFIG_TEMPLATE.yml"]`）。
3. `init` 通过 `importlib.resources.files("hetu") / "CONFIG_TEMPLATE.yml"`
   读取——单一可信源，无副本漂移。
4. 更新引用 `CONFIG_TEMPLATE.yml` 路径的文档（约 8 处）：`README.md`、
   `CLAUDE.md`、`AGENTS.md`、`docs/en/operations.md`、`docs/zh/operations.md`、
   `docs/en/advanced.md`、`docs/zh/advanced.md`，包括其中的 GitHub blob URL。
5. `hetu/cli/start.py` 中 `--config` 帮助文本提到的 `CONFIG_TEMPLATE.yml`
   仅是文件名，无需改动（也可顺手更新为新路径）。

实现前先确认无测试代码以根路径打开该文件（已知 grep 结果：仅 `tests/app.py`
的一行注释与 `start.py` 帮助文本提及，均非真实文件读取）。

## 7. 错误处理

- `uv` 不在 PATH（`subprocess` 抛 `FileNotFoundError`）→ 友好提示并指向
  uv 安装文档，`exit(1)`。
- `uv init` / `uv add` 返回非零 → 输出捕获到的 stderr/stdout，中止。
- `uv add hetudb` 失败（如离线）→ 因其在最后执行，`app.py` 与 `config.yml`
  已落盘；打印重试提示。

## 8. 测试 —— `tests/test_cli_init.py`

遵循 TDD：

- 纯渲染函数 `render_app_py(namespace)` / `render_config(namespace, app_file)`
  ——断言替换正确、关键字符串存在（`login`、`on_disconnect`、`namespace=` 等）。
- 文件写入助手 —— 断言「文件已存在则不覆盖」的跳过行为。
- `uv` 调用经一层薄封装（如 `_run_uv(args, cwd)`），便于测试 monkeypatch，
  从而在不联网的情况下验证编排逻辑（步骤顺序、跳过判断）。

测试需可离线运行；真实的 `uv add hetudb`（联网）不纳入默认测试。

## 9. 改动的文件

新增：
- `hetu/cli/init.py`
- `tests/test_cli_init.py`
- `docs/superpowers/specs/2026-05-21-cli-init-command-design.md`（本文件）

修改：
- `hetu/cli/__init__.py`（注册 `InitCommand`）
- `pyproject.toml`（`[tool.setuptools.package-data]`）
- `README.md`、`CLAUDE.md`、`AGENTS.md`、`docs/en/operations.md`、
  `docs/zh/operations.md`、`docs/en/advanced.md`、`docs/zh/advanced.md`
  （`CONFIG_TEMPLATE.yml` 路径引用）

移动：
- `CONFIG_TEMPLATE.yml` → `hetu/CONFIG_TEMPLATE.yml`

## 10. 不在本次范围

- 不修改 `start` / `upgrade` / `build` 的行为。
- 不为生成的 `config.yml` 自动生成随机 `auth_key`（保留模板占位符）。
- 不在启动提示中加入 `hetu upgrade` 步骤（按用户要求仅提示 `start`）。
- 不修改 `INSTANCES` 等模板字段（仅做第 5 节的两处替换）。

## 11. 启动提示（步骤 5 输出）

成功后打印类似：

```
✅ 项目 <name> 已创建！
下一步：
  cd <name>
  uv run hetu start --config=config.yml

提示：启动前需要一个可用的后端数据库（默认 redis://127.0.0.1:6379/0）。
```

（`cd <name>` 一行在就地初始化、无 `name` 参数时省略。）
