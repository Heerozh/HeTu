# `hetu init` 命令实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 HeTu CLI 增加 `init` 子命令，一条命令生成可运行的初始项目骨架。

**Architecture:** 新增 `hetu/cli/init.py`，包含纯渲染函数（`render_app_py` /
`render_config` / `read_config_template`）与 `InitCommand`（沿用现有
`CommandInterface` 子类 + argparse subparser 模式）。命令通过 `subprocess` 调用
`uv init --lib` 与 `uv add hetudb`，并写出 `app.py` 与 `config.yml`。
`CONFIG_TEMPLATE.yml` 移入 `hetu` 包成为可打包资源，经 `importlib.resources`
读取。

**Tech Stack:** Python 3.14、argparse、subprocess、importlib.resources、uv、
pytest、setuptools。

**前置条件:** 工作在已创建的分支 `feature/cli-init` 上。设计规格见
`docs/superpowers/specs/2026-05-21-cli-init-command-design.md`。

**注意:** 工作区有一处与本功能无关的预存改动 `M .vscode/settings.json`。
每次提交务必只 `git add` 本任务明确列出的文件，不要 `git add -A` / `git add .`。

---

## 文件结构

| 文件 | 职责 | 动作 |
|---|---|---|
| `hetu/CONFIG_TEMPLATE.yml` | 配置模板（打包资源，单一可信源） | 由根目录移入 |
| `hetu/cli/init.py` | `init` 命令：模板常量、渲染函数、`run_uv`、`InitCommand` | 新建 |
| `hetu/cli/__init__.py` | 注册 `InitCommand` | 修改 |
| `pyproject.toml` | `[tool.setuptools.package-data]` 打包 yml | 修改 |
| `tests/test_cli_init.py` | `init` 命令测试 | 新建 |
| `README.md` / `CLAUDE.md` / `AGENTS.md` / `docs/**` | 更新模板路径引用 | 修改 |

---

## Task 1: 将 CONFIG_TEMPLATE.yml 移入 hetu 包并配置打包

**Files:**
- Move: `CONFIG_TEMPLATE.yml` → `hetu/CONFIG_TEMPLATE.yml`
- Modify: `pyproject.toml`

- [ ] **Step 1: 用 git mv 移动文件**

Run:
```bash
git mv CONFIG_TEMPLATE.yml hetu/CONFIG_TEMPLATE.yml
```
Expected: 无输出；`git status` 显示
`renamed: CONFIG_TEMPLATE.yml -> hetu/CONFIG_TEMPLATE.yml`。

- [ ] **Step 2: 在 pyproject.toml 增加 package-data 段**

用 Edit 工具修改 `pyproject.toml`。

old_string:
```
exclude = ["tests*", "benchmark*", "docs*"]

[tool.pytest.ini_options]
```

new_string:
```
exclude = ["tests*", "benchmark*", "docs*"]

[tool.setuptools.package-data]
hetu = ["CONFIG_TEMPLATE.yml"]

[tool.pytest.ini_options]
```

- [ ] **Step 3: 验证 dev 模式下可作为包资源读取**

Run:
```bash
uv run python3 -c "import importlib.resources as r; f = r.files('hetu').joinpath('CONFIG_TEMPLATE.yml'); print(f.is_file(), 'NAMESPACE: game_short_name' in f.read_text(encoding='utf-8'))"
```
Expected: `True True`

- [ ] **Step 4: 验证文件被打入 wheel**

Run:
```bash
uv build --wheel && uv run python3 -c "import glob, zipfile; w = sorted(glob.glob('dist/*.whl'))[-1]; print('hetu/CONFIG_TEMPLATE.yml' in zipfile.ZipFile(w).namelist())"
```
Expected: 末行输出 `True`（构建约需数十秒）。

- [ ] **Step 5: 清理构建产物**

Run:
```bash
rm -rf dist
```
Expected: 无输出。`dist/` 为本次构建临时产物；不要动仓库已有的 `build/` 目录。

- [ ] **Step 6: Commit**

```bash
git add CONFIG_TEMPLATE.yml hetu/CONFIG_TEMPLATE.yml pyproject.toml
git commit -m "ENH: CONFIG_TEMPLATE.yml 移入 hetu 包，为 init 命令做准备"
```

---

## Task 2: 更新文档中的 CONFIG_TEMPLATE.yml 路径引用

**Files:**
- Modify: `README.md`、`CLAUDE.md`、`AGENTS.md`、`docs/en/operations.md`、
  `docs/zh/operations.md`、`docs/en/advanced.md`、`docs/zh/advanced.md`

> 说明：`hetu/cli/start.py:137` 的 `--config` 帮助文本中也提到
> `CONFIG_TEMPLATE.yml`，但它是 `_()` 包裹的 i18n 字符串，改动会使翻译条目
> 失效，且仅为文件名提示，**本任务不修改它**（设计规格第 6 节已允许跳过）。

- [ ] **Step 1: 更新 README.md**

Edit `README.md`：
- old_string: `配置模板见 CONFIG_TEMPLATE.yml 文件。`
- new_string: `配置模板见 hetu/CONFIG_TEMPLATE.yml 文件。`

- [ ] **Step 2: 更新 CLAUDE.md**

Edit `CLAUDE.md`（保持表格列宽：加入 `hetu/` 5 字符，同时删去 5 个尾随空格）：
- old_string: `配置见 CONFIG_TEMPLATE.yml                 |`
  （`CONFIG_TEMPLATE.yml` 与 `|` 之间为 17 个空格）
- new_string: `配置见 hetu/CONFIG_TEMPLATE.yml            |`
  （`CONFIG_TEMPLATE.yml` 与 `|` 之间为 12 个空格）

- [ ] **Step 3: 更新 AGENTS.md**

Edit `AGENTS.md`（该行内容与 CLAUDE.md 完全一致）：
- old_string: `配置见 CONFIG_TEMPLATE.yml                 |`
  （`CONFIG_TEMPLATE.yml` 与 `|` 之间为 17 个空格）
- new_string: `配置见 hetu/CONFIG_TEMPLATE.yml            |`
  （`CONFIG_TEMPLATE.yml` 与 `|` 之间为 12 个空格）

- [ ] **Step 4: 更新 docs/en/operations.md**

对 `docs/en/operations.md` 做两处 Edit，均 `replace_all=true`：
1. old_string: `https://github.com/Heerozh/HeTu/blob/main/CONFIG_TEMPLATE.yml`
   new_string: `https://github.com/Heerozh/HeTu/blob/main/hetu/CONFIG_TEMPLATE.yml`
2. old_string: `` `CONFIG_TEMPLATE.yml` ``
   new_string: `` `hetu/CONFIG_TEMPLATE.yml` ``

- [ ] **Step 5: 更新 docs/zh/operations.md**

对 `docs/zh/operations.md` 做两处 Edit，均 `replace_all=true`：
1. old_string: `https://github.com/Heerozh/HeTu/blob/main/CONFIG_TEMPLATE.yml`
   new_string: `https://github.com/Heerozh/HeTu/blob/main/hetu/CONFIG_TEMPLATE.yml`
2. old_string: `` `CONFIG_TEMPLATE.yml` ``
   new_string: `` `hetu/CONFIG_TEMPLATE.yml` ``

- [ ] **Step 6: 更新 docs/en/advanced.md**

Edit `docs/en/advanced.md`，`replace_all=true`：
- old_string: `` `CONFIG_TEMPLATE.yml` ``
- new_string: `` `hetu/CONFIG_TEMPLATE.yml` ``

- [ ] **Step 7: 更新 docs/zh/advanced.md**

Edit `docs/zh/advanced.md`，`replace_all=true`：
- old_string: `` `CONFIG_TEMPLATE.yml` ``
- new_string: `` `hetu/CONFIG_TEMPLATE.yml` ``

- [ ] **Step 8: 验证无遗漏**

Run:
```bash
grep -rn "CONFIG_TEMPLATE" README.md CLAUDE.md AGENTS.md docs/
```
Expected: 每一行中的 `CONFIG_TEMPLATE.yml` 都带 `hetu/` 前缀，没有裸引用。

- [ ] **Step 9: Commit**

```bash
git add README.md CLAUDE.md AGENTS.md docs/en/operations.md docs/zh/operations.md docs/en/advanced.md docs/zh/advanced.md
git commit -m "MAINT: 更新 CONFIG_TEMPLATE.yml 的文档路径引用"
```

---

## Task 3: 实现 init.py 的模板常量与渲染函数（TDD）

**Files:**
- Create: `hetu/cli/init.py`
- Test: `tests/test_cli_init.py`

- [ ] **Step 1: 写失败的测试**

创建 `tests/test_cli_init.py`：

```python
"""hetu init 命令的测试。"""

from hetu.cli.init import read_config_template, render_app_py, render_config


def test_render_app_py_substitutes_namespace():
    code = render_app_py("mygame")
    assert 'namespace="mygame"' in code
    assert "__NAMESPACE__" not in code
    assert "async def login(" in code
    assert "async def on_disconnect(" in code


def test_render_app_py_is_valid_python():
    compile(render_app_py("mygame"), "app.py", "exec")


def test_render_config_substitutes_namespace_and_app_file():
    template = "APP_FILE: app.py\nNAMESPACE: game_short_name\nDEBUG: false\n"
    out = render_config(template, "mygame", "src/mygame/app.py")
    assert "APP_FILE: src/mygame/app.py" in out
    assert "NAMESPACE: mygame" in out
    assert "game_short_name" not in out
    assert "DEBUG: false" in out  # 其余内容保留


def test_read_config_template_is_packaged():
    text = read_config_template()
    assert "APP_FILE: app.py" in text
    assert "NAMESPACE: game_short_name" in text
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `uv run pytest tests/test_cli_init.py -v`
Expected: 收集错误 / FAIL —— `ModuleNotFoundError: No module named 'hetu.cli.init'`。

- [ ] **Step 3: 创建 hetu/cli/init.py（仅模板与渲染函数）**

创建 `hetu/cli/init.py`，内容如下：

```python
"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import importlib.resources

# 初始 app.py 模板。__NAMESPACE__ 在渲染时被替换为项目的 namespace。
APP_PY_TEMPLATE = '''\
"""__NAMESPACE__ — HeTu starter app. 在此定义你的 Component 和 System。"""

import numpy as np

import hetu


@hetu.define_component(
    namespace="__NAMESPACE__", permission=hetu.Permission.EVERYBODY
)
class Player(hetu.BaseComponent):
    """玩家数据表。/ The player data table."""

    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", dtype="U32")
    online: bool = hetu.property_field(False)


@hetu.define_system(
    namespace="__NAMESPACE__",
    components=(Player,),
    permission=hetu.Permission.EVERYBODY,
)
async def login(ctx: hetu.SystemContext, user_id: int, name: str):
    """客户端通过 callSystem('login', user_id, name) 登录。"""
    await hetu.elevate(ctx, int(user_id), kick_logged_in=True)
    async with ctx.repo[Player].upsert(owner=ctx.caller) as row:
        row.name = name
        row.online = True


@hetu.define_system(
    namespace="__NAMESPACE__",
    components=(Player,),
    permission=None,
)
async def on_disconnect(ctx: hetu.SystemContext):
    """连接断开时由引擎自动调用，客户端无法直接调用。"""
    if row := await ctx.repo[Player].get(owner=ctx.caller):
        row.online = False
        await ctx.repo[Player].update(row)
'''


def render_app_py(namespace: str) -> str:
    """渲染初始 app.py 内容。"""
    return APP_PY_TEMPLATE.replace("__NAMESPACE__", namespace)


def read_config_template() -> str:
    """读取打包在 hetu 包内的 CONFIG_TEMPLATE.yml。"""
    resource = importlib.resources.files("hetu").joinpath("CONFIG_TEMPLATE.yml")
    return resource.read_text(encoding="utf-8")


def render_config(template_text: str, namespace: str, app_file: str) -> str:
    """根据 namespace 与 app 文件路径渲染 config.yml，保留模板注释。"""
    text = template_text.replace(
        "NAMESPACE: game_short_name",
        f"NAMESPACE: {namespace}",
    )
    text = text.replace("APP_FILE: app.py", f"APP_FILE: {app_file}")
    return text
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `uv run pytest tests/test_cli_init.py -v`
Expected: 4 个测试全部 PASS。

- [ ] **Step 5: lint 与格式化**

Run:
```bash
uv run ruff check hetu/cli/init.py tests/test_cli_init.py
uv run ruff format hetu/cli/init.py tests/test_cli_init.py
```
Expected: check 通过（无 lint 警告）；format 至多微调换行，提交格式化后的结果。

- [ ] **Step 6: Commit**

```bash
git add hetu/cli/init.py tests/test_cli_init.py
git commit -m "ENH: hetu init 命令的渲染函数"
```

---

## Task 4: 实现 InitCommand 与 run_uv 并注册命令（TDD）

**Files:**
- Modify: `hetu/cli/init.py`（补全 `run_uv`、`InitCommand`）
- Modify: `hetu/cli/__init__.py`（注册 `InitCommand`）
- Test: `tests/test_cli_init.py`（追加注册与编排测试）

- [ ] **Step 1: 写失败的测试（覆盖整个测试文件）**

将 `tests/test_cli_init.py` 整体写为以下内容（在 Task 3 基础上追加注册与
`execute` 编排测试）：

```python
"""hetu init 命令的测试。"""

import argparse

from hetu.cli import CommandIndex
from hetu.cli.init import (
    InitCommand,
    read_config_template,
    render_app_py,
    render_config,
)


# --- 渲染函数 ---


def test_render_app_py_substitutes_namespace():
    code = render_app_py("mygame")
    assert 'namespace="mygame"' in code
    assert "__NAMESPACE__" not in code
    assert "async def login(" in code
    assert "async def on_disconnect(" in code


def test_render_app_py_is_valid_python():
    compile(render_app_py("mygame"), "app.py", "exec")


def test_render_config_substitutes_namespace_and_app_file():
    template = "APP_FILE: app.py\nNAMESPACE: game_short_name\nDEBUG: false\n"
    out = render_config(template, "mygame", "src/mygame/app.py")
    assert "APP_FILE: src/mygame/app.py" in out
    assert "NAMESPACE: mygame" in out
    assert "game_short_name" not in out
    assert "DEBUG: false" in out  # 其余内容保留


def test_read_config_template_is_packaged():
    text = read_config_template()
    assert "APP_FILE: app.py" in text
    assert "NAMESPACE: game_short_name" in text


# --- 命令注册 ---


def test_init_command_registered():
    index = CommandIndex()
    index.register()
    args = index.parser.parse_args(["init", "myproj", "--python", "3.14"])
    assert args.command == "init"
    assert args.name == "myproj"
    assert args.python == "3.14"


def test_init_name_is_optional():
    index = CommandIndex()
    index.register()
    args = index.parser.parse_args(["init"])
    assert args.command == "init"
    assert args.name is None


# --- execute 编排 ---


def test_execute_fresh_project(tmp_path, monkeypatch):
    calls = []

    def fake_run_uv(uv_args, cwd):
        calls.append(list(uv_args))
        if uv_args[0] == "init":
            proj = tmp_path / "mygame"
            (proj / "src" / "mygame").mkdir(parents=True)
            (proj / "pyproject.toml").write_text(
                '[project]\nname = "mygame"\ndependencies = []\n',
                encoding="utf-8",
            )

    monkeypatch.setattr("hetu.cli.init.run_uv", fake_run_uv)
    monkeypatch.chdir(tmp_path)

    args = argparse.Namespace(name="mygame", python="3.14")
    InitCommand.execute(args)

    proj = tmp_path / "mygame"
    app_py = proj / "src" / "mygame" / "app.py"
    config = proj / "config.yml"
    assert app_py.exists()
    assert config.exists()
    assert 'namespace="mygame"' in app_py.read_text(encoding="utf-8")
    assert "APP_FILE: src/mygame/app.py" in config.read_text(encoding="utf-8")
    assert "NAMESPACE: mygame" in config.read_text(encoding="utf-8")
    assert ["init", "--lib", "--python", "3.14", "mygame"] in calls
    assert ["add", "hetudb"] in calls


def test_execute_skips_existing_files(tmp_path, monkeypatch):
    proj = tmp_path / "mygame"
    (proj / "src" / "mygame").mkdir(parents=True)
    (proj / "pyproject.toml").write_text(
        '[project]\nname = "mygame"\ndependencies = ["hetudb"]\n',
        encoding="utf-8",
    )
    app_py = proj / "src" / "mygame" / "app.py"
    app_py.write_text("# user code\n", encoding="utf-8")
    config = proj / "config.yml"
    config.write_text("# user config\n", encoding="utf-8")

    calls = []
    monkeypatch.setattr(
        "hetu.cli.init.run_uv", lambda uv_args, cwd: calls.append(list(uv_args))
    )
    monkeypatch.chdir(tmp_path)

    args = argparse.Namespace(name="mygame", python="3.14")
    InitCommand.execute(args)

    # 既有用户文件未被覆盖
    assert app_py.read_text(encoding="utf-8") == "# user code\n"
    assert config.read_text(encoding="utf-8") == "# user config\n"
    # pyproject 已存在 → 跳过 uv init；hetudb 已在依赖 → 跳过 uv add
    assert calls == []
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `uv run pytest tests/test_cli_init.py -v`
Expected: 导入即失败 —— `ImportError: cannot import name 'InitCommand' from
'hetu.cli.init'`。

- [ ] **Step 3: 写入完整的 hetu/cli/init.py（覆盖 Task 3 的版本）**

将 `hetu/cli/init.py` 整体写为以下内容（在 Task 3 基础上补全导入、`run_uv`
与 `InitCommand`）：

```python
"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import importlib.resources
import subprocess
import sys
from pathlib import Path

from ..i18n import _
from .base import CommandInterface

# 初始 app.py 模板。__NAMESPACE__ 在渲染时被替换为项目的 namespace。
APP_PY_TEMPLATE = '''\
"""__NAMESPACE__ — HeTu starter app. 在此定义你的 Component 和 System。"""

import numpy as np

import hetu


@hetu.define_component(
    namespace="__NAMESPACE__", permission=hetu.Permission.EVERYBODY
)
class Player(hetu.BaseComponent):
    """玩家数据表。/ The player data table."""

    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", dtype="U32")
    online: bool = hetu.property_field(False)


@hetu.define_system(
    namespace="__NAMESPACE__",
    components=(Player,),
    permission=hetu.Permission.EVERYBODY,
)
async def login(ctx: hetu.SystemContext, user_id: int, name: str):
    """客户端通过 callSystem('login', user_id, name) 登录。"""
    await hetu.elevate(ctx, int(user_id), kick_logged_in=True)
    async with ctx.repo[Player].upsert(owner=ctx.caller) as row:
        row.name = name
        row.online = True


@hetu.define_system(
    namespace="__NAMESPACE__",
    components=(Player,),
    permission=None,
)
async def on_disconnect(ctx: hetu.SystemContext):
    """连接断开时由引擎自动调用，客户端无法直接调用。"""
    if row := await ctx.repo[Player].get(owner=ctx.caller):
        row.online = False
        await ctx.repo[Player].update(row)
'''


def render_app_py(namespace: str) -> str:
    """渲染初始 app.py 内容。"""
    return APP_PY_TEMPLATE.replace("__NAMESPACE__", namespace)


def read_config_template() -> str:
    """读取打包在 hetu 包内的 CONFIG_TEMPLATE.yml。"""
    resource = importlib.resources.files("hetu").joinpath("CONFIG_TEMPLATE.yml")
    return resource.read_text(encoding="utf-8")


def render_config(template_text: str, namespace: str, app_file: str) -> str:
    """根据 namespace 与 app 文件路径渲染 config.yml，保留模板注释。"""
    text = template_text.replace(
        "NAMESPACE: game_short_name",
        f"NAMESPACE: {namespace}",
    )
    text = text.replace("APP_FILE: app.py", f"APP_FILE: {app_file}")
    return text


def run_uv(uv_args: list[str], cwd: Path) -> None:
    """运行 uv 命令；uv 缺失或执行失败时打印友好提示并退出。"""
    try:
        subprocess.run(["uv", *uv_args], cwd=cwd, check=True)
    except FileNotFoundError:
        print(_("❌ 未找到 uv 命令，请先安装 uv：https://docs.astral.sh/uv/"))
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(
            _("❌ uv {cmd} 执行失败（退出码 {code}）").format(
                cmd=" ".join(uv_args), code=e.returncode
            )
        )
        sys.exit(e.returncode or 1)


class InitCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "init"

    @classmethod
    def register(cls, subparsers):
        parser_init = subparsers.add_parser("init", help=_("初始化一个新的河图项目"))
        parser_init.add_argument(
            "name",
            nargs="?",
            metavar="project_name",
            help=_("项目目录名，省略则在当前目录初始化"),
        )
        parser_init.add_argument(
            "--python",
            metavar="3.14",
            help=_("项目使用的 Python 版本"),
            default=f"{sys.version_info.major}.{sys.version_info.minor}",
        )

    @classmethod
    def execute(cls, args):
        project_dir = Path(args.name).resolve() if args.name else Path.cwd()

        # 步骤1：uv init --lib（已存在 pyproject.toml 则跳过）
        if (project_dir / "pyproject.toml").exists():
            print(_("ℹ️  检测到 pyproject.toml，跳过 uv init"))
        else:
            uv_args = ["init", "--lib", "--python", args.python]
            if args.name:
                uv_args.append(args.name)
            run_uv(uv_args, cwd=Path.cwd())

        # 确定包目录、namespace 与 app.py 路径
        src_dir = project_dir / "src"
        pkg_dirs: list[Path] = []
        if src_dir.is_dir():
            pkg_dirs = sorted(
                p
                for p in src_dir.iterdir()
                if p.is_dir() and not p.name.startswith((".", "__"))
            )
        if pkg_dirs:
            namespace = pkg_dirs[0].name
            app_py_path = pkg_dirs[0] / "app.py"
            app_file_rel = f"src/{namespace}/app.py"
        else:
            namespace = project_dir.name.replace("-", "_")
            app_py_path = project_dir / "app.py"
            app_file_rel = "app.py"

        # 步骤2：写 app.py（已存在则跳过，绝不覆盖用户代码）
        if app_py_path.exists():
            print(_("ℹ️  {path} 已存在，跳过").format(path=app_file_rel))
        else:
            app_py_path.parent.mkdir(parents=True, exist_ok=True)
            app_py_path.write_text(render_app_py(namespace), encoding="utf-8")
            print(_("✅ 已创建 {path}").format(path=app_file_rel))

        # 步骤3：写 config.yml（已存在则跳过）
        config_path = project_dir / "config.yml"
        if config_path.exists():
            print(_("ℹ️  config.yml 已存在，跳过"))
        else:
            config_text = render_config(
                read_config_template(), namespace, app_file_rel
            )
            config_path.write_text(config_text, encoding="utf-8")
            print(_("✅ 已创建 config.yml"))

        # 步骤4：uv add hetudb（已在依赖中则跳过）
        # 走到这里 pyproject.toml 必定已存在（uv init 创建或本就存在）
        pyproject_path = project_dir / "pyproject.toml"
        if "hetudb" in pyproject_path.read_text(encoding="utf-8"):
            print(_("ℹ️  hetudb 已在依赖中，跳过 uv add"))
        else:
            run_uv(["add", "hetudb"], cwd=project_dir)

        # 步骤5：提示启动命令
        print()
        print(_("🎉 项目已就绪！下一步："))
        if args.name:
            print(f"  cd {args.name}")
        print("  uv run hetu start --config=config.yml")
        print()
        print(_("提示：启动前需要一个可用的后端数据库（默认 redis://127.0.0.1:6379/0）。"))
```

- [ ] **Step 4: 在 hetu/cli/__init__.py 注册 InitCommand**

Edit `hetu/cli/__init__.py`。

第一处 —— 导入区：
- old_string:
```
from .build import BuildCommand
from .migrate import MigrateCommand
from .start import StartCommand
```
- new_string:
```
from .build import BuildCommand
from .init import InitCommand
from .migrate import MigrateCommand
from .start import StartCommand
```

第二处 —— COMMANDS 列表：
- old_string:
```
COMMANDS = [
    StartCommand,
    MigrateCommand,
    BuildCommand,
]
```
- new_string:
```
COMMANDS = [
    StartCommand,
    MigrateCommand,
    BuildCommand,
    InitCommand,
]
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `uv run pytest tests/test_cli_init.py -v`
Expected: 8 个测试全部 PASS。

- [ ] **Step 6: lint 与格式化**

Run:
```bash
uv run ruff check hetu/cli/init.py hetu/cli/__init__.py tests/test_cli_init.py
uv run ruff format hetu/cli/init.py hetu/cli/__init__.py tests/test_cli_init.py
```
Expected: check 全部通过；format 至多微调换行，提交格式化后的结果。

- [ ] **Step 7: Commit**

```bash
git add hetu/cli/init.py hetu/cli/__init__.py tests/test_cli_init.py
git commit -m "ENH: 新增 hetu init 命令"
```

---

## Task 5: 整体验证与端到端冒烟测试

**Files:** 无新增；仅运行检查，必要时修复。

- [ ] **Step 1: 类型检查**

Run: `uv run basedpyright`
Expected: 不引入与 `hetu/cli/init.py`、`tests/test_cli_init.py` 相关的新错误。
若有，按提示修复后重新运行。

- [ ] **Step 2: 全量 lint 与格式检查**

Run:
```bash
uv run ruff check .
uv run ruff format --check .
```
Expected: `All checks passed!`，且无文件需要重新格式化。

- [ ] **Step 3: 运行 CLI 相关测试**

Run: `uv run pytest tests/test_cli_init.py tests/test_cli_start.py -v`
Expected: 全部 PASS。CONFIG_TEMPLATE.yml 的移动不影响其他测试——已通过
`grep` 确认无测试以根路径读取该文件。

- [ ] **Step 4: 端到端冒烟测试（推荐，`uv add` 步骤需要联网）**

Run:
```bash
rm -rf /tmp/hetu-init-smoke && mkdir -p /tmp/hetu-init-smoke
(cd /workspace-HeTu && uv run hetu init /tmp/hetu-init-smoke/smoketest)
ls -R /tmp/hetu-init-smoke/smoketest
grep -n "NAMESPACE\|APP_FILE" /tmp/hetu-init-smoke/smoketest/config.yml
```
Expected: 生成 `smoketest/src/smoketest/app.py`、`smoketest/config.yml`、
`smoketest/pyproject.toml`（依赖含 `hetudb`）；命令末尾打印启动提示；
`config.yml` 中为 `NAMESPACE: smoketest` 与 `APP_FILE: src/smoketest/app.py`。
若环境离线，`uv add hetudb` 会失败并给出重试提示，但 `app.py` 与
`config.yml` 仍应已生成——这属于预期的优雅降级。

- [ ] **Step 5: 清理冒烟测试产物**

Run: `rm -rf /tmp/hetu-init-smoke`
Expected: 无输出。

- [ ] **Step 6: 若 Step 1-3 有修复改动则提交**

```bash
git add <被修复的文件>
git commit -m "MAINT: hetu init 命令的检查修复"
```
若 Step 1-3 全程无需修改，则跳过本步骤。

---

## 完成标准

- `hetu init [name]` 可生成 `uv --lib` 项目、`src/<pkg>/app.py`（含 `login`
  与 `on_disconnect`）、根目录 `config.yml`，并添加 `hetudb` 依赖。
- 重复运行对已存在的 `pyproject.toml` / `app.py` / `config.yml` / `hetudb`
  依赖均跳过，绝不覆盖用户代码。
- `CONFIG_TEMPLATE.yml` 作为 `hetu` 包资源被打包，文档引用已更新。
- `ruff`、`basedpyright`、`pytest`（CLI 测试）全部通过。
