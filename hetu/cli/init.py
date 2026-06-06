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

# 各模板中的 __NAMESPACE__ 在渲染时被替换为项目的 namespace。

# src/app.py —— 引擎入口文件。它不属于包，只 import 包以触发全部注册。
APP_PY_TEMPLATE = '''\
"""__NAMESPACE__ — HeTu app 入口 / entry point。引擎按 APP_FILE 路径加载本文件。

本文件不属于 __NAMESPACE__ 包，只是入口；import 包即触发全部 component/system 注册。
"""

import __NAMESPACE__  # noqa: F401
'''

# src/<namespace>/__init__.py —— 用 iter_modules 自动加载 component/ 与 system/。
INIT_PY_TEMPLATE = '''\
"""HeTu app 包。import 本包即自动加载 component/ 与 system/ 下的所有模块。"""


def _autoload() -> None:
    """import component/ 与 system/ 根目录下的所有模块以触发装饰器注册。"""
    import importlib
    import pkgutil

    from . import component, system

    for pkg in (component, system):
        for info in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
            importlib.import_module(info.name)


_autoload()
'''

# src/<namespace>/component/__init__.py
COMPONENT_INIT_TEMPLATE = '''\
"""内建 component 目录。新增数据表：放一个 .py 文件并用 @define_component。"""
'''

# src/<namespace>/system/__init__.py
SYSTEM_INIT_TEMPLATE = '''\
"""内建 system 目录。新增逻辑：放一个 .py 文件并用 @define_system。"""
'''

# src/<namespace>/component/player.py
PLAYER_PY_TEMPLATE = '''\
"""玩家数据表 component / Player component."""

import numpy as np

# noinspection PyPackageRequirements
import hetu


@hetu.define_component(
    namespace="__NAMESPACE__", permission=hetu.Permission.EVERYBODY
)
class Player(hetu.BaseComponent):
    """玩家数据表。/ The player data table."""

    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", dtype="U32")
    online: bool = hetu.property_field(False)
'''

# src/<namespace>/system/login.py
LOGIN_PY_TEMPLATE = '''\
"""登录与断线 system / login & disconnect systems."""

# noinspection PyPackageRequirements
import hetu

from ..component.player import Player


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

# src/<namespace>/devtools.py —— uv run build / uv run dev 的入口（__OUTPUT_ENV__
# 替换为 <NAMESPACE 大写>_CLIENT_OUTPUT）。仅通过 subprocess 调 python -m hetu，
# 不 import HeTu 内核，避免与服务进程耦合。
DEVTOOLS_PY_TEMPLATE = '''\
"""开发脚本入口：客户端组件代码生成 + 启动开发服务器。

- ``uv run build``：把服务端 component 生成对应的客户端 C# 镜像（等价于
  ``hetu build``）。输出路径取自环境变量 ``__OUTPUT_ENV__``（在 .env.test 设置）；
  未设置则跳过生成，例如还没有客户端工程时。
- ``uv run dev``：先跑一遍 build，再强制迁移数据库，最后启动开发服务器。

输出路径放在 .env.test（已 gitignore），方便每台开发机指向各自的客户端工程。
新增 namespace 时把下面的常量改成列表循环处理即可。
"""

import os
import subprocess
import sys
from pathlib import Path

# 服务端入口与 namespace（hetu init 生成，按需修改）。
_APP_FILE = "src/app.py"
_NAMESPACE = "__NAMESPACE__"
_CONFIG = "config.yml"
# 客户端 C# 输出路径取自该环境变量（在 .env.test 设置），未设置则跳过 build。
_OUTPUT_ENV = "__OUTPUT_ENV__"


def _project_root() -> Path:
    """向上找到含 pyproject.toml 的目录（即项目根）。"""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("找不到项目根（含 pyproject.toml 的目录）")


def _load_dotenv(root: Path) -> None:
    """把项目根的 .env.test（gitignore，放开发机本地密钥/路径）加载进 os.environ。

    真实 shell 环境优先（已存在的变量不覆盖），可临时 export 覆盖 .env.test。
    简易解析：跳过空行/`#` 注释，按第一个 `=` 切分，去掉值两侧成对引号。
    """
    env_file = root / ".env.test"
    if not env_file.exists():
        return
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def build() -> None:
    """生成客户端 C# 组件镜像；未设置输出路径则跳过。"""
    root = _project_root()
    _load_dotenv(root)
    output = os.environ.get(_OUTPUT_ENV)
    if not output:
        print(
            f"ℹ️  未设置 {_OUTPUT_ENV}"
            "（在 .env.test 配置客户端输出路径），跳过 build"
        )
        return
    out_path = Path(output)
    if not out_path.is_absolute():
        out_path = (root / out_path).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"⚙️  生成 {_NAMESPACE} → {out_path}")
    subprocess.run(
        [
            sys.executable, "-m", "hetu", "build",
            "--app-file", _APP_FILE,
            "--namespace", _NAMESPACE,
            "--output", str(out_path),
        ],
        cwd=root,
        check=True,
    )
    print("✅ 客户端组件代码生成完成")


def _force_migrate(root: Path) -> None:
    """启动前强制迁移数据库表结构（开发态，丢弃无法迁移的数据）。

    ``--drop-data`` 无视列丢失，确保 schema 一改就能直接 ``uv run dev`` 起服，
    不被「需要迁移」拦住。切勿把这套参数用于生产环境！

    ``hetu upgrade`` 有两道交互确认：``-y`` 跳过「数据备份」提示；真要丢列时还有
    第二道「确认强制迁移」提示，``-y`` 跳不过，故这里直接喂 ``y`` 应答它。
    """
    print(f"\U0001f527 强制迁移数据库 — {_CONFIG}")
    subprocess.run(
        [
            sys.executable, "-m", "hetu", "upgrade",
            "--config", _CONFIG,
            "-y", "--drop-data",
        ],
        cwd=root,
        input="y\\n",
        text=True,
        check=True,
    )


def dev() -> None:
    """先生成客户端组件代码，再强制迁移数据库，最后启动开发服务器。"""
    build()
    root = _project_root()
    _force_migrate(root)
    print(f"\U0001f680 启动开发服务器 — {_CONFIG}")
    subprocess.run(
        [sys.executable, "-m", "hetu", "start", "--config", _CONFIG],
        cwd=root,
        check=False,
    )
'''

# .env.test —— 开发环境变量（gitignore）。__OUTPUT_ENV__ 替换同上。
ENV_TEST_TEMPLATE = """\
# HeTu 开发环境变量（本文件已 gitignore，放开发机本地的密钥与路径）。
# uv run dev / uv run build 启动时会自动加载本文件到环境变量。

# 设置客户端 C# 组件的输出路径以启用 uv run build（未设置则 build 跳过）。
# 指向你的客户端工程（如 Unity）中希望生成组件镜像的位置：
# __OUTPUT_ENV__=../yourclient/Assets/Scripts/Net/Components.cs
"""


def output_env_var(namespace: str) -> str:
    """build 读取客户端输出路径的环境变量名（按 namespace 命名，避免多项目串扰）。"""
    return namespace.upper() + "_CLIENT_OUTPUT"


def render_devtools(namespace: str) -> str:
    """渲染 src/<namespace>/devtools.py（uv run build/dev 入口）内容。"""
    return DEVTOOLS_PY_TEMPLATE.replace("__NAMESPACE__", namespace).replace(
        "__OUTPUT_ENV__", output_env_var(namespace)
    )


def render_env_test(namespace: str) -> str:
    """渲染 .env.test 模板内容。"""
    return ENV_TEST_TEMPLATE.replace("__OUTPUT_ENV__", output_env_var(namespace))


def render_app_py(namespace: str) -> str:
    """渲染 src/app.py 入口文件内容。"""
    return APP_PY_TEMPLATE.replace("__NAMESPACE__", namespace)


def render_player_py(namespace: str) -> str:
    """渲染 component/player.py 内容。"""
    return PLAYER_PY_TEMPLATE.replace("__NAMESPACE__", namespace)


def render_login_py(namespace: str) -> str:
    """渲染 system/login.py 内容。"""
    return LOGIN_PY_TEMPLATE.replace("__NAMESPACE__", namespace)


def read_config_template() -> str:
    """读取打包在 hetu 包内的 CONFIG_TEMPLATE.yml。"""
    resource = importlib.resources.files("hetu").joinpath("CONFIG_TEMPLATE.yml")
    return resource.read_text(encoding="utf-8")


# 随包发布、供编码 AI 阅读的 HeTu 开发索引 skill 名（写入项目 .claude/skills/）。
HETU_SKILL_NAME = "building-on-hetu"


def read_skill_doc() -> str:
    """读取打包在 hetu 包内的 building-on-hetu skill（给 LLM 的 HeTu 开发索引）。"""
    resource = importlib.resources.files("hetu").joinpath(
        "skills", HETU_SKILL_NAME, "SKILL.md"
    )
    return resource.read_text(encoding="utf-8")


def render_config(template_text: str, namespace: str, app_file: str) -> str:
    """根据 namespace 与 app 文件路径渲染 config.yml，保留模板注释。

    后端默认替换为 SQLite 调试数据库，让生成的项目无需额外服务即可启动。
    """
    text = template_text.replace(
        "NAMESPACE: game_short_name",
        f"NAMESPACE: {namespace}",
    )
    text = text.replace("APP_FILE: app.py", f"APP_FILE: {app_file}")
    # 默认用 SQLite 调试数据库，无需启动数据库服务即可 hetu start
    text = text.replace("type: Redis", "type: SQL")
    text = text.replace(
        "master: redis://127.0.0.1:6379/0",
        "master: sqlite:///./hetu.db",
    )
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


def _is_uv_boilerplate_init(content: str) -> bool:
    """判断包 __init__.py 是否仍是 uv init --lib 生成的 hello() 样板。"""
    return "def hello()" in content and "Hello from" in content


def write_project_file(
    path: Path,
    content: str,
    rel_label: str,
    *,
    replace_uv_boilerplate: bool = False,
) -> None:
    """写入项目文件；已存在则跳过，绝不覆盖用户代码。

    replace_uv_boilerplate=True 时，若文件仍是 uv init 的 hello() 样板则覆盖之。
    """
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if not (replace_uv_boilerplate and _is_uv_boilerplate_init(existing)):
            print(_("ℹ️  {path} 已存在，跳过").format(path=rel_label))
            return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(_("✅ 已创建 {path}").format(path=rel_label))


def ensure_project_scripts(pyproject_path: Path, namespace: str) -> None:
    """在 pyproject.toml 的 [project.scripts] 注册 build/dev 命令（幂等）。

    - 已指向 <namespace>.devtools 则跳过；
    - 已存在别处定义的 build/dev key 则跳过并提示（避免生成重复键即非法 TOML）；
    - 有 [project.scripts] 段则插入段首，否则在文件末尾追加新段。
    """
    import re

    text = pyproject_path.read_text(encoding="utf-8")
    module = f"{namespace}.devtools"
    if f"{module}:build" in text and f"{module}:dev" in text:
        print(_("ℹ️  pyproject.toml 已注册 build/dev 命令，跳过"))
        return
    if re.search(r"(?m)^\s*build\s*=", text) or re.search(r"(?m)^\s*dev\s*=", text):
        print(_("⚠️  pyproject.toml 已有 build/dev 命令定义，跳过注册，请手动检查"))
        return

    addition = f'build = "{module}:build"\ndev = "{module}:dev"\n'
    if "[project.scripts]" in text:
        header = text.index("[project.scripts]")
        nl = text.find("\n", header)
        if nl == -1:  # 段头在文件末尾且无换行
            text += "\n"
            nl = len(text) - 1
        new_text = text[: nl + 1] + addition + text[nl + 1 :]
    else:
        new_text = text.rstrip("\n") + "\n\n[project.scripts]\n" + addition
    pyproject_path.write_text(new_text, encoding="utf-8")
    print(_("✅ 已在 pyproject.toml 注册 build/dev 命令"))


def ensure_gitignore_entry(gitignore_path: Path, entry: str) -> None:
    """确保 .gitignore 含某条目（缺失则追加；文件不存在则创建）。"""
    text = gitignore_path.read_text(encoding="utf-8") if gitignore_path.exists() else ""
    if entry in text.splitlines():
        return
    new_text = (text.rstrip("\n") + "\n" if text else "") + f"{entry}\n"
    gitignore_path.write_text(new_text, encoding="utf-8")
    print(_("✅ 已将 {entry} 加入 .gitignore").format(entry=entry))


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

        # 确定包目录与 namespace：取 src/ 下的首个包目录，没有则按项目名新建
        src_dir = project_dir / "src"
        pkg_dirs: list[Path] = []
        if src_dir.is_dir():
            pkg_dirs = sorted(
                p
                for p in src_dir.iterdir()
                if p.is_dir() and not p.name.startswith((".", "__"))
            )
        if pkg_dirs:
            pkg_dir = pkg_dirs[0]
            namespace = pkg_dir.name
        else:
            namespace = project_dir.name.replace("-", "_")
            pkg_dir = src_dir / namespace

        # 步骤2：写入入口文件与包内 component / system（已存在均跳过，不覆盖用户代码）
        # src/app.py 是引擎入口，不属于包，只 import 包
        write_project_file(src_dir / "app.py", render_app_py(namespace), "src/app.py")
        # 包 __init__.py：uv init 生成的是 hello() 样板，替换为自动加载器
        write_project_file(
            pkg_dir / "__init__.py",
            INIT_PY_TEMPLATE,
            f"src/{namespace}/__init__.py",
            replace_uv_boilerplate=True,
        )
        write_project_file(
            pkg_dir / "component" / "__init__.py",
            COMPONENT_INIT_TEMPLATE,
            f"src/{namespace}/component/__init__.py",
        )
        write_project_file(
            pkg_dir / "component" / "player.py",
            render_player_py(namespace),
            f"src/{namespace}/component/player.py",
        )
        write_project_file(
            pkg_dir / "system" / "__init__.py",
            SYSTEM_INIT_TEMPLATE,
            f"src/{namespace}/system/__init__.py",
        )
        write_project_file(
            pkg_dir / "system" / "login.py",
            render_login_py(namespace),
            f"src/{namespace}/system/login.py",
        )

        # 步骤2.5：写 devtools.py 并注册 uv run build / uv run dev 命令；
        # .env.test（开发环境变量，build 的客户端输出路径放这里）并加入 .gitignore
        write_project_file(
            pkg_dir / "devtools.py",
            render_devtools(namespace),
            f"src/{namespace}/devtools.py",
        )
        # 走到这里 pyproject.toml 必定已存在（uv init 创建或本就存在）
        ensure_project_scripts(project_dir / "pyproject.toml", namespace)
        write_project_file(
            project_dir / ".env.test",
            render_env_test(namespace),
            ".env.test",
        )
        ensure_gitignore_entry(project_dir / ".gitignore", ".env.test")

        # 步骤3：写 config.yml（已存在则跳过）
        config_path = project_dir / "config.yml"
        if config_path.exists():
            print(_("ℹ️  config.yml 已存在，跳过"))
        else:
            config_text = render_config(read_config_template(), namespace, "src/app.py")
            config_path.write_text(config_text, encoding="utf-8")
            print(_("✅ 已创建 config.yml"))

        # 步骤4：写入供编码 AI 阅读的 HeTu 开发 skill（.claude/skills/，已存在则跳过）
        write_project_file(
            project_dir / ".claude" / "skills" / HETU_SKILL_NAME / "SKILL.md",
            read_skill_doc(),
            f".claude/skills/{HETU_SKILL_NAME}/SKILL.md",
        )

        # 步骤5：uv add hetudb numpy（hetudb 已在依赖中则跳过）
        # 走到这里 pyproject.toml 必定已存在（uv init 创建或本就存在）
        pyproject_path = project_dir / "pyproject.toml"
        if "hetudb" in pyproject_path.read_text(encoding="utf-8"):
            print(_("ℹ️  hetudb 已在依赖中，跳过 uv add"))
        else:
            run_uv(["add", "hetudb", "numpy"], cwd=project_dir)

        # 步骤6：提示启动命令
        print()
        print(_("🎉 项目已就绪！下一步："))
        if args.name:
            print(f"  cd {args.name}")
        print(
            "  uv run dev        " + _("# 强制迁移并启动开发服务器（含客户端代码生成）")
        )
        print("  uv run build      " + _("# 仅生成客户端 C# 组件代码"))
        print("  uv run hetu start --config=config.yml  " + _("# 仅启动服务"))
        print()
        print(_("提示：默认使用 SQLite 调试数据库，文件会自动创建，无需额外服务。"))
        print(_("      生产环境请在 config.yml 的 BACKENDS 中改用 Redis。"))
        print(
            _(
                "      uv run build 需在 .env.test 设置 {env} 指向客户端工程输出路径。"
            ).format(env=output_env_var(namespace))
        )
        print(
            _("      已写入 .claude/skills/{name}/，供编码 AI 了解 HeTu 用法。").format(
                name=HETU_SKILL_NAME
            )
        )
