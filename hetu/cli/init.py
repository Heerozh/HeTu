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
            config_text = render_config(read_config_template(), namespace, app_file_rel)
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
        print(_("提示：默认使用 SQLite 调试数据库，文件会自动创建，无需额外服务。"))
        print(_("      生产环境请在 config.yml 的 BACKENDS 中改用 Redis。"))
