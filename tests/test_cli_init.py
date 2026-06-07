"""hetu init 命令的测试。"""

import argparse
import tomllib

from hetu.cli import CommandIndex
from hetu.cli.init import (
    COMPONENT_INIT_TEMPLATE,
    INIT_PY_TEMPLATE,
    SYSTEM_INIT_TEMPLATE,
    InitCommand,
    ensure_gitignore_entry,
    ensure_project_scripts,
    output_env_var,
    read_config_template,
    render_app_py,
    render_config,
    render_devtools,
    render_env_test,
    render_login_py,
    render_player_py,
)

# --- 渲染函数 ---


def test_render_app_py_only_imports_package():
    # 入口文件不属于包，只 import 包以触发注册
    code = render_app_py("mygame")
    assert "import mygame" in code
    assert "__NAMESPACE__" not in code
    # 入口里不再内联 component/system 定义
    assert "define_component" not in code
    assert "define_system" not in code


def test_render_app_py_is_valid_python():
    compile(render_app_py("mygame"), "app.py", "exec")


def test_render_player_py_substitutes_namespace():
    code = render_player_py("mygame")
    assert 'namespace="mygame"' in code
    assert "__NAMESPACE__" not in code
    assert "class Player(" in code
    compile(code, "player.py", "exec")


def test_render_login_py_substitutes_namespace():
    code = render_login_py("mygame")
    assert 'namespace="mygame"' in code
    assert "__NAMESPACE__" not in code
    assert "async def login(" in code
    assert "async def on_disconnect(" in code
    # system 从 component 子包 import 它引用的数据表
    assert "from ..component.player import Player" in code
    compile(code, "login.py", "exec")


def test_package_templates_are_valid_python():
    compile(INIT_PY_TEMPLATE, "__init__.py", "exec")
    compile(COMPONENT_INIT_TEMPLATE, "component/__init__.py", "exec")
    compile(SYSTEM_INIT_TEMPLATE, "system/__init__.py", "exec")
    # __init__.py 用 iter_modules 自动加载子模块
    assert "iter_modules" in INIT_PY_TEMPLATE


def test_render_component_system_suppress_pycharm_warning():
    # hetudb 的 import 名是 hetu，PyCharm 会误报缺依赖，故模板内置抑制注释
    suppress = "# noinspection PyPackageRequirements\nimport hetu"
    assert suppress in render_player_py("ns")
    assert suppress in render_login_py("ns")


def test_render_config_substitutes_namespace_and_app_file():
    template = "APP_FILE: app.py\nNAMESPACE: game_short_name\nDEBUG: false\n"
    out = render_config(template, "mygame", "src/app.py")
    assert "APP_FILE: src/app.py" in out
    assert "NAMESPACE: mygame" in out
    assert "game_short_name" not in out
    assert "DEBUG: false" in out  # 其余内容保留


def test_render_config_switches_backend_to_sqlite():
    template = "  type: Redis\n  master: redis://127.0.0.1:6379/0\n"
    out = render_config(template, "ns", "src/app.py")
    assert "type: SQL" in out
    assert "master: sqlite:///./hetu.db" in out
    assert "type: Redis" not in out
    assert "master: redis://127.0.0.1:6379/0" not in out


def test_read_config_template_is_packaged():
    text = read_config_template()
    assert "APP_FILE: app.py" in text
    assert "NAMESPACE: game_short_name" in text
    # render_config 依赖以下两行作为后端替换锚点
    assert "type: Redis" in text
    assert "master: redis://127.0.0.1:6379/0" in text


# --- devtools / 命令注册到 pyproject ---


def test_output_env_var_is_namespaced():
    assert output_env_var("mygame") == "MYGAME_CLIENT_OUTPUT"
    assert output_env_var("ssw_lobby") == "SSW_LOBBY_CLIENT_OUTPUT"


def test_render_devtools_substitutes_namespace():
    code = render_devtools("mygame")
    assert '_NAMESPACE = "mygame"' in code
    assert '_OUTPUT_ENV = "MYGAME_CLIENT_OUTPUT"' in code
    assert "__NAMESPACE__" not in code
    assert "__OUTPUT_ENV__" not in code
    assert "def build(" in code
    assert "def dev(" in code
    # dev 走强制迁移（--drop-data），且 build 输出路径来自环境变量
    assert "--drop-data" in code
    compile(code, "devtools.py", "exec")


def test_render_env_test_substitutes_output_env():
    text = render_env_test("mygame")
    assert "MYGAME_CLIENT_OUTPUT" in text
    assert "__OUTPUT_ENV__" not in text


def test_ensure_project_scripts_fresh(tmp_path):
    # 没有 [project.scripts] 段时，追加新段并注册 build/dev
    pp = tmp_path / "pyproject.toml"
    pp.write_text('[project]\nname = "mygame"\ndependencies = []\n', encoding="utf-8")
    ensure_project_scripts(pp, "mygame")
    data = tomllib.loads(pp.read_text(encoding="utf-8"))
    assert data["project"]["scripts"]["build"] == "mygame.devtools:build"
    assert data["project"]["scripts"]["dev"] == "mygame.devtools:dev"


def test_ensure_project_scripts_into_existing_section(tmp_path):
    # 已有 [project.scripts] 段时，插入 build/dev 且保留既有项
    pp = tmp_path / "pyproject.toml"
    pp.write_text(
        '[project]\nname = "mygame"\n\n[project.scripts]\nhetu = "hetu.__main__:main"\n',
        encoding="utf-8",
    )
    ensure_project_scripts(pp, "mygame")
    data = tomllib.loads(pp.read_text(encoding="utf-8"))
    scripts = data["project"]["scripts"]
    assert scripts["hetu"] == "hetu.__main__:main"
    assert scripts["build"] == "mygame.devtools:build"
    assert scripts["dev"] == "mygame.devtools:dev"


def test_ensure_project_scripts_idempotent(tmp_path):
    pp = tmp_path / "pyproject.toml"
    pp.write_text('[project]\nname = "mygame"\n', encoding="utf-8")
    ensure_project_scripts(pp, "mygame")
    once = pp.read_text(encoding="utf-8")
    ensure_project_scripts(pp, "mygame")
    assert pp.read_text(encoding="utf-8") == once
    # 不重复键，仍是合法 TOML
    tomllib.loads(pp.read_text(encoding="utf-8"))


def test_ensure_project_scripts_skips_on_conflict(tmp_path):
    # 已有别处定义的 build key 时不动它（否则会生成重复键即非法 TOML）
    pp = tmp_path / "pyproject.toml"
    original = (
        '[project]\nname = "mygame"\n\n[project.scripts]\nbuild = "other:thing"\n'
    )
    pp.write_text(original, encoding="utf-8")
    ensure_project_scripts(pp, "mygame")
    assert pp.read_text(encoding="utf-8") == original
    tomllib.loads(pp.read_text(encoding="utf-8"))


def test_ensure_gitignore_entry_creates_and_appends(tmp_path):
    gi = tmp_path / ".gitignore"
    # 文件不存在 → 创建
    ensure_gitignore_entry(gi, ".env.test")
    assert ".env.test" in gi.read_text(encoding="utf-8").splitlines()
    # 已存在该条目 → 幂等
    before = gi.read_text(encoding="utf-8")
    ensure_gitignore_entry(gi, ".env.test")
    assert gi.read_text(encoding="utf-8") == before
    # 追加新条目，保留原有内容
    ensure_gitignore_entry(gi, ".venv")
    lines = gi.read_text(encoding="utf-8").splitlines()
    assert ".env.test" in lines
    assert ".venv" in lines


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

# uv init --lib 生成的 __init__.py 样板内容
UV_BOILERPLATE_INIT = 'def hello() -> str:\n    return "Hello from mygame!"\n'


def _fake_uv_init(tmp_path):
    """模拟 uv init --lib：创建 src/mygame 包、带 hello() 样板的 __init__.py 与 pyproject。"""
    pkg = tmp_path / "mygame" / "src" / "mygame"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(UV_BOILERPLATE_INIT, encoding="utf-8")
    (tmp_path / "mygame" / "pyproject.toml").write_text(
        '[project]\nname = "mygame"\ndependencies = []\n',
        encoding="utf-8",
    )


def test_execute_fresh_project(tmp_path, monkeypatch):
    calls = []

    def fake_run_uv(uv_args, cwd):
        calls.append(list(uv_args))
        if uv_args[0] == "init":
            _fake_uv_init(tmp_path)

    monkeypatch.setattr("hetu.cli.init.run_uv", fake_run_uv)
    monkeypatch.chdir(tmp_path)

    args = argparse.Namespace(name="mygame", python="3.14")
    InitCommand.execute(args)

    proj = tmp_path / "mygame"
    src = proj / "src"
    pkg = src / "mygame"

    # app.py 在 src/ 下，是入口而非包成员
    app_py = src / "app.py"
    assert app_py.exists()
    assert not (pkg / "app.py").exists()
    assert "import mygame" in app_py.read_text(encoding="utf-8")

    # 包 __init__.py 的 uv 样板被替换为自动加载器
    init_code = (pkg / "__init__.py").read_text(encoding="utf-8")
    assert "iter_modules" in init_code
    assert "Hello from" not in init_code

    # component / system 两个目录及内建文件
    assert (pkg / "component" / "__init__.py").exists()
    assert (pkg / "system" / "__init__.py").exists()
    player_code = (pkg / "component" / "player.py").read_text(encoding="utf-8")
    login_code = (pkg / "system" / "login.py").read_text(encoding="utf-8")
    assert 'namespace="mygame"' in player_code
    assert "class Player(" in player_code
    assert "async def login(" in login_code
    assert "async def on_disconnect(" in login_code

    # config.yml 指向 src/app.py，后端默认 SQLite
    cfg = (proj / "config.yml").read_text(encoding="utf-8")
    assert "APP_FILE: src/app.py" in cfg
    assert "NAMESPACE: mygame" in cfg
    assert "type: SQL" in cfg
    assert "master: sqlite:///./hetu.db" in cfg

    # devtools.py + uv run build/dev 注册到 pyproject.toml
    assert '_NAMESPACE = "mygame"' in (pkg / "devtools.py").read_text(encoding="utf-8")
    pp = tomllib.loads((proj / "pyproject.toml").read_text(encoding="utf-8"))
    assert pp["project"]["scripts"]["build"] == "mygame.devtools:build"
    assert pp["project"]["scripts"]["dev"] == "mygame.devtools:dev"

    # .env.test 含输出环境变量名，且被 gitignore
    env_test = (proj / ".env.test").read_text(encoding="utf-8")
    assert "MYGAME_CLIENT_OUTPUT" in env_test
    assert ".env.test" in (proj / ".gitignore").read_text(encoding="utf-8").splitlines()

    assert ["init", "--lib", "--python", "3.14", "mygame"] in calls
    assert ["add", "hetudb", "numpy"] in calls


def test_execute_skips_existing_user_files(tmp_path, monkeypatch):
    proj = tmp_path / "mygame"
    pkg = proj / "src" / "mygame"
    pkg.mkdir(parents=True)
    (proj / "pyproject.toml").write_text(
        '[project]\nname = "mygame"\ndependencies = ["hetudb"]\n',
        encoding="utf-8",
    )
    # 预置用户文件，execute 必须全部跳过
    app_py = proj / "src" / "app.py"
    app_py.write_text("# user entry\n", encoding="utf-8")
    init_py = pkg / "__init__.py"
    init_py.write_text("# user package init\n", encoding="utf-8")
    config = proj / "config.yml"
    config.write_text("# user config\n", encoding="utf-8")

    calls = []
    monkeypatch.setattr(
        "hetu.cli.init.run_uv", lambda uv_args, cwd: calls.append(list(uv_args))
    )
    monkeypatch.chdir(tmp_path)

    args = argparse.Namespace(name="mygame", python="3.14")
    InitCommand.execute(args)

    # 既有用户文件未被覆盖（用户自定义的 __init__.py 不是 uv 样板，不替换）
    assert app_py.read_text(encoding="utf-8") == "# user entry\n"
    assert init_py.read_text(encoding="utf-8") == "# user package init\n"
    assert config.read_text(encoding="utf-8") == "# user config\n"
    # pyproject 已存在 → 跳过 uv init；hetudb 已在依赖 → 跳过 uv add
    assert calls == []


def test_execute_replaces_uv_boilerplate_init(tmp_path, monkeypatch):
    # __init__.py 仍是 uv 样板时应被替换为自动加载器
    proj = tmp_path / "mygame"
    pkg = proj / "src" / "mygame"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(UV_BOILERPLATE_INIT, encoding="utf-8")
    (proj / "pyproject.toml").write_text(
        '[project]\nname = "mygame"\ndependencies = ["hetudb"]\n',
        encoding="utf-8",
    )

    monkeypatch.setattr("hetu.cli.init.run_uv", lambda uv_args, cwd: None)
    monkeypatch.chdir(tmp_path)

    args = argparse.Namespace(name="mygame", python="3.14")
    InitCommand.execute(args)

    init_code = (pkg / "__init__.py").read_text(encoding="utf-8")
    assert "iter_modules" in init_code
    assert "Hello from" not in init_code
