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


def test_render_app_py_suppresses_pycharm_warning():
    # hetudb 的 import 名是 hetu，PyCharm 会误报缺依赖，故模板内置抑制注释
    assert "# noinspection PyPackageRequirements\nimport hetu" in render_app_py("ns")


def test_render_config_substitutes_namespace_and_app_file():
    template = "APP_FILE: app.py\nNAMESPACE: game_short_name\nDEBUG: false\n"
    out = render_config(template, "mygame", "src/mygame/app.py")
    assert "APP_FILE: src/mygame/app.py" in out
    assert "NAMESPACE: mygame" in out
    assert "game_short_name" not in out
    assert "DEBUG: false" in out  # 其余内容保留


def test_render_config_switches_backend_to_sqlite():
    template = "  type: Redis\n  master: redis://127.0.0.1:6379/0\n"
    out = render_config(template, "ns", "app.py")
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
    cfg = config.read_text(encoding="utf-8")
    assert "APP_FILE: src/mygame/app.py" in cfg
    assert "NAMESPACE: mygame" in cfg
    # 默认后端为 SQLite，生成的项目无需数据库服务即可启动
    assert "type: SQL" in cfg
    assert "master: sqlite:///./hetu.db" in cfg
    assert ["init", "--lib", "--python", "3.14", "mygame"] in calls
    assert ["add", "hetudb", "numpy"] in calls


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
