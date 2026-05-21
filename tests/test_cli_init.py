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
