"""hetu.common.yamlloader 测试：${VAR} / ${VAR:-default} 环境变量插值。"""

import io

import pytest
import yaml

from hetu.common import yamlloader


def _load(text: str):
    return yaml.load(io.StringIO(text), yamlloader.Loader)


def test_env_substitution_basic(monkeypatch):
    monkeypatch.setenv("HETU_TEST_AUTH", "secret123")
    cfg = _load("auth_key: ${HETU_TEST_AUTH}\n")
    assert cfg["auth_key"] == "secret123"


def test_env_substitution_embedded_in_url(monkeypatch):
    monkeypatch.setenv("HETU_TEST_PW", "p@ss")
    cfg = _load("master: redis://:${HETU_TEST_PW}@127.0.0.1:6379/0\n")
    assert cfg["master"] == "redis://:p@ss@127.0.0.1:6379/0"


def test_env_substitution_multiple_in_one_value(monkeypatch):
    monkeypatch.setenv("HETU_TEST_HOST", "10.0.0.1")
    monkeypatch.setenv("HETU_TEST_PORT", "6380")
    cfg = _load("addr: ${HETU_TEST_HOST}:${HETU_TEST_PORT}\n")
    assert cfg["addr"] == "10.0.0.1:6380"


def test_env_default_used_when_missing(monkeypatch):
    monkeypatch.delenv("HETU_TEST_LISTEN", raising=False)
    cfg = _load("LISTEN: ${HETU_TEST_LISTEN:-0.0.0.0:2466}\n")
    # default 含冒号也应完整保留
    assert cfg["LISTEN"] == "0.0.0.0:2466"


def test_env_set_overrides_default(monkeypatch):
    monkeypatch.setenv("HETU_TEST_LISTEN", "1.2.3.4:9000")
    cfg = _load("LISTEN: ${HETU_TEST_LISTEN:-0.0.0.0:2466}\n")
    assert cfg["LISTEN"] == "1.2.3.4:9000"


def test_env_empty_default_allowed(monkeypatch):
    monkeypatch.delenv("HETU_TEST_OPT", raising=False)
    cfg = _load("opt: ${HETU_TEST_OPT:-}\n")
    assert cfg["opt"] == ""


def test_env_missing_without_default_raises(monkeypatch):
    monkeypatch.delenv("HETU_TEST_MISSING", raising=False)
    with pytest.raises(yaml.constructor.ConstructorError):
        _load("auth_key: ${HETU_TEST_MISSING}\n")


def test_env_substitution_in_quoted_string(monkeypatch):
    # 带引号的字符串同样应被插值
    monkeypatch.setenv("HETU_TEST_Q", "quoted-val")
    cfg = _load('auth_key: "${HETU_TEST_Q}"\n')
    assert cfg["auth_key"] == "quoted-val"


def test_non_env_string_unchanged():
    cfg = _load("name: hello_world\n")
    assert cfg["name"] == "hello_world"


def test_literal_braces_without_dollar_unchanged():
    # 不含 $ 的 {..} 不应被当作环境变量
    cfg = _load("tpl: '{not_env}'\n")
    assert cfg["tpl"] == "{not_env}"


def test_non_string_scalars_not_coerced(monkeypatch):
    # 插值只作用于字符串，数字/布尔仍保持原类型
    cfg = _load("num: 6379\nflag: true\n")
    assert cfg["num"] == 6379
    assert cfg["flag"] is True


def test_eval_tag_still_works():
    # 覆盖 str 构造器后，!eval 标签不受影响
    cfg = _load("size: !eval [ 2 ** 19 ]\n")
    assert cfg["size"] == 2**19


def test_eval_tag_requires_sequence():
    with pytest.raises(yaml.constructor.ConstructorError, match="expected a sequence"):
        _load("size: !eval 2 ** 19\n")


def _load_file(path):
    # 从真实文件加载：!include 相对文件所在目录解析
    with open(path, "r", encoding="utf-8") as f:
        return yaml.load(f, yamlloader.Loader)


def test_include_by_extension(tmp_path, monkeypatch):
    """!include 按扩展名解析：yaml/yml 递归按本 Loader 加载，json 按 json，其他原样文本；
    路径相对当前文件所在目录，被引入的 yaml 里的 !include 相对它自己的目录"""
    monkeypatch.setenv("HETU_TEST_INC", "from-env")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "backend.yml").write_text(
        "type: Redis\nmaster: ${HETU_TEST_INC}\nextra: !include extra.json\n",
        encoding="utf-8",
    )
    (sub / "extra.json").write_text(
        '{"weight": 3, "tags": ["a", "b"]}', encoding="utf-8"
    )
    (tmp_path / "limits.json").write_text("[[10, 1], [27, 5]]", encoding="utf-8")
    (tmp_path / "motd.txt").write_text("line1\nline2\n", encoding="utf-8")
    main = tmp_path / "config.yml"
    main.write_text(
        "BACKEND: !include sub/backend.yml\n"
        "LIMITS: !include limits.json\n"
        "MOTD: !include motd.txt\n",
        encoding="utf-8",
    )

    cfg = _load_file(main)
    assert cfg["BACKEND"] == {
        "type": "Redis",
        "master": "from-env",
        "extra": {"weight": 3, "tags": ["a", "b"]},
    }
    assert cfg["LIMITS"] == [[10, 1], [27, 5]]
    assert cfg["MOTD"] == "line1\nline2\n"


def test_include_reads_utf8_regardless_of_locale(tmp_path, monkeypatch):
    """被引入的文件与主配置（cli/start.py）一样按 utf-8 读：中文 Windows 未开 UTF-8
    模式时默认编码是 gbk，不指定编码的话被引入文件里的中文会乱码或解码失败"""
    real_open = open

    def gbk_default_open(file, mode="r", *args, encoding=None, **kwargs):
        # 模拟中文 Windows 未开 UTF-8 模式：文本模式不指定编码时按 gbk
        if "b" not in mode and encoding is None:
            encoding = "gbk"
        return real_open(file, mode, *args, encoding=encoding, **kwargs)

    monkeypatch.setattr(yamlloader, "open", gbk_default_open, raising=False)
    (tmp_path / "names.yml").write_text("name: 河图服务器\n", encoding="utf-8")
    main = tmp_path / "config.yml"
    main.write_text("SERVER: !include names.yml\n", encoding="utf-8")

    assert _load_file(main)["SERVER"] == {"name": "河图服务器"}
