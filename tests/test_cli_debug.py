"""
hetu call / get / range / shell 的端到端测试：子进程里跑真实命令，对一个临时 SQLite 项目
（表由 hetu upgrade 建好，和部署时一样）。外加纯函数的进程内单测。
"""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

APP = '''
import numpy as np
import hetu

print("app imported")  # 必须进 stderr，不能弄脏 stdout 的 JSON


@hetu.define_component(namespace="clidemo", permission=hetu.Permission.USER)
class Wallet(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", dtype="U16", index=True)
    gold: np.int64 = hetu.property_field(0)


@hetu.define_system(namespace="clidemo", components=(Wallet,), permission=hetu.Permission.ADMIN)
async def add_gold(ctx: hetu.SystemContext, uid: int, amount: int = 100):
    """给玩家加金币。/ Add gold."""
    print("inside system")
    async with ctx.repo[Wallet].upsert(owner=uid) as row:
        row.gold += amount
    return row.gold


@hetu.define_system(namespace="clidemo", components=(Wallet,), permission=hetu.Permission.USER)
async def rename(ctx: hetu.SystemContext, name: str):
    async with ctx.repo[Wallet].upsert(owner=ctx.caller) as row:
        row.name = name
    return hetu.ResponseToClient({"name": name})


@hetu.define_system(namespace="clidemo", components=(Wallet,), permission=hetu.Permission.EVERYBODY)
async def peek(ctx: hetu.SystemContext, uid):
    row = await ctx.repo[Wallet].get(owner=uid)
    return hetu.ResponseToClient(row.gold if row is not None else None)


@hetu.define_system(namespace="clidemo", components=(Wallet,), permission=hetu.Permission.EVERYBODY)
async def boom(ctx: hetu.SystemContext):
    raise RuntimeError("boom 💥")


@hetu.define_endpoint(namespace="clidemo", permission=hetu.Permission.EVERYBODY)
async def raw_endpoint(ctx, x):
    """纯 Endpoint"""
'''

CONFIG = """
APP_FILE: {app_file}
NAMESPACE: clidemo
INSTANCES: [s1]
DEBUG: {debug}
{extra}
BACKENDS:
  main:
    type: SQLite
    master: sqlite:///./game.db
"""


def run_hetu(*args: str, cwd: Path, env: dict | None = None, stdin: bytes = b""):
    """子进程跑 hetu 命令，返回 (退出码, stdout 文本, stderr 文本)"""
    full_env = {k: v for k, v in os.environ.items() if k != "HETU_CONFIG"}
    full_env.pop("PYTHONIOENCODING", None)
    full_env.update(env or {})
    proc = subprocess.run(
        [sys.executable, "-m", "hetu", *args],
        cwd=cwd,
        input=stdin,
        capture_output=True,
        timeout=120,
        env=full_env,
        check=False,
    )
    return proc.returncode, proc.stdout.decode("utf-8"), proc.stderr.decode("utf-8")


def run_json(*args: str, cwd: Path, **kwargs) -> tuple[int, dict]:
    code, out, err = run_hetu(*args, cwd=cwd, **kwargs)
    lines = out.splitlines()
    assert len(lines) == 1, f"stdout 应恰好一行 JSON：\n{out}\nstderr:\n{err}"
    return code, json.loads(lines[0])


def make_project(root: Path, *, debug: str = "false", extra: str = "") -> Path:
    (root / "src").mkdir(parents=True, exist_ok=True)
    (root / "src" / "app.py").write_text(APP, encoding="utf-8")
    (root / "config.yml").write_text(
        CONFIG.format(app_file="src/app.py", debug=debug, extra=extra), encoding="utf-8"
    )
    return root


@pytest.fixture(scope="module")
def project(tmp_path_factory) -> Path:
    """建好表的临时项目（hetu upgrade 建表，和部署时一样）"""
    root = make_project(tmp_path_factory.mktemp("cli") / "proj")
    code, out, err = run_hetu("upgrade", "-y", cwd=root)
    assert code in (0, None), out + err
    assert (root / "game.db").exists()
    return root


# ============ hetu call ============


def test_call_outputs_one_json_line(project):
    code, out = run_json("call", "add_gold", "1001", "500", cwd=project)
    assert code == 0, out
    assert out["ok"] is True and out["system"] == "add_gold"
    assert out["result"] == 500 and out["client"] == "ok" and out["wire_error"] is None
    assert out["identity"] == {"caller": 0, "group": "admin", "derived": True}
    assert out["target"]["instance"] == "s1"
    assert out["target"]["backends"]["main"].endswith("/game.db")
    (write,) = out["writes"]
    assert write["committed"] is True
    assert write["tables"]["Wallet"]["insert"][0]["gold"] == 500
    # DEBUG 关闭：真写入给警告
    assert any("CLI_ALLOW_WRITE" in w for w in out["warnings"])

    code, out = run_json("call", "add_gold", "1001", "-5", "--dry-run", cwd=project)
    assert code == 0 and out["result"] == 495 and out["dry_run"] is True
    assert out["writes"][0]["committed"] is False
    assert out["warnings"] == []
    code, out = run_json("get", "Wallet", "owner=1001", cwd=project)
    assert code == 0 and out["row"]["gold"] == 500  # dry-run 没有写进去


def test_call_identity_rules(project):
    code, out = run_json("call", "rename", "Alice", cwd=project)
    assert code == 2 and out["error_type"] == "IdentityRequired"
    assert "traceback" not in out

    code, out = run_json("call", "--as", "1002", "rename", "Alice", cwd=project)
    assert code == 0, out
    assert out["identity"] == {"caller": 1002, "group": "guest", "derived": False}
    assert out["client"] == {"name": "Alice"}

    # 显式身份照办，但生产端点不会放行时给警告
    code, out = run_json("call", "--as", "0", "rename", "Bob", cwd=project)
    assert code == 0
    assert any("生产端点不会放行" in w for w in out["warnings"])


def test_call_errors(project, tmp_path):
    code, out = run_json("call", "boom", cwd=project)
    assert code == 1 and out["error"] == "boom 💥"
    assert "RuntimeError" in out["traceback"]

    code, out = run_json("call", "add_gol", cwd=project)
    assert code == 2 and "add_gold" in out["error"]

    code, out = run_json("call", "add_gold", "1", "2", "3", cwd=project)
    assert code == 2

    code, out = run_json("call", "peek", "1001", cwd=project)
    assert code == 0
    assert out["client"] is None and "numpy.int64" in out["wire_error"]
    assert out["result"] == {"ResponseToClient": 500}

    # 库文件不存在：不新建
    empty = make_project(tmp_path / "empty")
    code, out = run_json("call", "add_gold", "1", "1", cwd=empty)
    assert code == 3 and out["error_type"] == "BackendNotReady"
    assert not (empty / "game.db").exists()

    # 库在，但服务器没在上面建过表
    (empty / "game.db").write_bytes(b"")
    code, out = run_json("call", "add_gold", "1", "1", cwd=empty)
    assert code == 3 and out["error_type"] == "TableNotReady"


def test_call_args(project, tmp_path):
    # str 注解的参数不做 JSON 解析
    code, out = run_json("call", "--as", "1003", "rename", "123", cwd=project)
    assert code == 0 and out["client"] == {"name": "123"}
    # 像 JSON 却解析失败（PowerShell 吃掉了引号）
    code, out = run_json("call", "add_gold", "{a:1}", cwd=project)
    assert code == 2 and "args-file" in out["error"]
    # --args-file - 从 stdin 读，容忍 BOM
    code, out = run_json(
        "call",
        "add_gold",
        "--args-file",
        "-",
        cwd=project,
        stdin="﻿[1004, 7]".encode(),
    )
    assert code == 0 and out["result"] == 7


def test_write_protection(tmp_path):
    root = make_project(tmp_path / "ro", extra="CLI_ALLOW_WRITE: false")
    code, out, err = run_hetu("upgrade", "-y", cwd=root)
    assert code == 0, out + err
    code, out = run_json("call", "add_gold", "1", "1", cwd=root)
    assert code == 3 and out["error_type"] == "CliWriteForbidden"
    code, out = run_json("call", "add_gold", "1", "1", "--dry-run", cwd=root)
    assert code == 0 and out["writes"][0]["committed"] is False
    code, out = run_json("call", "peek", "1", cwd=root)  # 只读照常
    assert code == 0 and out["client"] is None


def test_debug_config_has_no_write_warning(tmp_path):
    root = make_project(tmp_path / "dev", debug="true")
    code, out, err = run_hetu("upgrade", "-y", cwd=root)
    assert code == 0, out + err
    code, out = run_json("call", "add_gold", "1", "1", cwd=root)
    assert code == 0 and out["warnings"] == []


def test_config_discovery(project, tmp_path):
    sub = project / "src"
    # 子目录里没有 config.yml：找不到配置
    code, out = run_json("call", "--list", cwd=sub)
    assert code == 3 and out["error_type"] == "ConfigError"
    # HETU_CONFIG；SQLite 相对路径按配置目录解析，而不是当前目录
    code, out = run_json(
        "get", "Wallet", "owner=1001", cwd=sub, env={"HETU_CONFIG": "../config.yml"}
    )
    assert code == 0 and out["row"]["gold"] == 500
    assert not (sub / "game.db").exists()
    # --config 优先
    code, out = run_json(
        "get",
        "Wallet",
        "owner=1001",
        "--config",
        str(project / "config.yml"),
        cwd=tmp_path,
        env={"HETU_CONFIG": "nope.yml"},
    )
    assert code == 0


def test_encoding_on_gbk_pipe(project):
    """Windows 的 GBK 管道：输出照样是 UTF-8 JSON，emoji 也不会让进程崩"""
    code, out = run_json("call", "boom", cwd=project, env={"PYTHONIOENCODING": "gbk"})
    assert code == 1 and out["error"] == "boom 💥"


# ============ hetu get / range / --list ============


def test_get_and_range_without_app(project, tmp_path):
    for uid, name in ((2001, "[GM]Ann"), (2002, "Bea"), (2003, "Cid")):
        code, _ = run_json("call", "--as", str(uid), "rename", name, cwd=project)
        assert code == 0
    # app 文件坏掉也照样能读：get / range 不加载 app
    broken = tmp_path / "broken"
    broken.mkdir()
    config = (project / "config.yml").read_text(encoding="utf-8")
    config = config.replace("src/app.py", "missing.py").replace(
        "./game.db", str(project / "game.db").replace("\\", "/")
    )
    (broken / "config.yml").write_text(config, encoding="utf-8")

    code, out = run_json("get", "Wallet", "name=[GM]Ann", cwd=broken)
    assert code == 0 and out["row"]["owner"] == 2001  # [ 开头按字面值查
    code, out = run_json("get", "Wallet", "gold=1", cwd=broken)
    assert code == 2 and "name" in out["error"]
    code, out = run_json("get", "Wallet", "owner=999999", cwd=broken)
    assert code == 0 and out["row"] is None
    code, out = run_json("get", "Nope", "id=1", cwd=broken)
    assert code == 3 and out["error_type"] == "TableNotFound"

    code, out = run_json(
        "range",
        "Wallet",
        "owner",
        "2001",
        "2003",
        "--limit",
        "2",
        "--fields",
        "owner,name",
        cwd=broken,
    )
    assert code == 0 and out["truncated"] is True and out["count"] == 2
    assert out["rows"] == [
        {"owner": 2001, "name": "[GM]Ann"},
        {"owner": 2002, "name": "Bea"},
    ]
    code, out = run_json(
        "range",
        "Wallet",
        "owner",
        "2001",
        "2003",
        "--limit",
        "3",
        "--master",
        cwd=broken,
    )
    assert out["truncated"] is False and out["read_from"] == "master"


def test_lists(project):
    code, out = run_json("call", "--list", cwd=project)
    assert code == 0
    systems = {s["name"]: s for s in out["systems"]}
    assert not any(n.startswith("__core_pin_system_") for n in systems)
    assert systems["create_future_call"]["builtin"] is True
    add_gold = systems["add_gold"]
    assert add_gold["permission"] == "ADMIN" and add_gold["builtin"] is False
    assert add_gold["doc"] == "给玩家加金币。/ Add gold."
    assert add_gold["params"] == [
        {"name": "uid", "annotation": "int", "required": True},
        {"name": "amount", "annotation": "int", "required": False, "default": 100},
    ]
    assert [e["name"] for e in out["endpoints"]] == ["raw_endpoint"]
    assert out["endpoints"][0]["callable"] is False

    code, out = run_json("get", "--list", cwd=project)
    comps = {c["name"]: c for c in out["components"]}
    fields = {f["name"]: f for f in comps["Wallet"]["fields"]}
    assert fields["name"]["index"] is True and fields["gold"]["index"] is False
    assert "WorkerLease" in comps


# ============ hetu shell ============


def test_shell(project):
    code, out, err = run_hetu(
        "shell",
        "-c",
        "r = await call_system('add_gold', 3001, 9, raw=True)\nawait get('Wallet', owner=3001)",
        cwd=project,
    )
    assert code == 0, err
    shown = json.loads(out[out.index("{") :])
    assert shown["gold"] == 9 and shown["owner"] == 3001

    code, out, err = run_hetu(
        "shell",
        "-",
        cwd=project,
        stdin=b"show(len(await app.range('Wallet', 'owner', 3001)))",
    )
    assert code == 0 and out.strip().endswith("1"), err

    code, out, err = run_hetu("shell", "-c", "call('x')", cwd=project)
    assert code == 1 and "call_system" in err
    code, out, err = run_hetu("shell", "-c", "import sys; sys.exit(4)", cwd=project)
    assert code == 4

    code, out, err = run_hetu(
        "shell", "--dry-run", "-c", "await insert('Wallet', owner=3002)", cwd=project
    )
    assert code == 0, err
    code, out = run_json("get", "Wallet", "owner=3002", cwd=project)
    assert out["row"] is None


def test_audit_log(project):
    audit = project / "logs" / "hetu_cli_audit.jsonl"
    before = audit.read_text(encoding="utf-8").count("\n") if audit.exists() else 0
    run_json("call", "add_gold", "4001", "1", cwd=project)
    run_hetu("shell", "-c", "await insert('Wallet', owner=4002)", cwd=project)
    records = [
        json.loads(line)
        for line in audit.read_text(encoding="utf-8").splitlines()[before:]
    ]
    events = [(r["event"], r["argv"][1]) for r in records]
    assert events == [
        ("start", "call"),
        ("commit", "call"),
        ("end", "call"),
        ("start", "shell"),
        ("commit", "shell"),
        ("end", "shell"),
    ]
    assert records[0]["system"] == "add_gold" and records[2]["ok"] is True
    assert "Wallet" in records[4]["tables"] and records[4]["worker_id"] >= 1000


def test_concurrent_cli_processes_get_distinct_worker_ids(project):
    """持有租约的进程还在时，另一个 CLI 进程拿到别的 worker id"""
    full_env = {k: v for k, v in os.environ.items() if k != "HETU_CONFIG"}
    holder = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "hetu",
            "shell",
            "-c",
            textwrap.dedent(
                """
                from hetu.common.snowflake_id import SnowflakeID
                print("WID", SnowflakeID().worker_id, flush=True)
                await asyncio.sleep(8)
                """
            ),
        ],
        cwd=project,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=full_env,
    )
    try:
        assert holder.stdout is not None
        # shell 的 stdout 归用户代码：app 自己 import 时的 print 也在里面
        line = ""
        while not line.startswith("WID"):
            line = holder.stdout.readline().decode()
            assert line, "持有租约的 shell 进程提前退出了"
        holder_id = int(line.split()[-1])
        code, out = run_json("call", "add_gold", "5001", "1", cwd=project)
        assert code == 0
        new_id = out["writes"][0]["tables"]["Wallet"]["insert"][0]["id"]
        assert (new_id >> 12) & 1023 != holder_id
        assert (new_id >> 12) & 1023 >= 1000
    finally:
        holder.kill()
        holder.wait()


def test_cli_commands_do_not_load_sanic(project):
    """call / get / range / shell 进程不加载 sanic（start 的 sanic import 挪进了 execute）"""
    code = (
        "import sys, atexit\n"
        "atexit.register(lambda: sys.stderr.write('SANIC=%s' % ('sanic' in sys.modules)))\n"
        "sys.argv = ['hetu', 'call', '--list']\n"
        "from hetu.__main__ import main\n"
        "main()\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=project,
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert b"SANIC=False" in proc.stderr


# ============ 纯函数 ============


def test_parse_call_args():
    from hetu.cli.base import UsageError
    from hetu.cli.call import parse_call_args

    async def system(ctx, name: str, amount, data):
        pass

    assert parse_call_args(system, ["007", "12", '{"a": [1]}']) == [
        "007",
        12,
        {"a": [1]},
    ]
    assert parse_call_args(system, ["true", "-5", "plain"]) == ["true", -5, "plain"]
    with pytest.raises(UsageError):
        parse_call_args(system, ["x", "1", "[1,"])


def test_convert_value():
    import numpy as np

    from hetu.cli.base import UsageError
    from hetu.cli.data import convert_value

    assert convert_value(np.dtype("<i8"), "1001", point=True) == 1001
    assert convert_value(np.dtype("<i8"), "inf", point=False) == float("inf")
    assert convert_value(np.dtype("?"), "false", point=True) is False
    assert convert_value(np.dtype("<U8"), "[GM]x", point=True) == "[[GM]x"
    assert convert_value(np.dtype("<U8"), "(a", point=False) == "(a"
    with pytest.raises(UsageError):
        convert_value(np.dtype("?"), "maybe", point=True)


def test_to_jsonable_and_mask():
    import numpy as np

    from hetu.cli.console import mask_url, to_jsonable

    rows = np.rec.array(
        [(1, 2.5, "a")], dtype=[("id", "<i8"), ("v", "<f8"), ("n", "<U4")]
    )
    assert to_jsonable(rows[0]) == {"id": 1, "v": 2.5, "n": "a"}
    assert to_jsonable(rows) == [{"id": 1, "v": 2.5, "n": "a"}]
    assert to_jsonable([float("nan"), float("-inf"), (1, 2), b"\xff"]) == [
        "NaN",
        "-Infinity",
        [1, 2],
        {"__bytes__": "/w=="},
    ]
    assert mask_url("redis://:secret@10.0.0.5:6379/0") == "redis://:***@10.0.0.5:6379/0"
    assert mask_url("redis://127.0.0.1:6379/0") == "redis://127.0.0.1:6379/0"


def test_resolve_sqlite_url(tmp_path):
    from hetu.cli.base import resolve_sqlite_url

    config = tmp_path / "proj" / "config.yml"
    resolved = resolve_sqlite_url("sqlite:///./data/x.db", str(config))
    assert resolved == "sqlite:///" + str(tmp_path / "proj" / "data" / "x.db").replace(
        os.sep, "/"
    )
    assert resolve_sqlite_url("redis://h:1/0", str(config)) == "redis://h:1/0"
    absolute = "sqlite:///" + str(tmp_path / "a.db").replace(os.sep, "/")
    assert resolve_sqlite_url(absolute, str(config)) == absolute


def test_write_mode_and_production():
    from hetu.cli.console import is_production, write_mode_for

    assert write_mode_for({}, False) == "commit"
    assert write_mode_for({"CLI_ALLOW_WRITE": False}, False) == "forbid"
    assert write_mode_for({"CLI_ALLOW_WRITE": False}, True) == "dry_run"
    assert is_production({}) and is_production({"DEBUG": False})
    assert not is_production({"DEBUG": 1}) and not is_production({"DEBUG": 2})


def test_start_finds_config_by_env(monkeypatch, tmp_path):
    """start / upgrade 也认 HETU_CONFIG 与当前目录的 config.yml；命令行参数模式优先"""
    import argparse

    from hetu.cli.base import locate_config_file

    def ns(**kw):
        base = {"config": None, "app_file": None, "namespace": None, "db": None}
        return argparse.Namespace(**{**base, **kw})

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("HETU_CONFIG", raising=False)
    assert locate_config_file(ns()) is None
    (tmp_path / "config.yml").write_text("{}")
    assert locate_config_file(ns()) == "config.yml"
    monkeypatch.setenv("HETU_CONFIG", "/x/y.yml")
    assert locate_config_file(ns()) == "/x/y.yml"
    assert locate_config_file(ns(namespace="n")) is None
    assert locate_config_file(ns(config="c.yml", namespace="n")) == "c.yml"
