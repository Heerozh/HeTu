"""
提交序号列（`property_field(commit_seq=True)`）：定义校验、提交时发号（两个后端的提交脚本）、
禁写与分组列锁定、事务内看不到未分配号的行、迁移与重建时的计数器、消费方的推荐写法。
设计稿见 docs/superpowers/specs/2026-11-09-commit-seq-design.md。
"""

import json
import logging

import numpy as np
import pytest

from hetu.common.snowflake_id import SnowflakeID

SnowflakeID().init(1, 0)


def def_log():
    from hetu.data import BaseComponent, Permission, define_component, property_field

    global Log

    @define_component(namespace="pytest", permission=Permission.USER, force=True)
    class Log(BaseComponent):
        owner: np.int64 = property_field(0, index=True)
        # 每个 owner 各自从 1 连续编号
        seq: np.int64 = property_field(0, commit_seq=True, groupby="owner")
        kind: np.int16 = property_field(0)
        uid: np.int64 = property_field(0, unique=True)

    return Log


# ---------------------------------------------------------------- 定义


def test_define_commit_seq(new_component_env):
    """commit_seq 隐含 index；类属性、schema json 的形状；default 0.0 / False 规范成 0"""
    from hetu.data import BaseComponent, define_component, property_field

    comp = def_log()
    assert comp.commit_seq_ == "seq"
    assert "seq" in comp.indexes_ and "seq" not in comp.uniques_
    assert comp.index_groups_["seq"] == ("owner",)
    props = json.loads(comp.json_)["properties"]
    assert props["seq"]["commit_seq"] is True
    # 没开的列不写这个键：没用它的组件 schema 版本不变
    assert "commit_seq" not in props["owner"]
    assert BaseComponent.load_json(comp.json_).commit_seq_ == "seq"

    def define(default, **kwargs):
        @define_component(namespace="pytest", force=True)
        class G(BaseComponent):
            seq: np.int64 = property_field(default, commit_seq=True, **kwargs)

        return G

    plain = define(0)
    assert plain.commit_seq_ == "seq" and plain.index_groups_["seq"] == ()
    assert json.loads(plain.json_)["properties"]["seq"]["default"] == 0
    assert define(0.0).json_ == define(False).json_ == plain.json_

    @define_component(namespace="pytest", force=True)
    class NoSeq(BaseComponent):
        owner: np.int64 = property_field(0, index=True)

    assert NoSeq.commit_seq_ is None
    assert all(
        "commit_seq" not in p for p in json.loads(NoSeq.json_)["properties"].values()
    )


@pytest.mark.parametrize(
    "fields, match",
    [
        ({"seq": {"default": 0, "dtype": np.int32}}, "只能是np.int64"),
        ({"seq": {"default": 0, "dtype": np.uint64}}, "只能是np.int64"),
        ({"seq": {"default": 0, "dtype": np.float64}}, "只能是np.int64"),
        ({"seq": {"default": 1}}, "default必须是0"),
        ({"seq": {"default": 0, "unique": True}}, "不能同时设置unique"),
        ({"seq": {"default": 0}, "seq2": {"default": 0}}, "最多只能有一个"),
        (
            {"seq": {"default": 0}, "level": {"default": 0, "groupby": "seq"}},
            "不能当分组列",
        ),
    ],
)
def test_define_commit_seq_errors(new_component_env, fields, match):
    """类型只能是 int64、default 只能是 0、不能 unique、最多一个、不能被别的索引当分组列"""
    from hetu.data import BaseComponent, define_component, property_field

    namespace: dict = {"__annotations__": {}}
    for name, spec in fields.items():
        spec = dict(spec)
        dtype = spec.pop("dtype", np.int64)
        default = spec.pop("default")
        commit_seq = not spec.get("groupby")
        namespace["__annotations__"][name] = dtype
        namespace[name] = property_field(default, commit_seq=commit_seq, **spec)
    with pytest.raises(ValueError, match=match):
        define_component(namespace="pytest", force=True)(
            type("Bad", (BaseComponent,), namespace)
        )


def test_define_commit_seq_rls_column(new_component_env):
    """commit_seq 不能是 RLS 的判定列，也不能是 OWNER 组件的 owner"""
    from hetu.data import BaseComponent, Permission, define_component, property_field

    with pytest.raises(ValueError, match="行级权限"):

        @define_component(namespace="pytest", permission=Permission.OWNER, force=True)
        class BadOwner(BaseComponent):
            owner: np.int64 = property_field(0, commit_seq=True)

    with pytest.raises(ValueError, match="行级权限"):

        @define_component(
            namespace="pytest",
            permission=Permission.RLS,
            rls_compare=("le", "seq", "caller"),
            force=True,
        )
        class BadRls(BaseComponent):
            seq: np.int64 = property_field(0, commit_seq=True)


def test_define_commit_seq_point_sub_ignored(new_component_env, caplog):
    """
    point_sub 用不上：号要到提交时才有，发不出值频道。写了警告并忽略，hidden 的也一样（不走
    hidden + point_sub 的报错）；带 groupby 的由复合索引那条警告，只警告一次。index=False 改成 True
    """
    from hetu.data import BaseComponent, define_component, property_field

    with caplog.at_level(logging.WARNING, logger="HeTu.root"):

        @define_component(namespace="pytest", force=True)
        class A(BaseComponent):
            seq: np.int64 = property_field(0, commit_seq=True, point_sub=True)

        @define_component(namespace="pytest", force=True)
        class B(BaseComponent):
            seq: np.int64 = property_field(
                0, commit_seq=True, point_sub=True, hidden=True
            )

        @define_component(namespace="pytest", force=True)
        class C(BaseComponent):
            owner: np.int64 = property_field(0)
            seq: np.int64 = property_field(
                0, commit_seq=True, groupby="owner", point_sub=True
            )

        @define_component(namespace="pytest", force=True)
        class D(BaseComponent):
            seq: np.int64 = property_field(0, commit_seq=True, index=False)

    for comp in (A, B, C):
        assert comp.point_subs_ == frozenset(), comp.name_
        assert json.loads(comp.json_)["properties"]["seq"]["point_sub"] is False
    assert caplog.text.count("commit_seq，point_sub用不上") == 2
    assert caplog.text.count("groupby（复合索引），point_sub用不上") == 1
    assert "seq" in D.indexes_
    assert "commit_seq时，index不能设置为False" in caplog.text


def test_headless_schema_diff_commit_seq(new_component_env):
    """
    headless 的 schema 比对要比 commit_seq（对不上时 headless 的 commit 会把 0 原样写进去，或给
    普通列发号）；不认识的列属性键（更新版本加的）一律判为不一致
    """
    from hetu import headless
    from hetu.data import BaseComponent

    comp = def_log()
    data = json.loads(comp.json_)
    del data["properties"]["seq"]["commit_seq"]
    local = BaseComponent.load_json(json.dumps(data))
    errors, _warns = headless._schema_diff(local, comp.json_)
    assert errors == ["~ col seq.commit_seq: True -> False"]
    assert headless._schema_diff(comp, comp.json_) == ([], [])

    newer = json.loads(comp.json_)
    newer["properties"]["kind"]["future_flag"] = True
    errors, _warns = headless._schema_diff(comp, json.dumps(newer))
    assert errors == ["~ col kind.future_flag: unknown property"]


def test_cli_list_shows_commit_seq(new_component_env, monkeypatch):
    """hetu get --list 输出每列的 commit_seq"""
    from hetu.cli.data import list_components
    from hetu.system import SystemClusters

    comp = def_log()
    monkeypatch.setattr(
        SystemClusters(), "get_components", lambda namespace=None: {comp: 1}
    )
    [listed] = list_components("pytest")
    fields = {f["name"]: f for f in listed["fields"]}
    assert fields["seq"]["commit_seq"] is True
    assert fields["owner"]["commit_seq"] is False
