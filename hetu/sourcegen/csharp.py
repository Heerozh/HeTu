"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import re
from pathlib import Path

import numpy as np

from hetu import Permission
from hetu.data import BaseComponent
from hetu.i18n import _
from hetu.system import SystemClusters

DTYPE_CS_MAP = {
    "int8": "sbyte",
    "int16": "short",
    "int32": "int",
    "int64": "long",
    "uint8": "byte",
    "uint16": "ushort",
    "uint32": "uint",
    "uint64": "ulong",
    "float16": "float",  # 等unity支持half再改
    "float32": "float",
    "float64": "double",
}


def dtype_to_csharp(dtype: str | type):
    dtype = np.dtype(dtype).name
    ctype = DTYPE_CS_MAP.get(dtype, None)
    if ctype:
        return ctype
    elif dtype.startswith("str"):
        return "string"
    else:
        print(
            _("Warning: dtype：{dtype} 不可识别, 转换为 string 类型").format(
                dtype=dtype
            )
        )
        return "string"


def to_csharp_property_name(name: str) -> str:
    parts = [p for p in re.split(r"_+", name.strip("_")) if p]
    if not parts:
        return "Field"
    return "".join(part[:1].upper() + part[1:] for part in parts)


def generate_component(component_cls: type[BaseComponent]):
    attributes = [
        f'    [Key("{name}")] public {dtype_to_csharp(prop.dtype)} '
        f"{to_csharp_property_name(name)};"
        for name, prop in component_cls.properties_
        if name not in {"id", "_version"}
    ]

    lines = [
        "",
        "[MessagePackObject]",
        f"public class {component_cls.name_} : IBaseComponent",
        "{",
        '    [Key("id")] public long ID { get; set; }',
        *attributes,
        "}",
        "",
    ]
    return lines


def generate_all_components(namespace: str, output: str):
    lines = [
        "using HeTu;",
        "using MessagePack;",
        "",
        f"namespace {namespace}",
        "{",
    ]

    clusters = SystemClusters().get_clusters(namespace)
    assert clusters
    components = []
    visited: set[type[BaseComponent]] = set()
    for cluster in clusters:
        for comp in cluster.components:
            # 副本(duplicate)的 name_ 带冒号(如 ChatMessage:Universe)，是非法 C# 类名。
            # 副本与 master schema 完全相同，统一映射到 master 去重，只输出干净的 master 类。
            # 客户端用 master 类型 + componentName 字符串订阅各副本。
            master = comp.master_ or comp
            if master in visited or master.permission_ == Permission.ADMIN:
                continue
            components.append(master)
            visited.add(master)

    # Cluster.components 是 set，对类对象的迭代顺序按 id() 哈希、每进程不同。
    # 按 name_ 排序固定输出顺序，确保内容跨 build 稳定（配合上面的跳过写入）。
    components.sort(key=lambda c: c.name_)

    from tqdm import tqdm

    for component_cls in tqdm(components):
        lines.extend([" " * 4 + line for line in generate_component(component_cls)])

    lines.append("}")

    content = "\n".join(lines)
    # 内容不变时跳过写入，避免触发客户端（如 Unity）无谓的重编译。
    out_path = Path(output)
    if out_path.exists() and out_path.read_text(encoding="utf-8") == content:
        print(f"↩️  内容无变化，跳过写入 {output}")
        return
    out_path.write_text(content, encoding="utf-8")
