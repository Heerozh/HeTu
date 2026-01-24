"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import numpy as np

from hetu.data import BaseComponent
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
        print(f"Warning: dtype：{dtype} 不可识别, 转换为 string 类型")
        return "string"


def generate_component(component_cls: type[BaseComponent]):
    attributes = [
        f"    public {dtype_to_csharp(prop.dtype)} {name};"
        for name, prop in component_cls.properties_
        if name not in {"id", "_version"}
    ]

    lines = [
        "",
        f"class {component_cls.name_}: IBaseComponent",
        "{",
        "    public long id { get; set; }",
        *attributes,
        "}",
        "",
    ]
    return lines


def generate_all_components(namespace: str, output: str):
    lines = [
        "using HeTu;",
        "",
        f"namespace {namespace}",
        "{",
    ]

    clusters = SystemClusters().get_clusters(namespace)
    assert clusters
    components = [comp for cluster in clusters for comp in cluster.components]

    from tqdm import tqdm

    for component_cls in tqdm(components):
        lines.extend([" " * 4 + line for line in generate_component(component_cls)])

    lines.append("}")

    with open(output, "w") as f:
        f.write("\n".join(lines))
