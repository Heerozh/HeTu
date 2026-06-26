import json
import os
import re
from typing import Any, IO

import yaml

from ..i18n import _


class Loader(yaml.Loader):
    """YAML Loader：支持 ``!include``/``!eval`` 标签，以及 ``${VAR}`` 环境变量插值。"""

    def __init__(self, stream: IO) -> None:
        """Initialise Loader."""

        try:
            self.root = os.path.split(stream.name)[0]
        except AttributeError:
            self.root = os.path.curdir

        super().__init__(stream)


def construct_include(loader: Loader, node: yaml.Node) -> Any:
    """Include file referenced at node."""

    filename = os.path.abspath(os.path.join(loader.root, loader.construct_scalar(node)))
    extension = os.path.splitext(filename)[1].lstrip(".")

    with open(filename, "r") as f:
        if extension in ("yaml", "yml"):
            return yaml.load(f, Loader)
        elif extension in ("json",):
            return json.load(f)
        else:
            return "".join(f.readlines())


def construct_eval(loader: Loader, node: yaml.Node) -> Any:
    if isinstance(node, yaml.SequenceNode):
        args = loader.construct_sequence(node, deep=True)
    else:
        raise yaml.constructor.ConstructorError(
            None, None, "expected a sequence", node.start_mark
        )

    return loader.make_python_instance("eval", node, args, {}, False)


# 匹配 ${VAR} 或 ${VAR:-default}（default 可含冒号；变量名两侧空白会被忽略）
_ENV_SUBST = re.compile(r"\$\{([^{}]+)\}")


def _resolve_env(match: "re.Match[str]", node: yaml.Node) -> str:
    name, sep, default = match.group(1).partition(":-")
    name = name.strip()
    if name in os.environ:
        return os.environ[name]
    if sep:  # 写了 ":-"，即使默认值为空字符串也使用它
        return default
    raise yaml.constructor.ConstructorError(
        None,
        None,
        _("环境变量 ${%s} 未设置且无默认值，如需空值请用 ${%s:-}") % (name, name),
        node.start_mark,
    )


def construct_env_str(loader: Loader, node: yaml.Node) -> str:
    """String constructor that interpolates ``${VAR}`` / ``${VAR:-default}``.

    所有字符串值（含带引号的）里的 ``${VAR}`` 都会被同名环境变量替换；
    ``${VAR:-default}`` 在变量未设置时取默认值；``${VAR}`` 无默认且未设置则
    启动报错（fail-fast，避免空密钥静默生效）。插值结果恒为字符串。
    """
    value = loader.construct_scalar(node)
    return _ENV_SUBST.sub(lambda m: _resolve_env(m, node), value)


yaml.add_constructor("!include", construct_include, Loader)
yaml.add_constructor("!eval", construct_eval, Loader)
# 覆盖默认 str 构造器，使所有字符串（含带引号的）支持 ${VAR} 环境变量插值
yaml.add_constructor("tag:yaml.org,2002:str", construct_env_str, Loader)
