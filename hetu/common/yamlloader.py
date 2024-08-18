import os
import json

import yaml
from typing import Any, IO


class Loader(yaml.Loader):
    """YAML Loader with `!include` constructor."""

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
    extension = os.path.splitext(filename)[1].lstrip('.')

    with open(filename, 'r') as f:
        if extension in ('yaml', 'yml'):
            return yaml.load(f, Loader)
        elif extension in ('json', ):
            return json.load(f)
        else:
            return ''.join(f.readlines())

def construct_eval(loader: Loader, node: yaml.Node) -> Any:
    if isinstance(node, yaml.SequenceNode):
        args = loader.construct_sequence(node, deep=True)
    else:
        raise yaml.constructor.ConstructorError(
            None, None, 'expected a sequence', node.start_mark)

    return loader.make_python_instance('eval', node, args, {}, False)


yaml.add_constructor('!include', construct_include, Loader)
yaml.add_constructor('!eval', construct_eval, Loader)
