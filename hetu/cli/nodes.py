"""
`hetu nodes`：看节点发现的注册表 `hetu:nodes`（设计稿 docs/superpowers/specs/2026-11-15-node-discovery-design.md
§5.5），stdout 输出一行 JSON：没过期的登记按地址汇总（worker 数、各状态的个数、连接数、最大年龄、版本），
过期的、解析不了的单独列出，再列出注册表里出现过的 namespace。

不加载 app，用 HeTu 的配置文件（或 --db）连默认后端，默认读 servant，`--master` 读 master。在节点或运维机上
跑；网关上没有 HeTu 的配置，用 `hetu dns --once`。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import argparse
import copy
import time
from typing import Any, cast

from ..i18n import _
from .base import CommandInterface, ConfigError, UsageError, load_command_config
from .console import describe_target, run_json_command


async def _nodes_main(args: argparse.Namespace, report: dict) -> dict[str, Any]:
    from ..data.backend import Backend
    from ..data.backend.redis.client import RedisBackendClient
    from ..discovery.registry import REGISTRY_KEY, decode_registry, summarize
    from ..local import BackendNotReady

    config, config_file = load_command_config(args, need_app=False, need_db=False)
    report["target"] = describe_target(config, config_file, None)
    backends = config.get("BACKENDS") or {}
    if not backends:
        raise ConfigError(
            _("配置里没有 BACKENDS：用 --config 指定配置文件，或者用 --db 给出地址")
        )
    name, db_cfg = next(iter(backends.items()))
    kind = str((db_cfg or {}).get("type", "")).lower()
    if kind != "redis":
        raise UsageError(
            _(
                "节点发现的注册表在 Redis 后端里：默认后端（配置里的第一个）{name} 的 type 是 {kind}"
            ).format(name=name, kind=kind)
        )
    try:
        backend = Backend(copy.deepcopy(db_cfg))
    except ConnectionError as e:
        raise BackendNotReady(str(e)) from e
    try:
        client = cast(
            RedisBackendClient, backend.master if args.master else backend.servant
        )
        raw = await client.aio.hgetall(REGISTRY_KEY)
    finally:
        await backend.close()
    now = time.time()
    entries, invalid = decode_registry(raw or {})
    return {
        "read_from": "master" if args.master else "servant",
        "key": REGISTRY_KEY,
        "now": now,
        **summarize(entries, invalid, now),
    }


class NodesCommand(CommandInterface):
    json_output = True

    @classmethod
    def name(cls):
        return "nodes"

    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser(
            "nodes",
            help=_(
                "看节点发现的注册表（hetu:nodes）：各节点的 worker 数、连接数、状态，过期的登记，"
                "stdout 输出一行 JSON"
            ),
        )
        group = parser.add_argument_group(_("配置"))
        group.add_argument(
            "--config",
            metavar="config.yml",
            help=_("配置文件；不给时依次找环境变量 HETU_CONFIG、当前目录的 config.yml"),
        )
        group.add_argument(
            "--db",
            metavar="redis://127.0.0.1:6379/0",
            help=_("（无配置文件时）后端数据库地址"),
        )
        parser.add_argument(
            "--master",
            action="store_true",
            help=_("读 master；默认读 servant"),
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=0,
            help=_("stderr 上多打日志：-v 为 INFO，-vv 为 DEBUG"),
        )

    @classmethod
    def execute(cls, args):
        run_json_command(args.verbose, lambda report: _nodes_main(args, report))
