"""
`hetu dns`：网关上的 DNS 桥（设计稿 docs/superpowers/specs/2026-11-15-node-discovery-design.md §6），把节点
发现的注册表答成 A 记录，给 Caddy 的 `dynamic a` 用（`resolvers 127.0.0.1:1053`、`versions ipv4`）。

不读 HeTu 的配置文件（网关机上不该有 master 的口令）：Redis 地址从环境变量 HETU_DNS_REDIS（或 --redis）读，
指向副本、用只读 ACL 用户。只支持 host 网络。退出码：2 是用法或部署错误，3 是环境未就绪。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import sys

from ..i18n import _
from .base import CommandInterface


class DnsCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "dns"

    @classmethod
    def register(cls, subparsers):
        from ..discovery.dns import (
            DEFAULT_LISTEN,
            DEFAULT_NAME,
            DEFAULT_PORT,
            REDIS_ENV,
        )

        parser = subparsers.add_parser(
            "dns",
            help=_(
                "网关上的 DNS 桥：把节点发现的注册表（hetu:nodes）答成 A 记录，给 Caddy 的 dynamic a 用"
                "（resolvers 指向它）。只支持 host 网络"
            ),
        )
        parser.add_argument(
            "--namespace",
            required=True,
            help=_("只答这个项目（节点配置里的 NAMESPACE）的节点"),
        )
        parser.add_argument(
            "--name",
            default=DEFAULT_NAME,
            help=_(
                "答哪个名字，和 Caddyfile 里 dynamic a 的 name 一致（默认 {default}）"
            ).format(default=DEFAULT_NAME),
        )
        parser.add_argument(
            "--port",
            type=int,
            default=DEFAULT_PORT,
            help=_(
                "节点的端口，和节点的 LISTEN 端口、Caddyfile 里 dynamic a 的 port 一致（默认 "
                "{default}）：A 记录不带端口"
            ).format(default=DEFAULT_PORT),
        )
        parser.add_argument(
            "--listen",
            default=DEFAULT_LISTEN,
            help=_("在哪里应答 DNS 查询（UDP，默认 {default}）").format(
                default=DEFAULT_LISTEN
            ),
        )
        parser.add_argument(
            "--redis",
            metavar="redis://user:pass@replica:6379/0",
            help=_(
                "Redis 地址，指向副本、用只读 ACL 用户；不给时读环境变量 {env}（推荐，口令不会出现在"
                "进程列表里）"
            ).format(env=REDIS_ENV),
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help=_(
                "排查用：跑一遍启动检查，读一次注册表并探测，打印会答哪些地址、哪些不答、为什么，然后退出"
            ),
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=0,
            help=_("多打日志（DEBUG）"),
        )

    @classmethod
    def execute(cls, args):
        from ..discovery.dns import main

        sys.exit(main(args))
