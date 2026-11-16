"""
节点发现（设计稿 docs/superpowers/specs/2026-11-15-node-discovery-design.md）：节点把自己的地址登记进 Redis
的注册表（`registry`），网关上的 `hetu dns` 把注册表答成 A 记录给 Caddy 的 `dynamic a`。只支持 host 网络，
部署检查见 `netcheck`。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

Node discovery: nodes register their address in a Redis hash; `hetu dns` on the gateway
serves it to Caddy as A records.
"""
