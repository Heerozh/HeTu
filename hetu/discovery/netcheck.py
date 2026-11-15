"""
节点发现的部署检查（设计稿 docs/superpowers/specs/2026-11-15-node-discovery-design.md §5.2~§5.4）：算出
节点的广播地址，检查是不是 host 网络。

`hetu start` 在主进程里、开服之前调 `preflight`，不过就打印说明、以 2 退出；算出的地址经配置传给各
worker（`ADVERTISED_ADDRESS_CONFIG_KEY`）。`hetu dns` 用 `host_network_problem` 查自己的网络模式。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

Deployment checks for node discovery: the advertised address and host networking.
"""

import ipaddress
import os
import re
import socket
import struct
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from ..i18n import _

# 开服检查算出的广播地址（"ip:端口"），hetu start 的主进程经配置传给各 worker，同 boot uuid 的做法
ADVERTISED_ADDRESS_CONFIG_KEY = "DISCOVERY_ADVERTISED_ADDRESS"
SYSFS_NET = "/sys/class/net"
DEFAULT_REDIS_PORT = 6379
# 本机的容器网桥：Docker 的 docker0、自定义网络的 br-<12 位十六进制>，podman、CNI、libvirt、LXD、Incus 的
_CONTAINER_BRIDGE = re.compile(
    r"docker0|br-[0-9a-f]{12}|(?:podman|cni|virbr|lxdbr|incusbr).*"
)
_SIOCGIFCONF = 0x8912
# struct ifreq 的大小：16 字节网卡名，加上 24 字节（64 位）或 16 字节（32 位）的 union
_IFREQ_SIZE = 40 if struct.calcsize("P") == 8 else 32
# SIOCGIFCONF 一次最多列多少个地址
_MAX_IFREQS = 1024


class DiscoveryCheckFailed(Exception):
    """
    节点发现的部署检查没过。消息是给人看的说明，带改法。

    A node discovery deployment check failed; the message says how to fix it.
    """


@dataclass(frozen=True)
class NetworkProblem:
    """
    地址所在的网卡不在 host 网络里：kind 是 "veth"（容器网络的虚拟网卡，iflink ≠ ifindex）或 "bridge"
    （本机的容器网桥）
    """

    kind: str
    device: str
    ifindex: int = 0
    iflink: int = 0


def host_network_hint() -> str:
    """怎么改成 host 网络"""
    return _(
        "请用 host 网络运行：docker run --network host ...（compose：network_mode: host；"
        "k8s：hostNetwork: true）。host 网络还省掉了端口映射，性能最好。"
    )


def discovery_enabled(config: Mapping[str, Any]) -> bool:
    """
    配置里打开了节点发现（`DISCOVERY`）。不是布尔值时抛 ValueError。

    Whether node discovery (`DISCOVERY`) is on; raises ValueError unless it is a bool.
    """
    value = config.get("DISCOVERY", False)
    if not isinstance(value, bool):
        # 配置错误统一抛 ValueError（同 listen_backlog），hetu start 接住以 2 退出
        raise ValueError(  # noqa: TRY004
            _("DISCOVERY 必须是 true 或 false，现在是 {value!r}。{hint}").format(
                value=value, hint=string_value_hint(value)
            )
        )
    return value


def string_value_hint(value: Any) -> str:
    """配置值是字符串时的说明：多半是加了引号，或者是 ${VAR} 插值的结果（插值出来的都是字符串）"""
    if not isinstance(value, str):
        return ""
    return _(
        "它是字符串：加了引号，或者是 ${VAR} 插值的结果（插值出来的都是字符串）。请直接写 YAML 的"
        "布尔值、整数，或者用 !eval"
    )


# ---------------------------------------------------------------- 广播地址


def parse_listen(listen: Any) -> tuple[str, int]:
    """LISTEN（主机:端口）拆成主机和端口，拆法同 bind_socket。端口不是 1~65535 时抛 DiscoveryCheckFailed"""
    host, sep, port = str(listen).rpartition(":")
    try:
        number = int(port) if sep else 0
    except ValueError:
        number = 0
    if not 0 < number < 65536:
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现：LISTEN 要写成 主机:端口，端口是 1~65535（网关按同一个端口连各节点），"
                "现在是 {listen!r}"
            ).format(listen=listen)
        )
    return host.strip("[]"), number


def ip_is_local(ip: str) -> bool:
    """本机（这个网络命名空间里）有这个 IPv4 地址：bind 得上就有"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((ip, 0))
    except OSError:
        return False
    return True


def listen_ip(host: str) -> str | None:
    """
    LISTEN 的主机：0.0.0.0（或空）返回 None，要推出广播地址；本机的 IPv4 地址原样返回，就是广播地址；
    主机名、IPv6、回环地址、本机没有的地址抛 DiscoveryCheckFailed
    """
    if host in ("", "0.0.0.0"):
        return None
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现：LISTEN 的主机要写 IP（0.0.0.0，或者本机的一个 IPv4 地址），不能写主机名 "
                "{host}：网关要拿它当地址连过来"
            ).format(host=host)
        ) from None
    if ip.version != 4:
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现只支持 IPv4（网关答的是 A 记录），LISTEN 写的是 {host}。请改成 0.0.0.0，"
                "或者本机的一个 IPv4 地址"
            ).format(host=host)
        )
    if ip.is_loopback:
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现：LISTEN 绑在回环地址 {host} 上，别的机器上的网关连不到。请改成 0.0.0.0，"
                "或者本机的对外 IP"
            ).format(host=host)
        )
    if not ip_is_local(host):
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现：LISTEN 写的 {host} 不是本机（这个网络命名空间里）的地址。在容器里多半是"
                "没用 host 网络。{hint}"
            ).format(host=host, hint=host_network_hint())
        )
    return host


def master_endpoint(config: Mapping[str, Any]) -> tuple[str, int]:
    """默认后端（配置里的第一个）master 的主机和端口。unix socket、没有主机名的抛 DiscoveryCheckFailed"""
    backends = config.get("BACKENDS") or {}
    if not backends:
        raise DiscoveryCheckFailed(_("❌ 节点发现：配置里没有 BACKENDS"))
    name, db_cfg = next(iter(backends.items()))
    master = (db_cfg or {}).get("master")
    if isinstance(master, (list, tuple)):
        master = master[0] if master else None
    url = urlparse(str(master or ""))
    if url.scheme == "unix":
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现：默认后端 {name} 经 unix socket 连 Redis，推不出本机对外的地址。请在 "
                "LISTEN 里写明本机的对外 IP，比如 10.0.3.17:2466"
            ).format(name=name)
        )
    if not url.hostname:
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现：默认后端 {name} 的 master 地址里没有主机，推不出本机对外的地址。请在 "
                "LISTEN 里写明本机的对外 IP，比如 10.0.3.17:2466"
            ).format(name=name)
        )
    try:
        port = url.port or DEFAULT_REDIS_PORT
    except ValueError:
        port = DEFAULT_REDIS_PORT
    return url.hostname, port


def source_address(host: str, port: int) -> str:
    """
    连 host:port 时本机用的源地址：对它 connect 一个 UDP socket（不发包）再 getsockname。网关和 Redis
    一般在同一个内网，这个地址基本一定是网关连得到的。回环、链路本地地址抛 DiscoveryCheckFailed
    """
    hint = _("请在 LISTEN 里写明本机的对外 IP，比如 10.0.3.17:2466")
    try:
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_DGRAM)
        target = infos[0][4]
    except (OSError, IndexError) as e:
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现：解析不出 Redis 地址 {host} 的 IPv4 地址（{err}），推不出广播地址。{hint}"
            ).format(host=host, err=e, hint=hint)
        ) from e
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(target)
            src = s.getsockname()[0]
    except OSError as e:
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现：本机到 Redis {host} 没有路由（{err}），推不出广播地址。{hint}"
            ).format(host=host, err=e, hint=hint)
        ) from e
    ip = ipaddress.ip_address(src)
    if ip.is_loopback:
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现：连 Redis（{host}）用的是回环地址 {src}，Redis 在本机，推不出别的机器"
                "连得到的地址。{hint}"
            ).format(host=host, src=src, hint=hint)
        )
    if ip.is_link_local or ip.is_unspecified:
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现：连 Redis（{host}）用的源地址 {src} 是链路本地地址，别的机器连不到。{hint}"
            ).format(host=host, src=src, hint=hint)
        )
    return src


# ---------------------------------------------------------------- 网络模式


def local_ipv4_interfaces() -> dict[str, str]:
    """
    本机（这个网络命名空间里）的 IPv4 地址 → 网卡名（带别名标签的是 "eth0:1" 这样）。用 Linux 的
    SIOCGIFCONF 列，别的系统、出错时返回空
    """
    if not sys.platform.startswith("linux"):
        return {}
    import array
    import fcntl

    size = _MAX_IFREQS * _IFREQ_SIZE
    buf = array.array("B", bytes(size))
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            packed = fcntl.ioctl(
                s.fileno(),
                _SIOCGIFCONF,
                struct.pack("iL", size, buf.buffer_info()[0]),
            )
    except OSError:
        return {}
    used = struct.unpack("iL", packed)[0]
    data = buf.tobytes()[:used]
    interfaces: dict[str, str] = {}
    for offset in range(0, used - _IFREQ_SIZE + 1, _IFREQ_SIZE):
        name = data[offset : offset + 16].split(b"\0", 1)[0]
        # ifr_addr 是 sockaddr_in：2 字节地址族、2 字节端口，再是 4 字节地址
        ip = socket.inet_ntoa(data[offset + 20 : offset + 24])
        interfaces.setdefault(ip, name.decode("ascii", "replace"))
    return interfaces


def host_network_problem(
    ip: str,
    *,
    sysfs: str = SYSFS_NET,
    interfaces: Mapping[str, str] | None = None,
) -> NetworkProblem | None:
    """
    ip 所在的网卡不在 host 网络里就返回问题，在（或者判断不了）返回 None。只在 Linux 上判断（Docker
    Desktop 里的容器本身就是 Linux）；interfaces（地址 → 网卡名）和 sysfs 根目录给测试用。

    - 网卡是本机的容器网桥（docker0、br-xxx……）：kind="bridge"；
    - 网卡的 iflink 和 ifindex 不相等：veth 这类成对的虚拟网卡（iflink 指向另一个命名空间里的对端），
      macvlan / ipvlan（指向父网卡）同样不支持，kind="veth"。例外是 VLAN 子接口（uevent 里
      DEVTYPE=vlan），它的 iflink 指向宿主机上的父网卡，属于 host 网络。

    不按 IP 段判断：Docker 默认的地址池和 AWS 默认 VPC（172.31.0.0/16）这些常用网段重叠。

    Return why the interface holding ip is not on the host network, or None.
    """
    if interfaces is None:
        if not sys.platform.startswith("linux"):
            return None
        interfaces = local_ipv4_interfaces()
    label = interfaces.get(ip)
    if label is None:
        return None
    device = label.split(":", 1)[0]  # 别名标签 eth0:1 的网卡是 eth0
    if _CONTAINER_BRIDGE.fullmatch(device):
        return NetworkProblem("bridge", device)

    def read(name: str) -> str:
        with open(os.path.join(sysfs, device, name), encoding="ascii") as f:
            return f.read().strip()

    try:
        ifindex = int(read("ifindex"))
        iflink = int(read("iflink"))
    except OSError, ValueError:
        return None
    # iflink 是 0：没绑底层网卡的隧道（GRE、IPIP 等），不是成对的虚拟网卡
    if iflink in (ifindex, 0):
        return None
    try:
        uevent = read("uevent").splitlines()
    except OSError, ValueError:
        uevent = []
    if "DEVTYPE=vlan" in uevent:
        return None
    return NetworkProblem("veth", device, ifindex, iflink)


# ---------------------------------------------------------------- 开服检查


def check_backend(config: Mapping[str, Any]) -> None:
    """默认后端（配置里的第一个）是 Redis"""
    backends = config.get("BACKENDS") or {}
    if not backends:
        raise DiscoveryCheckFailed(_("❌ 节点发现：配置里没有 BACKENDS"))
    name, db_cfg = next(iter(backends.items()))
    kind = str((db_cfg or {}).get("type", "")).lower()
    if kind != "redis":
        raise DiscoveryCheckFailed(
            _(
                "❌ 节点发现要用 Redis 后端：默认后端（配置里的第一个）{name} 的 type 是 {kind}。"
                "SQLite 只用于单机开发，用不着节点发现，请关掉 DISCOVERY"
            ).format(name=name, kind=kind)
        )


def check_proxies(config: Mapping[str, Any]) -> None:
    """节点在网关后面：要从代理头取玩家的地址（PROXIES_COUNT ≥ 1，或者配了 REAL_IP_HEADER）"""
    if config.get("REAL_IP_HEADER"):
        return
    value = config.get("PROXIES_COUNT")
    if isinstance(value, int) and not isinstance(value, bool) and value >= 1:
        return
    raise DiscoveryCheckFailed(
        _(
            "❌ 节点发现要求 PROXIES_COUNT ≥ 1（现在是 {value!r}）：节点在网关后面，不从 "
            "X-Forwarded-For 取地址的话，所有玩家的地址都是网关的，同 IP 的匿名连接上限"
            "（MAX_ANONYMOUS_CONNECTION_BY_IP）会把玩家挡在外面。前面只有 Caddy 一层时设成 1。{hint}"
        ).format(value=value, hint=string_value_hint(value))
    )


def network_problem_message(ip: str, problem: NetworkProblem, derived: bool) -> str:
    """节点的广播地址不在 host 网络里时的说明。derived：地址是从 LISTEN 的 0.0.0.0 推出来的"""
    if problem.kind == "bridge":
        cause = (
            _(
                "LISTEN 是 0.0.0.0，按连 Redis 的源地址推出来的，说明到 Redis 走的是本机网桥（Redis 跑在"
                "本机的 bridge 网络容器里）。"
            )
            if derived
            else ""
        )
        return _(
            "❌ 节点发现：广播地址 {ip} 在本机的容器网桥 {device} 上，别的机器连不到。{cause}"
            "请在 LISTEN 里写明本机的对外 IP，比如 10.0.3.17:2466"
        ).format(ip=ip, device=problem.device, cause=cause)
    return _(
        "❌ 节点发现只支持 host 网络：广播地址 {ip} 在 {device} 上，{device} 是容器网络的虚拟网卡"
        "（veth 之类：iflink {iflink} ≠ ifindex {ifindex}），别的机器上的网关连不到。{hint}{already}"
    ).format(
        ip=ip,
        device=problem.device,
        iflink=problem.iflink,
        ifindex=problem.ifindex,
        hint=host_network_hint(),
        already=_(
            "已经是 host 网络的话（比如 k8s 的 hostNetwork，到 Redis 走的是 CNI 的虚拟网卡），在 LISTEN 里"
            "写明本机的对外 IP，比如 10.0.3.17:2466"
        )
        if derived
        else "",
    )


def preflight(
    config: Mapping[str, Any],
    *,
    sysfs: str = SYSFS_NET,
    interfaces: Mapping[str, str] | None = None,
) -> str:
    """
    节点开服之前的部署检查（§5.3），都过了返回广播地址 "ip:端口"，不过抛 DiscoveryCheckFailed：

    1. 默认后端是 Redis；
    2. PROXIES_COUNT ≥ 1（或者配了 REAL_IP_HEADER）；
    3. LISTEN 的主机是 0.0.0.0、或者本机的一个 IPv4 地址（不是回环、主机名），端口不是 0；
    4. LISTEN 是 0.0.0.0 时，广播地址取连默认后端 master 时用的源地址，要推得出来、不是回环或链路本地；
    5. 广播地址所在的网卡在 host 网络里（只在 Linux 上查）。

    端口取 LISTEN 的端口。sysfs、interfaces 给测试用，见 `host_network_problem`。

    Pre-start deployment checks; returns the advertised "ip:port".
    """
    check_backend(config)
    check_proxies(config)
    host, port = parse_listen(config.get("LISTEN", ""))
    ip = listen_ip(host)
    derived = ip is None
    if ip is None:
        ip = source_address(*master_endpoint(config))
    problem = host_network_problem(ip, sysfs=sysfs, interfaces=interfaces)
    if problem is not None:
        raise DiscoveryCheckFailed(network_problem_message(ip, problem, derived))
    return f"{ip}:{port}"


def advertised_address(config: Mapping[str, Any]) -> tuple[str, int]:
    """
    worker 登记用的广播地址：hetu start 的主进程检查过、经配置传进来的；没有（不是经 hetu start 开的服）
    就在这里检查、推算，不过抛 DiscoveryCheckFailed

    The advertised (ip, port) passed down by hetu start, or computed here.
    """
    value = config.get(ADVERTISED_ADDRESS_CONFIG_KEY) or preflight(config)
    host, port = parse_listen(value)
    return host, port
