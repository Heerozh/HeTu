"""
节点发现的部署检查（hetu/discovery/netcheck.py，设计稿 docs/superpowers/specs/2026-11-15-node-discovery-design.md
§5.2~§5.4）：广播地址怎么算、哪些部署会被拒绝，以及 hetu start 在开服之前做这些检查。

网络模式的判断用假 sysfs 和给定的"地址 → 网卡"表，不依赖本机的网卡；推广播地址要本机有到外网的路由
（connect 一个 UDP socket，不发包），没有时跳过。不需要 Docker。
"""

import ipaddress
import socket
import sys
from pathlib import Path

import pytest

from hetu.__main__ import main
from hetu.cli.start import StartCommand
from hetu.discovery import netcheck
from hetu.discovery.netcheck import (
    ADVERTISED_ADDRESS_CONFIG_KEY,
    DiscoveryCheckFailed,
    NetworkProblem,
    discovery_enabled,
    host_network_problem,
    listen_ip,
    local_ipv4_interfaces,
    master_endpoint,
    parse_listen,
    preflight,
    source_address,
)
from hetu.server.config import HeTuConfig

linux_only = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="SIOCGIFCONF 只有 Linux 有"
)

# TEST-NET-1（RFC 5737）：只用来选路由，connect UDP socket 不发包
UNREACHABLE_PEER = "192.0.2.1"


def lan_ip() -> str:
    """本机到外网时用的源地址；没有路由（离线的机器）时跳过"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect((UNREACHABLE_PEER, 9))
            ip = s.getsockname()[0]
    except OSError:
        pytest.skip("本机没有到外网的路由")
    if ipaddress.ip_address(ip).is_loopback:
        pytest.skip("到外网的源地址是回环地址")
    return ip


def node_config(**overrides) -> dict:
    """能过检查的节点配置：Redis 在 TEST-NET-1 上（按路由推出广播地址），前面一层代理"""
    return {
        "BACKENDS": {
            "main": {"type": "redis", "master": f"redis://{UNREACHABLE_PEER}:6379/0"}
        },
        "PROXIES_COUNT": 1,
        "LISTEN": "0.0.0.0:2466",
        **overrides,
    }


def fake_sysfs(tmp_path: Path, devices: dict[str, tuple[int, int, str]]) -> str:
    """/sys/class/net 的样子：网卡名 → (ifindex, iflink, uevent)"""
    root = tmp_path / "net"
    for name, (ifindex, iflink, uevent) in devices.items():
        device = root / name
        device.mkdir(parents=True)
        (device / "ifindex").write_text(f"{ifindex}\n", encoding="ascii")
        (device / "iflink").write_text(f"{iflink}\n", encoding="ascii")
        (device / "uevent").write_text(uevent, encoding="ascii")
    return str(root)


# ---------------------------------------------------------------- 开关


def test_discovery_is_off_by_default():
    assert discovery_enabled({}) is False
    assert discovery_enabled(HeTuConfig()) is False
    assert discovery_enabled({"DISCOVERY": True}) is True


@pytest.mark.parametrize("value", ["true", 1, None])
def test_discovery_must_be_a_bool(value):
    """YAML 里的 "true"（加了引号、或者是 ${VAR} 插值出来的）不能当成打开"""
    with pytest.raises(ValueError, match="DISCOVERY"):
        discovery_enabled({"DISCOVERY": value})


# ---------------------------------------------------------------- LISTEN


@pytest.mark.parametrize(
    "listen,expected",
    [
        ("0.0.0.0:2466", ("0.0.0.0", 2466)),
        ("10.0.3.17:80", ("10.0.3.17", 80)),
        ("[::1]:2466", ("::1", 2466)),
        (":2466", ("", 2466)),
    ],
)
def test_parse_listen(listen, expected):
    assert parse_listen(listen) == expected


@pytest.mark.parametrize("listen", ["0.0.0.0", "0.0.0.0:0", "0.0.0.0:70000", "a:b", ""])
def test_parse_listen_needs_a_fixed_port(listen):
    with pytest.raises(DiscoveryCheckFailed, match="LISTEN"):
        parse_listen(listen)


def test_wildcard_listen_means_derive_the_address():
    assert listen_ip("0.0.0.0") is None
    assert listen_ip("") is None


def test_listen_on_a_local_address():
    ip = lan_ip()
    assert listen_ip(ip) == ip


@pytest.mark.parametrize(
    "host,words",
    [
        ("game.internal", "不能写主机名"),
        ("::", "只支持 IPv4"),
        ("127.0.0.1", "回环地址"),
        # TEST-NET-3，不会是本机的地址
        ("203.0.113.7", "不是本机"),
    ],
)
def test_listen_host_is_rejected(host, words):
    with pytest.raises(DiscoveryCheckFailed, match=words):
        listen_ip(host)


def test_foreign_listen_address_suggests_host_network():
    """容器里 LISTEN 写了宿主机的 IP：多半是没用 host 网络，说明里要给出改法"""
    with pytest.raises(DiscoveryCheckFailed) as e:
        listen_ip("203.0.113.7")
    assert "--network host" in str(e.value)


# ---------------------------------------------------------------- 推广播地址


def _redis(master) -> dict:
    return {"BACKENDS": {"main": {"type": "redis", "master": master}}}


def test_master_endpoint():
    assert master_endpoint(_redis("redis://:pw@10.0.1.20:6380/0")) == (
        "10.0.1.20",
        6380,
    )
    assert master_endpoint(_redis("rediss://redis.internal/0")) == (
        "redis.internal",
        6379,
    )
    # url 列表取第一个
    urls = ["redis://10.0.1.21:6379/0", "redis://10.0.1.22:6379/0"]
    assert master_endpoint(_redis(urls)) == ("10.0.1.21", 6379)


@pytest.mark.parametrize(
    "master,words",
    [("unix:///run/redis.sock", "unix socket"), ("", "没有主机"), ([], "没有主机")],
)
def test_master_endpoint_needs_a_host(master, words):
    with pytest.raises(DiscoveryCheckFailed, match=words):
        master_endpoint(_redis(master))


def test_source_address_follows_the_route():
    ip = lan_ip()  # 先调：没有路由时它跳过，不然下一行先抛出来，用例就失败了
    assert source_address(UNREACHABLE_PEER, 6379) == ip


def test_source_address_rejects_a_local_redis():
    """Redis 在本机：连它用的是回环地址，推不出别的机器连得到的地址"""
    with pytest.raises(DiscoveryCheckFailed, match="回环地址") as e:
        source_address("127.0.0.1", 6379)
    assert "LISTEN" in str(e.value)


def test_source_address_rejects_an_unresolvable_host():
    with pytest.raises(DiscoveryCheckFailed, match="解析不出"):
        source_address("hetu-no-such-host.invalid", 6379)


# ---------------------------------------------------------------- 网络模式


def test_network_mode_by_interface(tmp_path):
    """
    物理网卡、VLAN 子接口、bond、宿主机自己的网桥都算 host 网络；veth（iflink 指向别的命名空间里的对端）、
    macvlan（指向父网卡）不算；本机的容器网桥按名字认出来
    """
    sysfs = fake_sysfs(
        tmp_path,
        {
            "ens5": (2, 2, "INTERFACE=ens5\nIFINDEX=2\n"),
            "bond0": (3, 3, "DEVTYPE=bond\nINTERFACE=bond0\nIFINDEX=3\n"),
            "br0": (4, 4, "DEVTYPE=bridge\nINTERFACE=br0\nIFINDEX=4\n"),
            "ens5.100": (5, 2, "DEVTYPE=vlan\nINTERFACE=ens5.100\nIFINDEX=5\n"),
            "macvlan0": (6, 2, "INTERFACE=macvlan0\nIFINDEX=6\n"),
            # 没绑底层网卡的隧道（GRE、IPIP）：iflink 是 0
            "gre1": (13, 0, "INTERFACE=gre1\nIFINDEX=13\n"),
            "eth0": (9, 10, "INTERFACE=eth0\nIFINDEX=9\n"),
        },
    )
    interfaces = {
        "10.0.0.2": "ens5",
        "10.0.0.3": "ens5:1",  # 别名标签
        "10.2.0.2": "bond0",
        "10.3.0.2": "br0",
        "10.1.0.2": "ens5.100",
        "10.4.0.2": "macvlan0",
        "10.5.0.2": "gre1",
        "172.17.0.3": "eth0:1",  # 别名标签：网卡是 eth0
        "172.17.0.4": "docker0:1",
        "10.10.0.1": "lxdbr0",
        "10.11.0.1": "incusbr0",
        "172.17.0.2": "eth0",
        "172.17.0.1": "docker0",
        "172.18.0.1": "br-0123456789ab",
        "10.88.0.1": "podman0",
        "10.22.0.1": "cni0",
        "192.168.122.1": "virbr0",
        "10.9.0.1": "eth9",  # sysfs 里没有：判断不了
    }

    def problem(ip: str) -> NetworkProblem | None:
        return host_network_problem(ip, sysfs=sysfs, interfaces=interfaces)

    for ip in (
        "10.0.0.2",
        "10.0.0.3",
        "10.2.0.2",
        "10.3.0.2",
        "10.1.0.2",
        "10.9.0.1",
        "10.5.0.2",
    ):
        assert problem(ip) is None, ip
    assert problem("10.8.8.8") is None  # 不是本机的地址：判断不了
    assert problem("172.17.0.2") == NetworkProblem("veth", "eth0", 9, 10)
    assert problem("10.4.0.2") == NetworkProblem("veth", "macvlan0", 6, 2)
    assert problem("172.17.0.3") == NetworkProblem("veth", "eth0", 9, 10)
    for ip, device in (
        ("172.17.0.1", "docker0"),
        ("172.18.0.1", "br-0123456789ab"),
        ("10.88.0.1", "podman0"),
        ("10.22.0.1", "cni0"),
        ("192.168.122.1", "virbr0"),
        ("172.17.0.4", "docker0"),
        ("10.10.0.1", "lxdbr0"),
        ("10.11.0.1", "incusbr0"),
    ):
        assert problem(ip) == NetworkProblem("bridge", device), ip


@linux_only
def test_local_interfaces_include_loopback():
    assert local_ipv4_interfaces().get("127.0.0.1") == "lo"


# ---------------------------------------------------------------- 开服检查


def test_preflight_derives_the_address_from_the_route_to_redis():
    ip = lan_ip()
    assert preflight(node_config(), interfaces={}) == f"{ip}:2466"


def test_preflight_uses_the_listen_ip_and_port():
    ip = lan_ip()
    assert preflight(node_config(LISTEN=f"{ip}:3000"), interfaces={}) == f"{ip}:3000"


def test_preflight_needs_redis():
    config = node_config(
        BACKENDS={"main": {"type": "sqlite", "master": "sqlite:///x.db"}}
    )
    with pytest.raises(DiscoveryCheckFailed, match="要用 Redis 后端"):
        preflight(config, interfaces={})


@pytest.mark.parametrize("count", [0, None, "1"])
def test_preflight_needs_proxies_count(count):
    """在网关后面不从 X-Forwarded-For 取地址，所有玩家都是网关的 IP，同 IP 匿名连接上限会挡人"""
    with pytest.raises(DiscoveryCheckFailed, match="PROXIES_COUNT") as e:
        preflight(node_config(PROXIES_COUNT=count), interfaces={})
    assert "MAX_ANONYMOUS_CONNECTION_BY_IP" in str(e.value)


def test_preflight_accepts_real_ip_header_instead_of_proxies_count():
    config = node_config(PROXIES_COUNT=0, REAL_IP_HEADER="X-Real-IP")
    ip = lan_ip()
    assert preflight(config, interfaces={}) == f"{ip}:2466"


def test_preflight_rejects_a_redis_on_this_machine():
    config = node_config(
        BACKENDS={"main": {"type": "redis", "master": "redis://127.0.0.1/0"}}
    )
    with pytest.raises(DiscoveryCheckFailed, match="回环地址"):
        preflight(config, interfaces={})


def test_preflight_rejects_container_networking(tmp_path):
    """广播地址在 veth 上：说明只支持 host 网络、怎么改，并写明判断依据"""
    ip = lan_ip()
    sysfs = fake_sysfs(tmp_path, {"eth0": (14, 15, "INTERFACE=eth0\nIFINDEX=14\n")})
    with pytest.raises(DiscoveryCheckFailed, match="只支持 host 网络") as e:
        preflight(node_config(), sysfs=sysfs, interfaces={ip: "eth0"})
    message = str(e.value)
    assert f"广播地址 {ip} 在 eth0 上" in message
    assert "iflink 15 ≠ ifindex 14" in message
    assert "--network host" in message
    assert "network_mode: host" in message
    assert "hostNetwork: true" in message


def test_derived_address_on_a_virtual_nic_also_suggests_writing_the_ip(tmp_path):
    """
    从 0.0.0.0 推出来的地址在虚拟网卡上：可能是 bridge 网络，也可能已经是 host 网络、只是到 Redis 走的是 CNI 的
    虚拟网卡（k8s hostNetwork）。说明里两条出路都给；LISTEN 写明了 IP 的就只说改用 host 网络
    """
    ip = lan_ip()
    sysfs = fake_sysfs(tmp_path, {"eth0": (14, 15, "INTERFACE=eth0\nIFINDEX=14\n")})
    with pytest.raises(DiscoveryCheckFailed) as e:
        preflight(node_config(), sysfs=sysfs, interfaces={ip: "eth0"})
    assert "LISTEN 里写明本机的对外 IP" in str(e.value)
    with pytest.raises(DiscoveryCheckFailed) as e:
        preflight(
            node_config(LISTEN=f"{ip}:2466"), sysfs=sysfs, interfaces={ip: "eth0"}
        )
    assert "LISTEN 里写明本机的对外 IP" not in str(e.value)


def test_preflight_rejects_a_redis_behind_the_local_docker_bridge():
    """到 Redis 走的是本机的 docker0（Redis 在本机的 bridge 网络容器里）：这个地址别的机器连不上"""
    ip = lan_ip()
    with pytest.raises(DiscoveryCheckFailed, match="容器网桥 docker0") as e:
        preflight(node_config(), interfaces={ip: "docker0"})
    assert "LISTEN 是 0.0.0.0" in str(e.value)


# ---------------------------------------------------------------- hetu start


START_CONFIG = """
APP_FILE: app.py
NAMESPACE: pytest
INSTANCES: [disc_start]
LISTEN: 0.0.0.0:2466
WORKER_NUM: 2
PROXIES_COUNT: 1
DISCOVERY: {discovery}
PACKET_LAYERS:
  - type: jsonb
BACKENDS:
  main:
    {backend}
"""


def write_start_config(tmp_path: Path, discovery: str, backend: str) -> str:
    config = tmp_path / "config.yml"
    config.write_text(
        START_CONFIG.format(discovery=discovery, backend=backend), encoding="utf-8"
    )
    return str(config)


def run_start(monkeypatch, config: str) -> None:
    monkeypatch.setattr(sys, "argv", ["hetu", "start", "--config", config])
    main()


def test_start_runs_the_checks_before_anything_else(tmp_path, monkeypatch, capsys):
    """检查不过：打出说明、以 2 退出，不开服（app.py 不存在也轮不到加载它）"""
    monkeypatch.chdir(tmp_path)
    backend = f"type: sqlite\n    master: sqlite:///{tmp_path / 'x.db'}"
    config = write_start_config(tmp_path, "true", backend)
    with pytest.raises(SystemExit) as e:
        run_start(monkeypatch, config)
    assert e.value.code == 2
    assert "要用 Redis 后端" in capsys.readouterr().out


def test_start_rejects_a_quoted_discovery(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    backend = f"type: redis\n    master: redis://{UNREACHABLE_PEER}:6379/0"
    config = write_start_config(tmp_path, '"true"', backend)
    with pytest.raises(SystemExit) as e:
        run_start(monkeypatch, config)
    assert e.value.code == 2
    assert "DISCOVERY 必须是 true 或 false" in capsys.readouterr().out


def test_start_passes_the_advertised_address_to_the_workers(tmp_path, monkeypatch):
    """检查算出的广播地址经配置交给 worker（同 boot uuid），worker 不再自己算"""
    monkeypatch.chdir(tmp_path)
    served: list[dict] = []
    monkeypatch.setattr(netcheck, "preflight", lambda config: "10.9.8.7:2466")
    monkeypatch.setattr(
        StartCommand,
        "serve",
        classmethod(lambda cls, config, workers: served.append(config)),
    )
    backend = f"type: redis\n    master: redis://{UNREACHABLE_PEER}:6379/0"
    run_start(monkeypatch, write_start_config(tmp_path, "true", backend))
    assert served[0][ADVERTISED_ADDRESS_CONFIG_KEY] == "10.9.8.7:2466"


def test_start_skips_the_checks_when_discovery_is_off(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    served: list[dict] = []

    def fail(config):
        raise AssertionError("DISCOVERY 关着时不该检查")

    monkeypatch.setattr(netcheck, "preflight", fail)
    monkeypatch.setattr(
        StartCommand,
        "serve",
        classmethod(lambda cls, config, workers: served.append(config)),
    )
    backend = f"type: sqlite\n    master: sqlite:///{tmp_path / 'x.db'}"
    run_start(monkeypatch, write_start_config(tmp_path, "false", backend))
    assert ADVERTISED_ADDRESS_CONFIG_KEY not in served[0]
