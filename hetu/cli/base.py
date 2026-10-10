"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import argparse
import logging
import os
from urllib.parse import urlparse

import yaml

from ..common import yamlloader
from ..i18n import _

logger = logging.getLogger("HeTu.root")

# 配置文件路径的环境变量：没给 --config 时用它（Docker 镜像里设为 /app/config.yml）
CONFIG_ENV = "HETU_CONFIG"
# 当前目录下默认找的配置文件名
DEFAULT_CONFIG_FILE = "config.yml"
SQLITE_PREFIX = "sqlite:///"


class ConfigError(Exception):
    """找不到 / 读不了配置，或命令行参数模式缺参数。

    Configuration could not be located or read, or flag-mode arguments are missing.
    """


class UsageError(Exception):
    """用法错误（hetu call 等的退出码 2）：参数、System / 组件不存在、字段不可查等"""


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0", "None"):
        return False
    else:
        raise argparse.ArgumentTypeError(_("Boolean value expected."))


def resolve_app_file(app_file: str, config_file: str) -> str:
    """把 config 文件中的相对 APP_FILE 路径，按 config 文件所在目录解析为绝对路径。

    Resolve a relative ``APP_FILE`` path against the directory of the config
    file, not the process CWD. Absolute paths are returned unchanged.
    """
    config_dir = os.path.dirname(os.path.abspath(config_file))
    return os.path.join(config_dir, app_file)


def infer_backend_type_from_db_url(db_url: str) -> str:
    """根据db url推断后端类型。"""
    scheme = urlparse(db_url).scheme.lower()
    if scheme in {"redis", "rediss", "valkey", "valkeys"}:
        return "redis"
    if scheme == "sqlite":
        return "sqlite"
    if scheme in {"postgres", "postgresql", "mariadb", "mysql"}:
        raise ValueError(
            _(
                "SQL 后端已移除，PostgreSQL / MariaDB 不再支持：'{scheme}'。"
                "开发用 sqlite:///<库文件路径>，生产用 Redis"
            ).format(scheme=scheme)
        )
    if scheme in {"file"}:
        return "sharedmemory"
    raise ValueError(
        _(
            "不支持的数据库URL scheme: '{scheme}'。"
            "目前支持 redis/rediss/valkey/valkeys/sqlite"
        ).format(scheme=scheme)
    )


def resolve_sqlite_url(url: str, config_file: str) -> str:
    """
    SQLite 地址里的相对路径按配置文件所在目录解析成绝对路径，与 APP_FILE 一致。不是 SQLite
    地址、或已是绝对路径的原样返回。

    相对路径以前相对进程当前目录：在别的目录运行就会打开（并新建）另一个库文件。旧位置有库
    文件而新位置没有时打 warning，提示把文件挪过去。
    """
    if not isinstance(url, str) or not url.startswith(SQLITE_PREFIX):
        return url
    path = url[len(SQLITE_PREFIX) :]
    if not path or os.path.isabs(path) or path == ":memory:":
        return url
    config_dir = os.path.dirname(os.path.abspath(config_file))
    new_path = os.path.normpath(os.path.join(config_dir, path))
    old_path = os.path.abspath(path)
    if (
        old_path != new_path
        and os.path.exists(old_path)
        and not os.path.exists(new_path)
    ):
        logger.warning(
            _(
                "⚠️ SQLite 库文件的相对路径现在按配置文件所在目录解析：{new}。"
                "旧位置（相对当前目录）{old} 有库文件，如需沿用请把它挪过去"
            ).format(new=new_path, old=old_path)
        )
    return SQLITE_PREFIX + new_path.replace(os.sep, "/")


def read_config_file(config_file: str) -> dict:
    """
    读配置文件：环境变量插值、`!eval`、`!include`（`yamlloader.Loader`），`APP_FILE` 与 SQLite
    库文件的相对路径按配置文件所在目录解析。start / upgrade / call 等命令共用。

    Read a config file; relative APP_FILE and SQLite paths resolve against its directory.
    """
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.load(f, yamlloader.Loader)
    except OSError as e:
        raise ConfigError(
            _("读不了配置文件 {path}：{err}").format(path=config_file, err=e)
        ) from e
    if not isinstance(config, dict):
        raise ConfigError(_("配置文件 {path} 不是一个映射").format(path=config_file))
    if config.get("APP_FILE"):
        config["APP_FILE"] = resolve_app_file(config["APP_FILE"], config_file)
    for db_cfg in (config.get("BACKENDS") or {}).values():
        if not isinstance(db_cfg, dict):
            continue
        if "master" in db_cfg:
            db_cfg["master"] = resolve_sqlite_url(db_cfg["master"], config_file)
        if db_cfg.get("servants"):
            db_cfg["servants"] = [
                resolve_sqlite_url(url, config_file) for url in db_cfg["servants"]
            ]
    return config


def flag_mode_requested(args: argparse.Namespace) -> bool:
    """显式给了 --app-file / --namespace / --db 之一，就是命令行参数模式（不读配置文件）。
    --instance 单独出现不算：配置文件模式下它用来选实例。"""
    return any(
        getattr(args, name, None) is not None
        for name in ("app_file", "namespace", "db")
    )


def locate_config_file(args: argparse.Namespace) -> str | None:
    """
    找配置文件：``--config`` > ``$HETU_CONFIG`` > 当前目录的 ``config.yml``。命令行参数模式
    （见 `flag_mode_requested`）不找。都没有返回 None。
    """
    if getattr(args, "config", None):
        return args.config
    if flag_mode_requested(args):
        return None
    if env := os.environ.get(CONFIG_ENV):
        return env
    if os.path.exists(DEFAULT_CONFIG_FILE):
        return DEFAULT_CONFIG_FILE
    return None


def config_from_flags(
    args: argparse.Namespace,
    *,
    need_app: bool,
    need_db: bool,
    default_app_file: str | None = None,
    default_db: str | None = None,
) -> dict:
    """命令行参数模式：用 --app-file / --namespace / --instance / --db 拼出配置 dict"""
    app_file = getattr(args, "app_file", None) or default_app_file
    namespace = getattr(args, "namespace", None)
    instance = getattr(args, "instance", None)
    db = getattr(args, "db", None) or default_db
    missing = []
    if need_app and not app_file:
        missing.append("--app-file")
    if need_app and not namespace:
        missing.append("--namespace")
    if need_db and not db:
        missing.append("--db")
    if need_db and not instance:
        missing.append("--instance")
    if missing:
        raise ConfigError(
            _("命令行参数模式缺少：{missing}（或改用 --config）").format(
                missing=" ".join(missing)
            )
        )
    config: dict = {
        "APP_FILE": app_file,
        "NAMESPACE": namespace,
        "INSTANCES": [instance] if instance else [],
        "BACKENDS": {},
    }
    if db:
        backend_type = infer_backend_type_from_db_url(db)
        config["BACKENDS"][backend_type.capitalize()] = {
            "type": backend_type,
            "master": db,
        }
    return config


def load_command_config(
    args: argparse.Namespace, *, need_app: bool, need_db: bool
) -> tuple[dict, str | None]:
    """新命令的配置：``--config`` > 命令行参数模式 > ``$HETU_CONFIG`` > ``./config.yml``。
    返回 (配置 dict, 配置文件路径或 None)。"""
    config_file = locate_config_file(args)
    if config_file:
        return read_config_file(config_file), config_file
    if flag_mode_requested(args):
        return config_from_flags(args, need_app=need_app, need_db=need_db), None
    raise ConfigError(
        _(
            "找不到配置：用 --config 指定，或设环境变量 HETU_CONFIG，或在当前目录放 "
            "config.yml；也可以用 --app-file / --namespace / --instance / --db 直接给出"
        )
    )


def pick_instance(config: dict, instance: str | None) -> str:
    """--instance，默认 INSTANCES[0]；不在 INSTANCES 里报错"""
    instances = list(config.get("INSTANCES") or [])
    if not instances:
        raise ConfigError(_("配置里没有 INSTANCES"))
    if instance is None:
        return instances[0]
    if instance not in instances:
        raise UsageError(
            _("实例 {instance} 不在配置的 INSTANCES 里：{instances}").format(
                instance=instance, instances=instances
            )
        )
    return instance


def add_config_arguments(parser: argparse.ArgumentParser) -> None:
    """新命令（call / get / range / shell）共用的配置参数"""
    group = parser.add_argument_group(_("配置"))
    group.add_argument(
        "--config",
        metavar="config.yml",
        help=_("配置文件；不给时依次找环境变量 HETU_CONFIG、当前目录的 config.yml"),
    )
    group.add_argument(
        "--instance",
        metavar="server1",
        help=_("实例名，默认取配置里 INSTANCES 的第一个"),
    )
    group.add_argument(
        "--app-file", metavar="app.py", help=_("（无配置文件时）河图app的py文件")
    )
    group.add_argument(
        "--namespace", metavar="game1", help=_("（无配置文件时）app的namespace")
    )
    group.add_argument(
        "--db",
        metavar="redis://127.0.0.1:6379/0",
        help=_("（无配置文件时）后端数据库地址"),
    )


class CommandInterface:
    json_output = False
    """stdout 只输出一行 JSON 的命令：命令行用法错误也要写成 JSON（见 CommandIndex）"""

    @classmethod
    def name(cls):
        raise NotImplementedError("Subclasses should implement this method.")

    @classmethod
    def register(cls, subparsers):
        pass

    @classmethod
    def execute(cls, args):
        raise NotImplementedError("Subclasses should implement this method.")
