"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import importlib.resources

# 初始 app.py 模板。__NAMESPACE__ 在渲染时被替换为项目的 namespace。
APP_PY_TEMPLATE = '''\
"""__NAMESPACE__ — HeTu starter app. 在此定义你的 Component 和 System。"""

import numpy as np

import hetu


@hetu.define_component(
    namespace="__NAMESPACE__", permission=hetu.Permission.EVERYBODY
)
class Player(hetu.BaseComponent):
    """玩家数据表。/ The player data table."""

    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", dtype="U32")
    online: bool = hetu.property_field(False)


@hetu.define_system(
    namespace="__NAMESPACE__",
    components=(Player,),
    permission=hetu.Permission.EVERYBODY,
)
async def login(ctx: hetu.SystemContext, user_id: int, name: str):
    """客户端通过 callSystem('login', user_id, name) 登录。"""
    await hetu.elevate(ctx, int(user_id), kick_logged_in=True)
    async with ctx.repo[Player].upsert(owner=ctx.caller) as row:
        row.name = name
        row.online = True


@hetu.define_system(
    namespace="__NAMESPACE__",
    components=(Player,),
    permission=None,
)
async def on_disconnect(ctx: hetu.SystemContext):
    """连接断开时由引擎自动调用，客户端无法直接调用。"""
    if row := await ctx.repo[Player].get(owner=ctx.caller):
        row.online = False
        await ctx.repo[Player].update(row)
'''


def render_app_py(namespace: str) -> str:
    """渲染初始 app.py 内容。"""
    return APP_PY_TEMPLATE.replace("__NAMESPACE__", namespace)


def read_config_template() -> str:
    """读取打包在 hetu 包内的 CONFIG_TEMPLATE.yml。"""
    resource = importlib.resources.files("hetu").joinpath("CONFIG_TEMPLATE.yml")
    return resource.read_text(encoding="utf-8")


def render_config(template_text: str, namespace: str, app_file: str) -> str:
    """根据 namespace 与 app 文件路径渲染 config.yml，保留模板注释。"""
    text = template_text.replace(
        "NAMESPACE: game_short_name",
        f"NAMESPACE: {namespace}",
    )
    text = text.replace("APP_FILE: app.py", f"APP_FILE: {app_file}")
    return text
