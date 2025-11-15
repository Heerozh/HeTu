from hetu.data import BaseComponent, Property, define_component, Permission
from hetu.system import define_system, Context, ResponseToClient
import numpy as np
import asyncio
import logging
logger = logging.getLogger('HeTu.root')


# 首先定于game_short_name的namespace，让CONFIG_TEMPLATE.yml能启动
@define_system(namespace="game_short_name")
async def do_nothing(ctx: Context, sleep):
    pass