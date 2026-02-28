import numpy as np

import hetu


@hetu.define_component(namespace="Chat", permission=hetu.Permission.EVERYBODY)
class OnlineUser(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", unique=True, dtype="U32")


@hetu.define_component(namespace="Chat", permission=hetu.Permission.EVERYBODY)
class ChatMessage(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, index=True)
    name: str = hetu.property_field("", dtype="U32")
    text: str = hetu.property_field("", dtype="U256")


@hetu.define_system(
    namespace="Chat", components=(OnlineUser,), permission=hetu.Permission.EVERYBODY
)
async def user_login(ctx: hetu.SystemContext, user_id: int, name: str):
    await hetu.elevate(ctx, int(user_id), kick_logged_in=True)
    async with ctx.repo[OnlineUser].upsert(owner=ctx.caller) as row:
        row.name = str(name)[:32]


@hetu.define_system(
    namespace="Chat", components=(OnlineUser,), permission=hetu.Permission.USER
)
async def user_quit(ctx: hetu.SystemContext):
    if row := await ctx.repo[OnlineUser].get(owner=ctx.caller):
        ctx.repo[OnlineUser].delete(row.id)


@hetu.define_system(
    namespace="Chat",
    components=(OnlineUser, ChatMessage),
    permission=hetu.Permission.USER,
)
async def user_chat(ctx: hetu.SystemContext, text: str):
    me = await ctx.repo[OnlineUser].get(owner=ctx.caller)
    assert me, "call user_login first"
    row = ChatMessage.new_row()
    row.owner, row.name, row.text = ctx.caller, me.name, str(text)[:256]
    await ctx.repo[ChatMessage].insert(row)


@hetu.define_system(
    namespace="Chat",
    components=(OnlineUser,),
    depends=("user_quit",),
    permission=None,
)
async def on_disconnect(ctx: hetu.SystemContext):
    await ctx.depend["user_quit"](ctx)
