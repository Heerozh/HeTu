import numpy as np
import time

import hetu


@hetu.define_component(namespace="Chat", permission=hetu.Permission.EVERYBODY)
class OnlineUser(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", unique=True, dtype="U32")
    online: bool = hetu.property_field(False)
    last_seen_ms: np.int64 = hetu.property_field(0)


@hetu.define_component(namespace="Chat", permission=hetu.Permission.EVERYBODY)
class ChatMessage(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, index=True)
    name: str = hetu.property_field("", dtype="U32")
    text: str = hetu.property_field("", dtype="U256")
    kind: str = hetu.property_field("chat", dtype="U16")
    created_at_ms: np.int64 = hetu.property_field(0, index=True)


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _insert_message(ctx: hetu.SystemContext, owner: int, name: str, text: str, kind: str):
    row = ChatMessage.new_row()
    row.owner = int(owner)
    row.name = str(name)[:32]
    row.text = str(text)[:256]
    row.kind = str(kind)[:16]
    row.created_at_ms = _now_ms()
    await ctx.repo[ChatMessage].insert(row)


@hetu.define_system(
    namespace="Chat",
    components=(OnlineUser, ChatMessage),
    permission=hetu.Permission.EVERYBODY,
)
async def user_login(ctx: hetu.SystemContext, user_id: int, name: str):
    await hetu.elevate(ctx, int(user_id), kick_logged_in=True)
    username = str(name)[:32]
    async with ctx.repo[OnlineUser].upsert(owner=ctx.caller) as row:
        row.name = username
        row.online = True
        row.last_seen_ms = _now_ms()

    await _insert_message(
        ctx,
        owner=ctx.caller,
        name=username,
        text=f"{username} joined the chat",
        kind="system",
    )


@hetu.define_system(
    namespace="Chat",
    components=(OnlineUser, ChatMessage),
    permission=hetu.Permission.USER,
)
async def user_quit(ctx: hetu.SystemContext):
    if row := await ctx.repo[OnlineUser].get(owner=ctx.caller):
        username = row.name
        async with ctx.repo[OnlineUser].upsert(owner=ctx.caller) as current:
            current.name = username
            current.online = False
            current.last_seen_ms = _now_ms()

        await _insert_message(
            ctx,
            owner=ctx.caller,
            name=username,
            text=f"{username} left the chat",
            kind="system",
        )


@hetu.define_system(
    namespace="Chat",
    components=(OnlineUser, ChatMessage),
    permission=hetu.Permission.USER,
)
async def user_chat(ctx: hetu.SystemContext, text: str):
    me = await ctx.repo[OnlineUser].get(owner=ctx.caller)
    assert me and me.online, "call user_login first"
    await _insert_message(
        ctx,
        owner=ctx.caller,
        name=me.name,
        text=text,
        kind="chat",
    )


@hetu.define_system(
    namespace="Chat",
    components=(OnlineUser,),
    depends=("user_quit",),
    permission=None,
)
async def on_disconnect(ctx: hetu.SystemContext):
    await ctx.depend["user_quit"](ctx)
