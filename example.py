from hetu.data import BaseComponent, define_component, property_field
from hetu.data.backend import Session
from hetu.system import define_system, Context, ResponseToClient


# 定义Component
@define_component
class Stock(BaseComponent):
    owner: int = property_field(0)
    value: int = property_field(0)


@define_component
class Order(BaseComponent):
    owner: int = property_field(0)
    paid: bool = property_field(False)
    qty: int = property_field(0)


@define_component
class Log(BaseComponent):
    info: str = property_field("", dtype="U32")


@define_system(
    namespace="example", components=(Stock, Order), depends=("log_pay:Order")
)
async def deliver_order(ctx: Context, order_id, paid):
    async with ctx.select[Order].upsert(id=order_id) as order:
        order.paid = paid
    async with ctx.select[Stock].upsert(owner=order.owner) as stock:
        stock.value += order.qty

    ctx.depend["log_pay"](ctx, order_id, paid)
    # ctx.session.commit()  提前提交事务 # 不对，不应该允许提前提交，而是让endpoint才能提前提交

    return ResponseToClient(["anything", "blah blah"])


@define_system(namespace="example", components=(Log,))
async def log_pay(ctx: Context, order_id, paid):
    log = Log.new_row()
    log.info = f"Order {order_id} paid {paid}"
    ctx.select[Log].insert(log)

    return ResponseToClient(["anything", "blah blah"])


# 如果直接调用system，就是类似自动生成一个小的endpoint
# 也可以自己写自己组织
@define_endpoint(namespace="example")
async def pay_callback(ctx: Context, session, order_id, paid):
    async for selects, depends in session.retry(deliver_order):
        ctx.select = selects
        ctx.depend = depends
        _ = deliver_order(ctx, order_id, paid)
    async for selects, depends in session.retry(log_pay):
        ctx.select = selects
        ctx.depend = depends
        _ = log_pay(ctx, order_id, paid)
