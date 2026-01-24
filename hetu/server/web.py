from sanic import Blueprint, Request, text

import hetu

HETU_BLUEPRINT = Blueprint("HeTuDB")


@HETU_BLUEPRINT.route("/")
async def web_root(request: Request):
    return text(f"Powered by HeTu(v{hetu.__version__}) Database! ")
