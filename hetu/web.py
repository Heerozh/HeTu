from sanic import Blueprint
from sanic import Request, text
import hetu


APP_BLUEPRINT = Blueprint("my_blueprint")


@APP_BLUEPRINT.route("/")
async def web_root(request: Request):
    return text(f"Powered by HeTu(v{hetu.__version__}) Database! ")
