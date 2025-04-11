from hetu.cli.base import CommandInterface


class UnlockCommand(CommandInterface):

    @classmethod
    def name(cls):
        return "unlock"

    @classmethod
    def remove_head_lock(cls, url):
        import redis
        r = redis.Redis.from_url(url)
        r.delete('head_lock')

    @classmethod
    def register(cls, subparsers):
        parser_unlock = subparsers.add_parser('unlock', help='解锁head_lock，用于服务器非正常关闭')
        parser_unlock.add_argument(
            "--db", metavar="redis://127.0.0.1:6379/0", help="后端数据库地址",
            default='redis://127.0.0.1:6379/0')

    @classmethod
    def execute(cls, args):
        cls.remove_head_lock(args.db)
        print("🔓 已解锁head_lock")
