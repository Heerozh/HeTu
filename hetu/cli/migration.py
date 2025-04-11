from hetu.cli.base import CommandInterface


class MigrationCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "migration"

    @classmethod
    def register(cls, subparsers):
        # parser_start = subparsers.add_parser(
        #     'schema_migration', help='如果Component定义发生改变，在数据库执行版本迁移(未完成）')
        pass

    @classmethod
    def execute(cls, args):
        raise NotImplementedError("Subclasses should implement this method.")
