"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import sys

from hetu.cli.base import CommandInterface


class BuildCommand(CommandInterface):

    @classmethod
    def name(cls):
        return "build"

    @classmethod
    def register(cls, subparsers):
        parser_build = subparsers.add_parser('build', help='生成客户端SDK C# 类型代码')
        parser_build.add_argument(
            "--app-file", help="河图app的py文件", metavar=".app.py", default="/app/app.py")
        parser_build.add_argument(
            "--namespace", metavar="game1", help="编译app.py中哪个namespace下的数据类型",
            required=True)
        parser_build.add_argument(
            "--output", metavar="./Components.cs", help="输出文件路径",
            required=True)

    @classmethod
    def execute(cls, args):
        import importlib.util
        # 加载玩家的app文件
        spec = importlib.util.spec_from_file_location('HeTuApp', args.app_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules['HeTuApp'] = module
        spec.loader.exec_module(module)
        from hetu.system import SystemClusters
        SystemClusters().build_clusters(args.namespace)

        from hetu.sourcegen.csharp import generate_all_components
        generate_all_components(args.namespace, args.output)
        print(f"✅ 已生成C#代码到 {args.output}")
