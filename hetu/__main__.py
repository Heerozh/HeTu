"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
from hetu.cli import CommandIndex


def main():
    cli = CommandIndex()
    cli.register()
    return cli.execute()


if __name__ == "__main__":
    main()
