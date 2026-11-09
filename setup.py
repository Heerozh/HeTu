"""
只为构建钩子存在，包的元数据都在 pyproject.toml。

.mo 不进 git（.gitignore），打 wheel 时由这里把 hetu/i18n 下各语言的 messages.po
编译进包；没有 .mo 时 hetu.i18n 的 gettext 静默回落成原文（中文），译文等于没发出去。
"""

from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py


class BuildPyWithCatalogs(build_py):
    """build_py 跑完后把 .po 编译成 .mo 写进 build_lib。

    读源码树里的 .po 而不是 build_lib 里的：Docker 镜像构建时没有 .git，
    setuptools-scm 列不出受版本控制的文件，.po 未必会被拷进 build_lib。
    只写 build_lib、不碰源码树。editable 安装（uv sync，本地开发和 CI 跑测试）
    不编：测试固定按 zh_CN 跑、断言的是中文原文，编了 zh 的 .mo 会被译文顶掉。
    """

    def run(self):
        super().run()
        if self.editable_mode or self.dry_run:
            return
        from babel.messages.mofile import write_mo
        from babel.messages.pofile import read_po

        for po, mo in self._catalogs():
            with po.open("rb") as f:
                catalog = read_po(f, locale=po.parent.parent.name)
            mo.parent.mkdir(parents=True, exist_ok=True)
            with mo.open("wb") as f:
                write_mo(f, catalog)

    def get_outputs(self, include_bytecode=True):
        outputs = super().get_outputs(include_bytecode)
        if self.editable_mode:
            return outputs
        return outputs + [str(mo) for _, mo in self._catalogs()]

    def _catalogs(self):
        src = Path(self.get_package_dir("hetu"), "i18n")
        dst = Path(self.build_lib, "hetu", "i18n")
        for po in sorted(src.glob("*/LC_MESSAGES/*.po")):
            yield po, dst / po.relative_to(src).with_suffix(".mo")


setup(cmdclass={"build_py": BuildPyWithCatalogs})
