"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import gettext
import locale
import sys
from importlib.resources import files

from autolang.config import get_domain
from babel import Locale
from babel.support import Format

DEFAULT_I18N_DIR = str(files("hetu").joinpath("i18n"))


def get_system_language() -> str:
    """Return the system UI language as a short code (e.g. 'en', 'zh')."""
    # 优先从环境变量获取
    try:
        env_default = Locale.default()
        if env_default:
            print(f"Use language defined by ENV (LANG, LC_*): {str(env_default)}")
            return str(env_default)
    except TypeError:
        pass

    # 获取系统语言
    if sys.platform == "win32":
        import ctypes

        lang_id = ctypes.windll.kernel32.GetUserDefaultUILanguage()
        # locale.windows_locale 将 Windows LCID 映射到 POSIX locale
        posix = locale.windows_locale.get(lang_id)
    else:
        posix = locale.getlocale()[0]

    if posix:
        print(f"Use system language: {str(posix)}")
        return posix

    print("Use fallback language: en")
    return "en"


def get_translator(language=None, directory: str = DEFAULT_I18N_DIR):
    language_list = None
    if language:
        language_list = [language, "en"]

    return gettext.translation(
        get_domain(),
        localedir=directory,
        languages=language_list,
        fallback=True,
    ), Format(Locale.parse(language))


translator, fmt = get_translator(get_system_language())
_ = translator.gettext


__all__ = ["_", "fmt"]
