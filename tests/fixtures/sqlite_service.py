from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def ses_sqlite_service(tmp_path_factory):
    """
    创建sqlite测试数据库文件，测试结束后自动清理临时目录
    """
    db_dir = tmp_path_factory.mktemp("hetu_sqlite")
    db_file = Path(db_dir) / "hetu_test.sqlite3"
    dsn = f"sqlite:///{db_file.as_posix()}"
    print(f"⚠️ 已创建sqlite测试库: {db_file}")
    yield dsn
