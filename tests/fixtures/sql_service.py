from pathlib import Path

import pytest
import sqlalchemy as sa
from fixtures.docker_infra import (
    LOCAL_RANDOM_PORT,
    DockerStack,
    docker_stack,
    host_port,
)


def _wait_sql_ready(stack: DockerStack, sync_dsn: str, what: str, **connect_args):
    """轮询到数据库能执行 select version() 为止"""
    engine = sa.create_engine(sync_dsn, future=True, connect_args=connect_args)

    def ready():
        with engine.connect() as conn:
            ver = conn.execute(sa.text("select version()")).scalar_one()
        print(f"{what} version: {ver}")
        return True

    try:
        stack.wait_until(ready, what, timeout=120)
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def ses_postgres_service():
    """
    启动postgres docker服务，测试结束后销毁服务
    """
    with docker_stack("postgres") as stack:
        container = stack.run(
            "db",
            "postgres:latest",
            ports={"5432/tcp": LOCAL_RANDOM_PORT},
            environment={
                "POSTGRES_USER": "hetu",
                "POSTGRES_PASSWORD": "hetu_test",
                "POSTGRES_DB": "hetu_test",
            },
        )
        addr = f"hetu:hetu_test@127.0.0.1:{host_port(container, '5432/tcp')}/hetu_test"
        _wait_sql_ready(
            stack, f"postgresql+psycopg://{addr}", "PostgreSQL", connect_timeout=2
        )
        print("⚠️ 已启动postgres docker.")

        yield f"postgresql://{addr}"


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


@pytest.fixture(scope="session")
def ses_mariadb_service():
    """
    启动mariadb docker服务，测试结束后销毁服务
    """
    with docker_stack("mariadb") as stack:
        container = stack.run(
            "db",
            "mariadb:latest",
            ports={"3306/tcp": LOCAL_RANDOM_PORT},
            environment={
                "MARIADB_USER": "hetu",
                "MARIADB_PASSWORD": "hetu_test",
                "MARIADB_DATABASE": "hetu_test",
                "MARIADB_ROOT_PASSWORD": "hetu_root",
            },
        )
        addr = f"hetu:hetu_test@127.0.0.1:{host_port(container, '3306/tcp')}/hetu_test"
        _wait_sql_ready(stack, f"mysql+pymysql://{addr}", "MariaDB", connect_timeout=2)
        print("⚠️ 已启动mariadb docker.")

        yield f"mysql://{addr}"
