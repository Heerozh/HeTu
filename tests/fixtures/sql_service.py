import time
from pathlib import Path

import docker
import docker.errors
import pytest
import sqlalchemy as sa
from docker.errors import NotFound


@pytest.fixture(scope="session")
def ses_postgres_service():
    """
    启动postgres docker服务，测试结束后销毁服务
    """
    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        return pytest.skip("请启动DockerDesktop或者Docker服务后再运行测试")

    container_name = "hetu_test_postgres"
    port = 23520

    # 先删除已启动的
    try:
        client.containers.get(container_name).kill()
        client.containers.get(container_name).remove()
    except docker.errors.NotFound, docker.errors.APIError:
        pass

    # 启动服务器
    container = client.containers.run(
        "postgres:latest",
        detach=True,
        ports={"5432/tcp": port},
        name=container_name,
        auto_remove=True,
        environment={
            "POSTGRES_USER": "hetu",
            "POSTGRES_PASSWORD": "hetu_test",
            "POSTGRES_DB": "hetu_test",
        },
    )

    # 验证docker启动完毕
    dsn = f"postgresql://hetu:hetu_test@127.0.0.1:{port}/hetu_test"
    for _ in range(30):
        try:
            time.sleep(1)
            sync_dsn = f"postgresql+psycopg://hetu:hetu_test@127.0.0.1:{port}/hetu_test"
            engine = sa.create_engine(sync_dsn, future=True)
            with engine.connect() as conn:
                ver = conn.execute(sa.text("select version()")).scalar_one()
                print(f"PostgreSQL version: {ver}")
            engine.dispose()
            break
        except Exception:
            pass
    else:
        raise Exception("PostgreSQL启动超时，无法连接")

    print("⚠️ 已启动postgres docker.")

    yield dsn

    print("ℹ️ 清理postgres docker...")
    try:
        container.stop()
        container.wait()
    except NotFound, ImportError, docker.errors.APIError:
        pass


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
    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        return pytest.skip("请启动DockerDesktop或者Docker服务后再运行测试")

    container_name = "hetu_test_mariadb"
    port = 23530

    try:
        client.containers.get(container_name).kill()
        client.containers.get(container_name).remove()
    except docker.errors.NotFound, docker.errors.APIError:
        pass

    container = client.containers.run(
        "mariadb:latest",
        detach=True,
        ports={"3306/tcp": port},
        name=container_name,
        auto_remove=True,
        environment={
            "MARIADB_USER": "hetu",
            "MARIADB_PASSWORD": "hetu_test",
            "MARIADB_DATABASE": "hetu_test",
            "MARIADB_ROOT_PASSWORD": "hetu_root",
        },
    )

    dsn = f"mysql://hetu:hetu_test@127.0.0.1:{port}/hetu_test"
    for _ in range(40):
        try:
            time.sleep(1)
            sync_dsn = f"mysql+pymysql://hetu:hetu_test@127.0.0.1:{port}/hetu_test"
            engine = sa.create_engine(sync_dsn, future=True)
            with engine.connect() as conn:
                ver = conn.execute(sa.text("select version()")).scalar_one()
                print(f"MariaDB version: {ver}")
            engine.dispose()
            break
        except Exception:
            pass
    else:
        raise Exception("MariaDB启动超时，无法连接")

    print("⚠️ 已启动mariadb docker.")

    yield dsn

    print("ℹ️ 清理mariadb docker...")
    try:
        container.stop()
        container.wait()
    except NotFound, ImportError, docker.errors.APIError:
        pass
