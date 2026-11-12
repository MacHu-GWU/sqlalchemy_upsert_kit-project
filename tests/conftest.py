# -*- coding: utf-8 -*-

import os
import sys

import pytest
import sqlalchemy as sa

from sqlalchemy_upsert_kit.tests.contract import reset

# ------------------------------------------------------------------------------
# Local PostgreSQL test container, started by ``mise run psql-up``.
#
# ``.mise/tasks/psql.py`` reads the literal values below with ``ast``,
# so keep each one a plain literal assignment.
# ------------------------------------------------------------------------------
PG_IMAGE = "docker.io/library/postgres:18"
PG_CONTAINER_NAME = "sqlalchemy_upsert_kit-psql"
PG_HOST = "127.0.0.1"
PG_PORT = 45432  # unusual port, so it does not clash with a local PostgreSQL on 5432
PG_USER = "postgres"
PG_PASSWORD = "password"
PG_DATABASE = "postgres"
PG_URL = (
    f"postgresql+psycopg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)
# CI has no container: it starts the runner's preinstalled PostgreSQL and sets
# this environment variable to its URL instead.
ENV_PG_URL = "SQLALCHEMY_UPSERT_KIT_PG_URL"

# ------------------------------------------------------------------------------
# Local MySQL test container, started by ``mise run mysql-up``.
#
# ``.mise/tasks/mysql.py`` reads the literal values below with ``ast``,
# so keep each one a plain literal assignment.
# ------------------------------------------------------------------------------
MYSQL_IMAGE = "docker.io/library/mysql:8.4"
MYSQL_CONTAINER_NAME = "sqlalchemy_upsert_kit-mysql"
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 43306  # unusual port, so it does not clash with a local MySQL on 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "test"
MYSQL_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)
# CI has no container: it starts the runner's preinstalled MySQL and sets
# this environment variable to its URL instead.
ENV_MYSQL_URL = "SQLALCHEMY_UPSERT_KIT_MYSQL_URL"


@pytest.fixture(autouse=True)
def ensure_newline_before_test_output():
    sys.stdout.write("\n")
    sys.stdout.flush()


@pytest.fixture(scope="session")
def pg_url() -> str:
    """
    SQLAlchemy URL of the PostgreSQL test database.
    """
    return os.environ.get(ENV_PG_URL, PG_URL)


@pytest.fixture(scope="session")
def mysql_url() -> str:
    """
    SQLAlchemy URL of the MySQL test database.
    """
    return os.environ.get(ENV_MYSQL_URL, MYSQL_URL)


def _enable_sqlite_foreign_keys(dbapi_connection, connection_record):
    dbapi_connection.execute("PRAGMA foreign_keys = ON")


@pytest.fixture
def engine(db_url) -> sa.Engine:
    """
    A fresh contract database (``reset``) on the backend that the test
    folder's ``db_url`` fixture names.

    The pool holds exactly one connection, so after an auto-managed call a test
    inspects the very connection the call used. SQLite enforces foreign keys,
    so a strategy that deleted a parent row would cascade, and fail the test.
    """
    engine = sa.create_engine(db_url, pool_size=1, max_overflow=0)
    if engine.dialect.name == "sqlite":
        sa.event.listen(engine, "connect", _enable_sqlite_foreign_keys)
    reset(engine)
    yield engine
    engine.dispose()
