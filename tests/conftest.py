# -*- coding: utf-8 -*-

import os
import sys
import pytest

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
