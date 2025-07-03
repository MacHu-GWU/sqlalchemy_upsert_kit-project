# -*- coding: utf-8 -*-
import os

import pytest
import sqlalchemy as sa

from sqlalchemy_upsert_kit.paths import dir_project_root
from sqlalchemy_upsert_kit.tests.data import (
    Base,
    DataFaker,
)

dir_tmp = dir_project_root / "tmp"
path_sqlite = dir_tmp / "test.sqlite"

dialects_no_transactions = [
    "crate",
]


@pytest.fixture
def database_url() -> str:
    """
    export DATABASE_URL=crate://
    """
    return os.environ.get("DATABASE_URL", f"sqlite:///{path_sqlite}")


@pytest.fixture
def engine(database_url) -> sa.Engine:
    engine = sa.create_engine(database_url)
    if database_url.startswith("crate://"):
        # uv pip install --upgrade 'sqlalchemy-cratedb>=0.42.0.dev2'
        from sqlalchemy_cratedb.support import refresh_after_dml
        refresh_after_dml(engine)
    return engine


@pytest.fixture(autouse=True)
def skip_transactions(request, database_url):
    """
    https://stackoverflow.com/a/28198398
    """
    for dialect in dialects_no_transactions:
        if database_url.startswith(dialect):
            if "rollback" in request.node.name:
                pytest.skip('skipped for dialect: {}'.format(dialect))


@pytest.fixture
def data_faker():
    """
    Fixture providing a standard DataFaker configuration for tests.
    """
    return DataFaker(
        n_existing=4,
        n_input=3,
        n_conflict=2,
    )


@pytest.fixture
def clean_database(engine, data_faker):
    """
    Fixture to ensure clean database state for each test.
    """
    dir_tmp.mkdir(parents=True, exist_ok=True)
    try:
        path_sqlite.unlink(missing_ok=True)
    except:
        pass
    Base.metadata.create_all(engine)
    data_faker.prepare_existing_data(engine)
    yield engine

    pass


@pytest.fixture
def error_scenarios():
    return [
        ("_raise_on_temp_table_create", "temp_create_test"),
        ("_raise_on_temp_data_insert", "temp_data_test"),
        ("_raise_on_target_insert", "temp_target_test"),
        ("_raise_on_temp_table_drop", "temp_drop_test"),
    ]
