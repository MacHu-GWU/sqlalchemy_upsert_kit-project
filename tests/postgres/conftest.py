# -*- coding: utf-8 -*-

import pytest
import sqlalchemy as sa

from sqlalchemy_upsert_kit.tests.data import Base
from sqlalchemy_upsert_kit.tests.data import DataFaker


@pytest.fixture(scope="session")
def pg_engine(pg_url) -> sa.Engine:
    """
    Engine of the PostgreSQL test database, shared by the whole session.

    The session time zone is UTC, so naive ``timestamp`` columns round-trip
    the timezone-aware test datetimes the same way on every machine.
    """
    engine = sa.create_engine(
        pg_url,
        connect_args={"options": "-c timezone=UTC"},
    )
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
    except sa.exc.OperationalError as e:  # pragma: no cover
        pytest.fail(
            f"Cannot connect to the PostgreSQL test database at {engine.url!r}. "
            f"Run 'mise run psql-up' first.\n{e}"
        )
    yield engine
    engine.dispose()


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
def clean_database(pg_engine, data_faker):
    """
    Recreate the test tables and load the existing records before each test.
    """
    Base.metadata.drop_all(pg_engine)
    Base.metadata.create_all(pg_engine)
    data_faker.prepare_existing_data(pg_engine)
    yield pg_engine


@pytest.fixture
def error_scenarios():
    return [
        ("_raise_on_temp_table_create", "temp_create_test"),
        ("_raise_on_temp_data_insert", "temp_data_test"),
        ("_raise_on_target_insert", "temp_target_test"),
        ("_raise_on_temp_table_drop", "temp_drop_test"),
    ]
