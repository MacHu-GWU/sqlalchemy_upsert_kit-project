# -*- coding: utf-8 -*-

import pytest
import sqlalchemy as sa

from sqlalchemy_upsert_kit.paths import dir_project_root
from sqlalchemy_upsert_kit.tests.data import (
    Base,
    DataFaker,
)

dir_tmp = dir_project_root / "tmp"
path_sqlite = dir_tmp / "test.sqlite"


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
def clean_database(data_faker):
    """
    Fixture to ensure clean database state for each test.
    """
    dir_tmp.mkdir(parents=True, exist_ok=True)
    path_sqlite.unlink(missing_ok=True)
    engine = sa.create_engine(f"sqlite:///{path_sqlite}")
    Base.metadata.create_all(engine)
    data_faker.prepare_existing_data(engine)
    yield engine

    pass
