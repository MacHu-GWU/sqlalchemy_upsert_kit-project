# -*- coding: utf-8 -*-

from sqlalchemy_upsert_kit.utils import (
    get_utc_now,
    get_pk_name,
    clone_temp_table,
)

import pytest
from datetime import timezone
import sqlalchemy as sa


@pytest.fixture
def engine():
    engine = sa.create_engine(f"sqlite:///:memory:")
    yield engine


def test_get_utc_now():
    now = get_utc_now()
    assert now.tzinfo == timezone.utc


def test_get_pk_name_single_primary_key():
    metadata = sa.MetaData()
    table = sa.Table(
        "test_table",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(50)),
    )

    pk_name = get_pk_name(table)
    assert pk_name == "id"


def test_get_pk_name_no_primary_key():
    metadata = sa.MetaData()
    table = sa.Table(
        "test_table_no_pk",
        metadata,
        sa.Column("name", sa.String(50)),
        sa.Column("value", sa.Integer),
    )

    with pytest.raises(ValueError):
        get_pk_name(table)


def test_get_pk_name_multiple_primary_keys():
    metadata = sa.MetaData()
    table = sa.Table(
        "test_table_composite_pk",
        metadata,
        sa.Column("id1", sa.Integer, primary_key=True),
        sa.Column("id2", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(50)),
    )

    with pytest.raises(ValueError):
        get_pk_name(table)


def test_clone_temp_table_default_name():
    """
    Test clone_temp_table with auto-generated table name.
    """
    # Use the existing t_record table from test data
    metadata = sa.MetaData()
    table = sa.Table(
        "test_table",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(50)),
    )

    temp_metadata = sa.MetaData()

    temp_table = clone_temp_table(table, temp_metadata)

    # Verify it's a different table object
    assert temp_table is not table

    # Verify it has a temp name with timestamp
    assert temp_table.name.startswith("temp_")

    # Verify it has the same columns
    original_columns = {col.name: col.type.__class__ for col in table.columns}
    temp_columns = {col.name: col.type.__class__ for col in temp_table.columns}
    assert original_columns == temp_columns

    # Verify it uses the new metadata
    assert temp_table.metadata is temp_metadata
    assert temp_table.metadata is not table.metadata


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.utils",
        preview=False,
    )
