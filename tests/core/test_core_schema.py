# -*- coding: utf-8 -*-

from sqlalchemy_upsert_kit.core.schema import get_pk_column
from sqlalchemy_upsert_kit.core.schema import get_writable_columns
from sqlalchemy_upsert_kit.core.schema import make_staging_name
from sqlalchemy_upsert_kit.core.schema import make_staging_table

import pytest
import sqlalchemy as sa

from sqlalchemy_upsert_kit.tests.contract import t_child
from sqlalchemy_upsert_kit.tests.contract import t_long
from sqlalchemy_upsert_kit.tests.contract import t_rich


def test_get_pk_column():
    assert get_pk_column(t_rich) is t_rich.c.code


@pytest.mark.parametrize("n_pk", [0, 2])
def test_get_pk_column_needs_exactly_one(n_pk):
    columns = [sa.Column(f"c{i}", sa.Integer, primary_key=i < n_pk) for i in range(2)]
    table = sa.Table("t", sa.MetaData(), *columns)
    with pytest.raises(ValueError):
        get_pk_column(table)


def test_get_writable_columns():
    assert "total" not in [col.key for col in get_writable_columns(t_rich)]


def test_make_staging_name():
    name_1 = make_staging_name(t_long, max_length=63)
    name_2 = make_staging_name(t_long, max_length=63)
    assert name_1 != name_2
    assert len(name_1) == 63
    assert name_1.startswith("temp_")


def test_make_staging_table():
    """
    Writable columns with their keys, defaults and primary key; nothing else.
    """
    staging = make_staging_table(t_rich, sa.MetaData(), "stg")
    assert list(staging.columns.keys()) == ["code", "sel", "status", "score", "label"]
    assert staging.c.sel.name == "select"
    assert [col.key for col in staging.primary_key] == ["code"]
    assert staging.c.code.autoincrement is False
    assert staging.c.status.server_default.arg == "new"
    assert staging.c.score.default.arg == 7
    assert not staging.indexes
    assert not [c for c in staging.constraints if isinstance(c, sa.UniqueConstraint)]
    assert staging._prefixes == ["TEMPORARY"]
    assert not make_staging_table(t_child, sa.MetaData(), "stg").foreign_keys


def test_make_staging_table_skips_sequences():
    table = sa.Table(
        "t",
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("seq", sa.Integer, sa.Sequence("t_seq")),
    )
    assert make_staging_table(table, sa.MetaData(), "stg").c.seq.default is None


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.core.schema",
        preview=False,
    )
