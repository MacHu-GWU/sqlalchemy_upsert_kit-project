# -*- coding: utf-8 -*-

"""
:class:`~sqlalchemy_upsert_kit.sqlite.executor.UpsertExecutor` is abstract.
Its full run is exercised through the concrete executors in the
``test_sqlite_insert_or_*.py`` files; this file tests the shared setup logic.
"""

from sqlalchemy_upsert_kit.sqlite.executor import UpsertExecutor

import sqlalchemy as sa

from sqlalchemy_upsert_kit.sqlite.insert_or_ignore import InsertOrIgnoreExecutor
from sqlalchemy_upsert_kit.tests.data import t_record


def test_new_auto_managed():
    engine = sa.create_engine("sqlite:///:memory:")
    executor = InsertOrIgnoreExecutor.new(
        engine=engine,
        table=t_record,
        values=[],
    )
    assert isinstance(executor, UpsertExecutor)
    assert executor.auto_managed is True
    assert executor.user_managed is False
    assert isinstance(executor.metadata, sa.MetaData)
    assert executor.temp_table_name.startswith("temp_")
    assert executor.temp_table_name.endswith(t_record.name)
    assert executor.pk_name == "id"


def test_new_user_managed():
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        trans = conn.begin()
        executor = InsertOrIgnoreExecutor.new(
            engine=engine,
            table=t_record,
            values=[],
            temp_table_name="my_temp_table",
            conn=conn,
            trans=trans,
        )
        assert executor.user_managed is True
        assert executor.auto_managed is False
        assert executor.temp_table_name == "my_temp_table"
        trans.rollback()


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.sqlite.executor",
        preview=False,
    )
