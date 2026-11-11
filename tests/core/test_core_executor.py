# -*- coding: utf-8 -*-

"""
:class:`~sqlalchemy_upsert_kit.core.executor.UpsertExecutor` is abstract.
These tests drive its shared workflow (staging table lifecycle, both
transaction modes, failure cleanup) through a minimal concrete strategy on a
SQLite file. The real strategies are tested in each backend's test folder.
"""

from sqlalchemy_upsert_kit.core.executor import UpsertExecutor

import dataclasses

import pytest
import sqlalchemy as sa

from sqlalchemy_upsert_kit.exc import UpsertTestError
from sqlalchemy_upsert_kit.tests.data import Base
from sqlalchemy_upsert_kit.tests.data import DataFaker
from sqlalchemy_upsert_kit.tests.data import t_record


@dataclasses.dataclass
class CopyAllExecutor(UpsertExecutor):
    """
    Minimal strategy: copy every staged row into the target table.
    """

    def apply_strategy(
        self,
        conn: sa.Connection,
        trans: sa.Transaction,
    ):
        if self._raise_on_target_insert:  # Testing flag
            raise UpsertTestError("error on target insertion")
        stmt = self.table.insert().from_select(
            list(self._temp_table.columns.keys()),
            sa.select(self._temp_table),
        )
        res = conn.execute(stmt)
        self._inserted_rows = res.rowcount


@pytest.fixture
def engine(tmp_path) -> sa.Engine:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'test.sqlite'}")
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def faker() -> DataFaker:
    # no conflicts, so CopyAllExecutor never hits a primary key violation
    return DataFaker(n_existing=2, n_input=3, n_conflict=0)


def test_new_auto_managed():
    engine = sa.create_engine("sqlite:///:memory:")
    executor = CopyAllExecutor.new(
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
        executor = CopyAllExecutor.new(
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


def test_run_auto_managed(engine, faker):
    faker.prepare_existing_data(engine)
    executor = CopyAllExecutor.new(
        engine=engine,
        table=t_record,
        values=faker.input_data,
    )
    ignored, inserted = executor.run()
    assert (ignored, inserted) == (0, faker.n_input)
    faker.check_all_data(faker.get_all_records(engine))
    faker.check_no_temp_tables(engine)
    assert executor._temp_table_created is False


def test_run_user_managed(engine, faker):
    faker.prepare_existing_data(engine)
    with engine.connect() as conn:
        with conn.begin() as trans:
            executor = CopyAllExecutor.new(
                engine=engine,
                table=t_record,
                values=faker.input_data,
                conn=conn,
                trans=trans,
            )
            executor.run()
    faker.check_all_data(faker.get_all_records(engine))
    faker.check_no_temp_tables(engine)


@pytest.mark.parametrize(
    "flag_name",
    [
        "_raise_on_temp_table_create",
        "_raise_on_temp_data_insert",
        "_raise_on_target_insert",
        "_raise_on_temp_table_drop",
    ],
)
def test_run_auto_managed_failure(engine, faker, flag_name):
    """
    Every failure point rolls back the data and leaves no staging table.
    """
    faker.prepare_existing_data(engine)
    executor = CopyAllExecutor.new(
        engine=engine,
        table=t_record,
        values=faker.input_data,
        **{flag_name: True},
    )
    with pytest.raises(UpsertTestError):
        executor.run()
    faker.check_rollback(engine)
    faker.check_no_temp_tables(engine)


@pytest.mark.parametrize(
    "flag_name",
    [
        "_raise_on_temp_data_insert",
        pytest.param(
            "_raise_on_target_insert",
            marks=pytest.mark.xfail(
                strict=True,
                reason=(
                    "Known SQLite bug: when the caller's transaction has not written "
                    "yet, CREATE TABLE autocommits; after the staged rows are written, "
                    "the fresh cleanup connection is blocked by the caller's write lock, "
                    "times out, and the staging table is left behind."
                ),
            ),
        ),
    ],
)
def test_run_user_managed_failure(engine, faker, flag_name):
    faker.prepare_existing_data(engine)
    with pytest.raises(UpsertTestError):
        with engine.connect() as conn:
            with conn.begin() as trans:
                CopyAllExecutor.new(
                    engine=engine,
                    table=t_record,
                    values=faker.input_data,
                    conn=conn,
                    trans=trans,
                    **{flag_name: True},
                ).run()
    faker.check_rollback(engine)
    faker.check_no_temp_tables(engine)


def test_cleanup_temp_table_on_failure_when_already_dropped(engine):
    """
    A failed drop (the table is already gone) is swallowed, and the table
    is still removed from the metadata.
    """
    executor = CopyAllExecutor.new(
        engine=engine,
        table=t_record,
        values=[],
    )
    executor.clone_temp_table()
    executor._temp_table_created = True  # pretend it was created, but it was not
    executor.cleanup_temp_table_on_failure()
    assert executor.temp_table_name not in executor.metadata.tables


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.core.executor",
        preview=False,
    )
