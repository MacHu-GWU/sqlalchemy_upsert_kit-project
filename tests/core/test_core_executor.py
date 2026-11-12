# -*- coding: utf-8 -*-

"""
:class:`~sqlalchemy_upsert_kit.core.executor.UpsertExecutor` is abstract; these
tests drive it through the SQLite executor. The full behavior contract runs in
each backend's strategy test files.
"""

from sqlalchemy_upsert_kit.core.executor import FAIL_STEPS
from sqlalchemy_upsert_kit.core.executor import UpsertExecutor

import pytest
import sqlalchemy as sa

from sqlalchemy_upsert_kit.exc import UpsertTestError
from sqlalchemy_upsert_kit.sqlite.executor import SqliteUpsertExecutor
from sqlalchemy_upsert_kit.tests.contract import INPUT
from sqlalchemy_upsert_kit.tests.contract import EXISTING_ROWS
from sqlalchemy_upsert_kit.tests.contract import EXPECTED_ROWS
from sqlalchemy_upsert_kit.tests.contract import assert_no_staging
from sqlalchemy_upsert_kit.tests.contract import read_rows
from sqlalchemy_upsert_kit.tests.contract import t_record


def new_executor(engine, **kwargs) -> UpsertExecutor:
    kwargs.setdefault("values", INPUT)
    kwargs.setdefault("strategy", "replace")
    return SqliteUpsertExecutor(engine=engine, table=t_record, **kwargs)


def test_defaults(engine):
    executor = new_executor(engine)
    assert isinstance(executor.metadata, sa.MetaData)
    assert executor.temp_table_name.startswith("temp_")
    assert executor.pk_column is t_record.c.id
    assert executor.update_keys == ["name", "note"]
    assert executor.user_managed is False


def test_run_auto_managed(engine):
    assert new_executor(engine).run() == (2, 1)
    assert read_rows(engine) == EXPECTED_ROWS["replace"]
    assert_no_staging(engine)


def test_run_user_managed(engine):
    with engine.connect() as conn:
        with conn.begin() as trans:
            executor = new_executor(engine, conn=conn, trans=trans)
            assert executor.user_managed is True
            assert executor.run() == (2, 1)
    assert read_rows(engine) == EXPECTED_ROWS["replace"]


def test_run_ignore_counts(engine):
    """
    Without update keys, the conflicts are the records that were not inserted.
    """
    assert new_executor(engine, strategy="ignore").run() == (2, 1)


class CountFirstExecutor(SqliteUpsertExecutor):
    """
    A backend whose row count cannot be trusted, such as MySQL.
    """

    rowcount_counts_inserts = False


@pytest.mark.parametrize("strategy", ["ignore", "replace"])
def test_run_counts_conflicts_first(engine, strategy):
    executor = CountFirstExecutor(
        engine=engine, table=t_record, values=INPUT, strategy=strategy
    )
    assert executor.run() == (2, 1)
    assert read_rows(engine) == EXPECTED_ROWS[strategy]


def test_default_hooks(engine):
    executor = new_executor(engine)
    assert executor.max_identifier_length() == engine.dialect.max_identifier_length
    assert UpsertExecutor.rowcount_counts_inserts is True


class BrokenCleanupExecutor(SqliteUpsertExecutor):
    def cleanup_after_rollback(self, conn, staging):
        raise RuntimeError("connection lost")


def test_failed_cleanup_keeps_the_error(engine):
    """
    If the cleanup after a failure fails too, the original error is raised,
    with the cleanup error as a note.
    """
    executor = BrokenCleanupExecutor(
        engine=engine,
        table=t_record,
        values=INPUT,
        strategy="replace",
        fail_at="upsert",
    )
    with pytest.raises(UpsertTestError) as exc_info:
        executor.run()
    assert exc_info.value.__notes__ == [
        "cleanup after the rollback also failed: RuntimeError('connection lost')"
    ]
    assert read_rows(engine) == EXISTING_ROWS


def test_run_empty_values(engine):
    assert new_executor(engine, values=[]).run() == (0, 0)


@pytest.mark.parametrize("fail_at", FAIL_STEPS)
def test_run_failure(engine, fail_at):
    executor = new_executor(engine, fail_at=fail_at)
    with pytest.raises(UpsertTestError, match=fail_at):
        executor.run()
    assert read_rows(engine) == EXISTING_ROWS
    assert executor.temp_table_name not in executor.metadata.tables
    assert_no_staging(engine)


@pytest.mark.parametrize(
    "kwargs, message",
    [
        (dict(conn="conn"), "both conn and trans"),
        (dict(trans="trans"), "both conn and trans"),
        (dict(fail_at="nowhere"), "fail_at must be one of"),
        (dict(temp_table_name="records"), "must differ"),
    ],
)
def test_invalid_arguments(engine, kwargs, message):
    with pytest.raises(ValueError, match=message):
        new_executor(engine, **kwargs)


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.core.executor",
        preview=False,
    )
