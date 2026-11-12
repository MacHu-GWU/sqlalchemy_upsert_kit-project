# -*- coding: utf-8 -*-

"""
The MySQL-specific hooks of the executor, each on its own. The full workflow
runs in the strategy test files, through the shared contract.
"""

from sqlalchemy_upsert_kit.mysql.executor import MAX_IDENTIFIER_LENGTH
from sqlalchemy_upsert_kit.mysql.executor import MysqlUpsertExecutor

import pytest
import sqlalchemy as sa

from sqlalchemy_upsert_kit.exc import UpsertTestError
from sqlalchemy_upsert_kit.exc import UniqueKeyConflictError
from sqlalchemy_upsert_kit.core.schema import make_staging_table
from sqlalchemy_upsert_kit.tests.contract import EXISTING_ROWS
from sqlalchemy_upsert_kit.tests.contract import INPUT
from sqlalchemy_upsert_kit.tests.contract import read_rows
from sqlalchemy_upsert_kit.tests.contract import staging_tables
from sqlalchemy_upsert_kit.tests.contract import t_long
from sqlalchemy_upsert_kit.tests.contract import t_record
from sqlalchemy_upsert_kit.tests.contract import t_rich


def new_executor(**kwargs) -> MysqlUpsertExecutor:
    kwargs.setdefault("table", t_record)
    kwargs.setdefault("values", [{"id": 1, "name": "a", "note": "n"}])
    return MysqlUpsertExecutor(
        engine=sa.create_engine("mysql+pymysql://"),  # never connects
        **kwargs,
    )


@pytest.mark.parametrize(
    "strategy, columns, update_sql",
    [
        ("ignore", None, "ON DUPLICATE KEY UPDATE id = records.id"),
        ("merge", ["name"], "ON DUPLICATE KEY UPDATE name = stg.name"),
        (
            "replace",
            None,
            "ON DUPLICATE KEY UPDATE name = stg.name, note = stg.note",
        ),
    ],
)
def test_build_upsert(strategy, columns, update_sql):
    executor = new_executor(strategy=strategy, columns=columns)
    staging = make_staging_table(t_record, sa.MetaData(), "stg")
    stmt = executor.build_upsert(staging)
    sql = " ".join(str(stmt.compile(executor.engine)).split())
    assert sql.startswith(
        "INSERT INTO records (id, name, note) SELECT stg.id, stg.name, stg.note FROM stg"
    )
    assert sql.endswith(update_sql)
    assert "VALUES(" not in sql  # deprecated in MySQL 8.0.20


def test_max_identifier_length():
    """
    The staging name is cut to MySQL's 64, not the dialect's 255.
    """
    executor = new_executor(table=t_long, values=[], strategy="ignore")
    assert executor.engine.dialect.max_identifier_length > MAX_IDENTIFIER_LENGTH
    assert executor.max_identifier_length() == MAX_IDENTIFIER_LENGTH == 64
    assert len(executor.temp_table_name) == 64


def test_drop_staging_does_not_commit(engine):
    """
    The staging table is dropped with DROP TEMPORARY TABLE: a plain DROP TABLE
    would commit the caller's transaction, prior write included.
    """
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(t_record.insert(), [{"id": 100, "name": "x"}])
        executor = MysqlUpsertExecutor(
            engine=engine,
            table=t_record,
            values=INPUT,
            strategy="ignore",
            conn=conn,
            trans=trans,
        )
        assert executor.run() == (2, 1)
        assert staging_tables(conn) == []
        trans.rollback()
    assert read_rows(engine) == EXISTING_ROWS


@pytest.mark.parametrize("fail_at", ["create_staging", "upsert"])
def test_cleanup_after_rollback(engine, fail_at):
    """
    A failed call drops the staging table if it was created: the rollback to the
    SAVEPOINT does not.
    """
    with engine.connect() as conn:
        with conn.begin() as trans:
            executor = MysqlUpsertExecutor(
                engine=engine,
                table=t_record,
                values=INPUT,
                strategy="replace",
                conn=conn,
                trans=trans,
                fail_at=fail_at,
            )
            with pytest.raises(UpsertTestError):
                executor.run()
            assert staging_tables(conn) == []


@pytest.mark.parametrize(
    "values, problem",
    [
        ([{"code": "Z", "label": "la"}], "have the same value as an existing row"),
        (
            [{"code": "Y", "label": "lz"}, {"code": "Z", "label": "lz"}],
            "share a value with another record",
        ),
    ],
)
def test_check_unique_keys(engine, values, problem):
    executor = MysqlUpsertExecutor(
        engine=engine,
        table=t_rich,
        values=values,
        strategy="ignore",
    )
    with pytest.raises(sa.exc.IntegrityError) as exc_info:
        executor.run()
    assert isinstance(exc_info.value.orig, UniqueKeyConflictError)
    assert problem in str(exc_info.value.orig)
    assert "unique key (label) of 'RichItems'" in str(exc_info.value.orig)


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.mysql.executor",
        preview=False,
    )
