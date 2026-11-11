# -*- coding: utf-8 -*-

"""
The SQLite-specific parts of the executor. The full workflow runs in the
strategy test files, through the shared contract.
"""

from sqlalchemy_upsert_kit.sqlite.executor import SqliteUpsertExecutor

import pytest
import sqlalchemy as sa

from sqlalchemy_upsert_kit.core.schema import make_staging_table
from sqlalchemy_upsert_kit.tests.contract import t_record


def new_executor(engine, strategy="ignore", **kwargs) -> SqliteUpsertExecutor:
    return SqliteUpsertExecutor(
        engine=engine,
        table=t_record,
        values=[{"id": 1, "name": "a", "note": "n"}],
        strategy=strategy,
        **kwargs,
    )


@pytest.mark.parametrize("prior_write", [False, True])
def test_begin_transaction(engine, prior_write):
    """
    A real transaction is open afterwards, whether or not the driver had begun one.
    """
    executor = new_executor(engine)
    with engine.connect() as conn:
        with conn.begin():
            if prior_write:
                conn.execute(t_record.delete())
            dbapi_conn = conn.connection.dbapi_connection
            assert dbapi_conn.in_transaction is prior_write
            executor.begin_transaction(conn)
            assert dbapi_conn.in_transaction is True


@pytest.mark.parametrize(
    "strategy, columns, conflict_sql",
    [
        ("ignore", None, "ON CONFLICT (id) DO NOTHING"),
        ("merge", ["name"], "ON CONFLICT (id) DO UPDATE SET name = excluded.name"),
    ],
)
def test_build_upsert(engine, strategy, columns, conflict_sql):
    executor = new_executor(engine, strategy=strategy, columns=columns)
    staging = make_staging_table(t_record, sa.MetaData(), "stg")
    sql = " ".join(str(executor.build_upsert(staging).compile(engine)).split())
    assert "SELECT stg.id, stg.name, stg.note FROM stg WHERE 1 = 1" in sql
    assert sql.endswith(conflict_sql)


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.sqlite.executor",
        preview=False,
    )
