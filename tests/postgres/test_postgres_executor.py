# -*- coding: utf-8 -*-

"""
The PostgreSQL-specific part of the executor: the upsert statement. The full
workflow runs in the strategy test files, through the shared contract.
"""

from sqlalchemy_upsert_kit.postgres.executor import PostgresUpsertExecutor

import pytest
import sqlalchemy as sa

from sqlalchemy_upsert_kit.core.schema import make_staging_table
from sqlalchemy_upsert_kit.tests.contract import t_record


@pytest.mark.parametrize(
    "strategy, columns, conflict_sql",
    [
        ("ignore", None, "ON CONFLICT (id) DO NOTHING"),
        ("merge", ["name"], "ON CONFLICT (id) DO UPDATE SET name = excluded.name"),
    ],
)
def test_build_upsert(strategy, columns, conflict_sql):
    executor = PostgresUpsertExecutor(
        engine=sa.create_engine("postgresql+psycopg://"),  # never connects
        table=t_record,
        values=[{"id": 1, "name": "a", "note": "n"}],
        strategy=strategy,
        columns=columns,
    )
    staging = make_staging_table(t_record, sa.MetaData(), "stg")
    stmt = executor.build_upsert(staging)
    sql = " ".join(str(stmt.compile(executor.engine)).split())
    assert "SELECT stg.id, stg.name, stg.note FROM stg ON CONFLICT" in sql
    assert sql.endswith(conflict_sql)


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.postgres.executor",
        preview=False,
    )
