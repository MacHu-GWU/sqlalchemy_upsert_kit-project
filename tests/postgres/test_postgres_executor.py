# -*- coding: utf-8 -*-

"""
The full run of :class:`~sqlalchemy_upsert_kit.postgres.executor.PostgresUpsertExecutor`
is exercised through the strategies in the ``test_postgres_insert_or_*.py`` files;
this file tests the staging table definition and the PostgreSQL-specific hooks.
"""

from sqlalchemy_upsert_kit.postgres.executor import clone_staging_table
from sqlalchemy_upsert_kit.postgres.executor import PostgresUpsertExecutor

import sqlalchemy as sa

from sqlalchemy_upsert_kit.postgres.insert_or_ignore import InsertOrIgnoreExecutor
from sqlalchemy_upsert_kit.tests.data import t_record


def make_tables() -> tuple[sa.MetaData, sa.Table, sa.Table]:
    """
    A parent table, and a child table with a foreign key, a named index,
    a unique constraint, a server default, a Python default and a sequence.
    """
    metadata = sa.MetaData()
    parent = sa.Table(
        "pg_exec_parent",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
    )
    child = sa.Table(
        "pg_exec_child",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("parent_id", sa.Integer, sa.ForeignKey("pg_exec_parent.id")),
        sa.Column("code", sa.String, unique=True),
        sa.Column("status", sa.String, server_default="new"),
        sa.Column("score", sa.Integer, default=7),
        sa.Column("seq", sa.Integer, sa.Sequence("pg_exec_child_seq")),
        sa.Index("ix_pg_exec_child_code", "code"),
    )
    return metadata, parent, child


def test_clone_staging_table():
    _, _, child = make_tables()
    temp = clone_staging_table(
        original_table=child,
        metadata=sa.MetaData(),
        temp_table_name="temp_child",
    )
    assert list(temp.columns.keys()) == list(child.columns.keys())
    assert [c.name for c in temp.primary_key] == ["id"]
    assert not temp.foreign_keys
    assert not temp.indexes
    assert not [c for c in temp.constraints if isinstance(c, sa.UniqueConstraint)]
    assert temp.c.status.server_default.arg == "new"
    assert temp.c.score.default.arg == 7
    assert temp.c.seq.default is None  # sequences are not copied
    assert temp.dialect_options["postgresql"]["on_commit"] == "DROP"


def test_clone_staging_table_creates_in_postgres(pg_engine):
    """
    Cloning the indexes or foreign keys would fail in PostgreSQL: index names
    are unique per schema, and a temporary table may not reference a permanent
    one. The staging table must create cleanly and disappear at commit.
    """
    metadata, _, child = make_tables()
    metadata.drop_all(pg_engine)
    metadata.create_all(pg_engine)
    try:
        temp = clone_staging_table(
            original_table=child,
            metadata=sa.MetaData(),
            temp_table_name="temp_child",
        )
        with pg_engine.connect() as conn:
            with conn.begin():
                temp.create(conn)
                conn.execute(temp.insert(), [{"id": 1, "code": "a"}])
                row = conn.execute(sa.select(temp)).mappings().one()
                assert row["status"] == "new"
                assert row["score"] == 7
            # ON COMMIT DROP: gone after the commit, in the same session
            assert not sa.inspect(conn).has_table("temp_child")
    finally:
        metadata.drop_all(pg_engine)


def test_cleanup_temp_table_on_failure():
    engine = sa.create_engine("postgresql+psycopg://")  # never connects
    executor = InsertOrIgnoreExecutor.new(
        engine=engine,
        table=t_record,
        values=[],
    )
    assert isinstance(executor, PostgresUpsertExecutor)
    executor.clone_temp_table()
    executor._temp_table_created = True
    executor.cleanup_temp_table_on_failure()
    assert executor.temp_table_name not in executor.metadata.tables
    assert executor._temp_table_created is False
    # idempotent: a second call finds nothing to remove
    executor.cleanup_temp_table_on_failure()


def test_count_conflicts(clean_database, data_faker):
    engine = clean_database
    executor = InsertOrIgnoreExecutor.new(
        engine=engine,
        table=t_record,
        values=data_faker.input_data,
    )
    executor.clone_temp_table()
    with engine.connect() as conn:
        with conn.begin():
            executor.create_temp_table(conn)
            executor.insert_temp_data(conn)
            assert executor.count_conflicts(conn) == data_faker.n_conflict


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.postgres.executor",
        preview=False,
    )
