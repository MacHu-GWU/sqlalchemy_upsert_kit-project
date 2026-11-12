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
from sqlalchemy_upsert_kit.tests.contract import assert_no_staging
from sqlalchemy_upsert_kit.tests.contract import read_rows
from sqlalchemy_upsert_kit.tests.contract import staging_tables
from sqlalchemy_upsert_kit.tests.contract import t_long
from sqlalchemy_upsert_kit.tests.contract import t_pair
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


@pytest.mark.parametrize("strategy", ["ignore", "replace"])
@pytest.mark.parametrize(
    "values, problem",
    [
        (
            [{"code": "Z", "label": "la"}],
            "1 record(s) with a new primary key have the value of an existing row",
        ),
        (
            [{"code": "Y", "label": "lz"}, {"code": "Z", "label": "lz"}],
            "1 value(s) would be written by several records",
        ),
        (
            # The default collation is case-insensitive: 'lq' and 'LQ' are equal.
            [{"code": "Y", "label": "lq"}, {"code": "Z", "label": "LQ"}],
            "would be written by several records",
        ),
    ],
)
def test_check_unique_keys(engine, strategy, values, problem):
    rows_before = read_rows(engine, t_rich)
    executor = MysqlUpsertExecutor(
        engine=engine,
        table=t_rich,
        values=values,
        strategy=strategy,
    )
    with pytest.raises(sa.exc.IntegrityError) as exc_info:
        executor.run()
    assert isinstance(exc_info.value.orig, UniqueKeyConflictError)
    message = str(exc_info.value.orig)
    assert message.startswith(
        "On the unique key (label) of 'RichItems', which is not the primary key: "
    )
    assert problem in message
    assert read_rows(engine, t_rich) == rows_before


def test_check_unique_keys_compares_final_values(engine):
    """
    Merge writes only ``b`` of the unique key ``(a, b)``: row 1 keeps ``a = 1``
    and becomes ``(1, 2)``, the value of the new record 2. Its staged value,
    ``(9, 2)``, differs, so the check must compare the row's final value.
    """
    executor = MysqlUpsertExecutor(
        engine=engine,
        table=t_pair,
        values=[
            {"id": 1, "a": 9, "b": 2, "tag": "t1"},
            {"id": 2, "a": 1, "b": 2, "tag": "t2"},
        ],
        strategy="merge",
        columns=["b"],
    )
    with pytest.raises(sa.exc.IntegrityError) as exc_info:
        executor.run()
    assert str(exc_info.value.orig) == (
        "On the unique key (a, b) of 'pairs', which is not the primary key: "
        "1 value(s) would be written by several records"
    )


@pytest.fixture
def t_undeclared(engine):
    """
    A table with a unique index on ``label`` in the database that its ``Table``
    leaves out, so :meth:`check_unique_keys` cannot see it.
    """
    table = sa.Table(
        "undeclared_items",
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("label", sa.String(10)),
    )
    table.drop(engine, checkfirst=True)
    table.create(engine)
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE UNIQUE INDEX ux_label ON undeclared_items (label)")
        conn.execute(table.insert(), [{"id": 1, "label": "la"}])
    yield table
    table.drop(engine)


@pytest.mark.parametrize("strategy", ["ignore", "replace"])
def test_check_after_upsert(engine, t_undeclared, strategy):
    """
    A record lost to another row on a unique key the ``Table`` does not declare
    is caught after the upsert, which is then undone.
    """
    executor = MysqlUpsertExecutor(
        engine=engine,
        table=t_undeclared,
        values=[{"id": 2, "label": "lb"}, {"id": 3, "label": "la"}],
        strategy=strategy,
    )
    with pytest.raises(sa.exc.IntegrityError) as exc_info:
        executor.run()
    assert isinstance(exc_info.value.orig, UniqueKeyConflictError)
    assert str(exc_info.value.orig).startswith(
        "1 record(s) were not written to 'undeclared_items'"
    )
    assert read_rows(engine, t_undeclared) == [(1, "la")]
    assert_no_staging(engine)


@pytest.fixture
def other_engine(db_url):
    """
    A second connection, for another transaction that commits in between.
    """
    engine = sa.create_engine(db_url)
    yield engine
    engine.dispose()


def test_old_snapshot_unique_key(engine, other_engine):
    """
    The caller's transaction read the table before another transaction
    committed a row with label 'lx'. The checks before the upsert read the old
    snapshot and miss that row; the check after the upsert catches the record
    that the upsert lost to it.
    """
    with engine.connect() as conn:
        with conn.begin() as trans:
            assert len(read_rows(conn, t_rich)) == 2  # takes the snapshot
            with other_engine.begin() as other:
                other.execute(t_rich.insert(), [{"code": "X", "label": "lx"}])
            executor = MysqlUpsertExecutor(
                engine=engine,
                table=t_rich,
                values=[{"code": "Z", "label": "lx"}],
                strategy="ignore",
                conn=conn,
                trans=trans,
            )
            with pytest.raises(sa.exc.IntegrityError) as exc_info:
                executor.run()
            assert "were not written" in str(exc_info.value.orig)
    assert [row[0] for row in read_rows(engine, t_rich)] == ["A", "B", "X"]


def test_old_snapshot_counts(engine, other_engine):
    """
    The conflict count reads the snapshot too: id 4, committed by another
    transaction after it, is counted as inserted, though it was a conflict.
    The sum and the resulting rows are still right.
    """
    with engine.connect() as conn:
        with conn.begin() as trans:
            assert read_rows(conn) == EXISTING_ROWS  # takes the snapshot
            with other_engine.begin() as other:
                other.execute(t_record.insert(), [{"id": 4, "name": "x"}])
            executor = MysqlUpsertExecutor(
                engine=engine,
                table=t_record,
                values=INPUT,
                strategy="ignore",
                conn=conn,
                trans=trans,
            )
            assert executor.run() == (2, 1)
    assert read_rows(engine) == EXISTING_ROWS + [(4, "x", None)]


@pytest.fixture
def t_binary(engine):
    """
    A table whose table-level collation is case-sensitive (``utf8mb4_bin``).
    """
    table = sa.Table(
        "binary_items",
        sa.MetaData(),
        sa.Column("code", sa.String(10), primary_key=True),
        sa.Column("label", sa.String(10), unique=True),
        mysql_collate="utf8mb4_bin",
        mysql_default_charset="utf8mb4",
    )
    table.drop(engine, checkfirst=True)
    table.create(engine)
    yield table
    table.drop(engine)


def test_table_collation(engine, t_binary):
    """
    The staging table compares like the target: under ``utf8mb4_bin``, 'e' and
    'E', and 'Y' and 'y', are different values.
    """
    values = [{"code": "e", "label": "Y"}, {"code": "E", "label": "y"}]
    executor = MysqlUpsertExecutor(
        engine=engine,
        table=t_binary,
        values=values,
        strategy="replace",
    )
    assert executor.run() == (0, 2)
    assert read_rows(engine, t_binary) == [("E", "y"), ("e", "Y")]


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.mysql.executor",
        preview=False,
    )
