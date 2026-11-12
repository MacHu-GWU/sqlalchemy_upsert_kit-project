# -*- coding: utf-8 -*-

"""
MySQL bulk INSERT-OR-REPLACE: ``INSERT ... SELECT ... ON DUPLICATE
KEY UPDATE`` from a staging table.
"""

import typing as T

import sqlalchemy as sa

from .executor import MysqlUpsertExecutor


def insert_or_replace(
    engine: sa.Engine,
    table: sa.Table,
    values: list[dict[str, T.Any]],
    metadata: sa.MetaData | None = None,
    temp_table_name: str | None = None,
    conn: sa.Connection | None = None,
    trans: sa.Transaction | None = None,
    _fail_at: str | None = None,
) -> tuple[int, int]:
    """
    Bulk insert records into a MySQL table, overwriting every column of the
    rows whose primary key already exists.

    The records are loaded into a temporary staging table, then written with one
    ``INSERT ... SELECT ... ON DUPLICATE KEY UPDATE`` statement. A conflicting row is
    updated in place to equal the record: every column the records leave out is
    reset to its default (or NULL). It is never deleted, so ``ON DELETE`` foreign
    key actions and delete triggers do not fire.

    A record that would break a unique key other than the primary key (the
    same unique value as an existing row, or as another record) raises
    :class:`sqlalchemy.exc.IntegrityError` and changes nothing, as on the other
    databases; MySQL itself would update the other row instead.

    Runs in its own transaction (auto-managed: ``conn`` and ``trans`` are None), or
    inside the caller's (user-managed: both given; the caller commits or rolls
    back). Either way, a failure undoes everything the call did and leaves no
    staging table behind.

    :param engine: SQLAlchemy engine for a MySQL database
    :param table: Target table, with exactly one primary key column
    :param values: Records keyed by column key. All records must have the same
        keys, including a non-None, unique primary key.
    :param metadata: Metadata that holds the staging table definition while the
        call runs. A new one when None.
    :param temp_table_name: Name of the staging table. Generated when None.
    :param conn: Connection of a user-managed transaction, given with ``trans``
    :param trans: Transaction of a user-managed transaction, given with ``conn``
    :param _fail_at: **Testing only**: inject a failure before this step.

    :returns: Tuple of (replaced_rows, inserted_rows): existing rows overwritten, and new rows

    :raises ValueError: On invalid records, or a mismatched
        ``conn`` / ``trans`` pair, before any SQL runs.
    """
    return MysqlUpsertExecutor(
        engine=engine,
        table=table,
        values=values,
        strategy="replace",
        metadata=metadata,
        temp_table_name=temp_table_name,
        conn=conn,
        trans=trans,
        fail_at=_fail_at,
    ).run()
