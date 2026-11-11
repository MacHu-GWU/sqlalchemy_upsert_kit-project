# -*- coding: utf-8 -*-

"""
SQLite bulk INSERT-OR-MERGE: ``INSERT ... SELECT ... ON CONFLICT DO UPDATE``
from a staging table.
"""

import typing as T

import sqlalchemy as sa

from .executor import SqliteUpsertExecutor


def insert_or_merge(
    engine: sa.Engine,
    table: sa.Table,
    values: list[dict[str, T.Any]],
    columns: list[str],
    metadata: sa.MetaData | None = None,
    temp_table_name: str | None = None,
    conn: sa.Connection | None = None,
    trans: sa.Transaction | None = None,
    _fail_at: str | None = None,
) -> tuple[int, int]:
    """
    Bulk insert records into a SQLite table; for rows whose primary key already
    exists, update only ``columns`` and keep the other columns.

    The records are loaded into a temporary staging table, then written with one
    ``INSERT ... SELECT ... ON CONFLICT DO UPDATE`` statement. A conflicting row is
    updated in place, never deleted, so ``ON DELETE`` foreign key actions and delete
    triggers do not fire.

    Runs in its own transaction (auto-managed: ``conn`` and ``trans`` are None), or
    inside the caller's (user-managed: both given; the caller commits or rolls
    back). Either way, a failure undoes everything the call did and leaves no
    staging table behind.

    :param engine: SQLAlchemy engine for a SQLite database
    :param table: Target table, with exactly one primary key column
    :param values: Records keyed by column key. All records must have the same
        keys, including a non-None, unique primary key.
    :param columns: Keys of the columns to update on conflicting rows.
        Must not be empty, and each must be a key of the records.
    :param metadata: Metadata that holds the staging table definition while the
        call runs. A new one when None.
    :param temp_table_name: Name of the staging table. Generated when None.
    :param conn: Connection of a user-managed transaction, given with ``trans``
    :param trans: Transaction of a user-managed transaction, given with ``conn``
    :param _fail_at: **Testing only**: inject a failure before this step.

    :returns: Tuple of (updated_rows, inserted_rows): existing rows updated, and new rows

    :raises ValueError: On invalid records or ``columns``, or a mismatched
        ``conn`` / ``trans`` pair, before any SQL runs.
    """
    return SqliteUpsertExecutor(
        engine=engine,
        table=table,
        values=values,
        strategy="merge",
        columns=columns,
        metadata=metadata,
        temp_table_name=temp_table_name,
        conn=conn,
        trans=trans,
        fail_at=_fail_at,
    ).run()
