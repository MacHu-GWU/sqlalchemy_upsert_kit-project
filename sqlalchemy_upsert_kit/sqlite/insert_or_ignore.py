# -*- coding: utf-8 -*-

"""
SQLite bulk INSERT-OR-IGNORE: ``INSERT ... SELECT ... ON CONFLICT DO NOTHING``
from a staging table.
"""

import typing as T

import sqlalchemy as sa

from .executor import SqliteUpsertExecutor


def insert_or_ignore(
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
    Bulk insert records into a SQLite table, skipping those whose primary key
    already exists.

    The records are loaded into a temporary staging table, then written with one
    ``INSERT ... SELECT ... ON CONFLICT DO NOTHING`` statement. Existing rows are
    never modified.

    Runs in its own transaction (auto-managed: ``conn`` and ``trans`` are None), or
    inside the caller's (user-managed: both given; the caller commits or rolls
    back). Either way, a failure undoes everything the call did and leaves no
    staging table behind.

    :param engine: SQLAlchemy engine for a SQLite database
    :param table: Target table, with exactly one primary key column
    :param values: Records keyed by column key. All records must have the same
        keys, including a non-None, unique primary key.
    :param metadata: Metadata that holds the staging table definition while the
        call runs. A new one when None.
    :param temp_table_name: Name of the staging table. Generated when None.
    :param conn: Connection of a user-managed transaction, given with ``trans``
    :param trans: Transaction of a user-managed transaction, given with ``conn``
    :param _fail_at: **Testing only**: inject a failure before this step.

    :returns: Tuple of (ignored_rows, inserted_rows): records whose primary key already existed, and new rows

    :raises ValueError: On invalid records, or a mismatched
        ``conn`` / ``trans`` pair, before any SQL runs.
    """
    return SqliteUpsertExecutor(
        engine=engine,
        table=table,
        values=values,
        strategy="ignore",
        metadata=metadata,
        temp_table_name=temp_table_name,
        conn=conn,
        trans=trans,
        fail_at=_fail_at,
    ).run()
