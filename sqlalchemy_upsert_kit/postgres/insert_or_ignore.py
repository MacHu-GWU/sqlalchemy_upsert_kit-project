# -*- coding: utf-8 -*-

"""
PostgreSQL bulk INSERT-OR-IGNORE: ``INSERT ... SELECT ... ON CONFLICT DO NOTHING``
from a temporary staging table.
"""

import typing as T
import dataclasses

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..exc import UpsertTestError

from .executor import PostgresUpsertExecutor


@dataclasses.dataclass
class InsertOrIgnoreExecutor(PostgresUpsertExecutor):
    """
    Insert the staged rows whose primary key is not in the target table yet.
    """

    def apply_strategy(
        self,
        conn: sa.Connection,
        trans: sa.Transaction,
    ):
        if self._raise_on_target_insert:  # Testing flag
            raise UpsertTestError("error on target insertion")
        stmt = (
            pg_insert(self.table)
            .from_select(
                list(self._temp_table.columns.keys()),
                sa.select(self._temp_table),
            )
            .on_conflict_do_nothing(index_elements=[self.pk_name])
        )
        # SQLAlchemy only keeps rowcount of UPDATE/DELETE by default;
        # ask it to keep the rowcount of this INSERT too.
        res = conn.execute(stmt.execution_options(preserve_rowcount=True))
        self._inserted_rows = res.rowcount
        self._ignored_rows = len(self.values) - self._inserted_rows


def insert_or_ignore(
    engine: sa.Engine,
    table: sa.Table,
    values: list[dict[str, T.Any]],
    metadata: sa.MetaData | None = None,
    temp_table_name: str | None = None,
    conn: sa.Connection | None = None,
    trans: sa.Transaction | None = None,
    _raise_on_temp_table_create: bool = False,
    _raise_on_temp_data_insert: bool = False,
    _raise_on_target_insert: bool = False,
    _raise_on_temp_table_drop: bool = False,
) -> tuple[int, int]:
    """
    Bulk insert records into a PostgreSQL table, skipping those whose primary
    key already exists.

    The records are loaded into a temporary staging table, then copied into
    the target table with one ``INSERT ... SELECT ... ON CONFLICT DO NOTHING``.
    Existing rows are never modified.

    Supports the same two transaction modes as the SQLite version:
    auto-managed (``conn`` and ``trans`` are None) and user-managed
    (both are given, and the caller commits or rolls back).

    :param engine: SQLAlchemy engine for a PostgreSQL database
    :param table: Target table, with exactly one primary key column
    :param values: Records to insert, each including the primary key
    :param metadata: Optional metadata instance that holds the staging table.
        If None, a new MetaData instance is created.
    :param temp_table_name: Optional name of the staging table.
        If None, a name with a timestamp is generated.
    :param conn: Connection of a user-managed transaction, given together with ``trans``
    :param trans: Transaction of a user-managed transaction, given together with ``conn``

    :returns: Tuple of (ignored_rows, inserted_rows)

    .. note::

        Parameters prefixed with ``_raise_on_`` are exclusively for testing error
        handling and cleanup behavior. Never use these in production code.
    """
    if not values:  # pragma: no cover
        return 0, 0  # No-op for empty data
    executor = InsertOrIgnoreExecutor.new(
        engine=engine,
        table=table,
        values=values,
        metadata=metadata,
        temp_table_name=temp_table_name,
        conn=conn,
        trans=trans,
        _raise_on_temp_table_create=_raise_on_temp_table_create,
        _raise_on_temp_data_insert=_raise_on_temp_data_insert,
        _raise_on_target_insert=_raise_on_target_insert,
        _raise_on_temp_table_drop=_raise_on_temp_table_drop,
    )
    executor.run()
    return executor._ignored_rows, executor._inserted_rows
