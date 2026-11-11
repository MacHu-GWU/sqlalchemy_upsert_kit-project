# -*- coding: utf-8 -*-

"""
PostgreSQL bulk INSERT-OR-REPLACE: ``INSERT ... SELECT ... ON CONFLICT DO UPDATE``
of every non-key column, from a temporary staging table.
"""

import typing as T
import dataclasses

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..exc import UpsertTestError

from .executor import PostgresUpsertExecutor


@dataclasses.dataclass
class InsertOrReplaceExecutor(PostgresUpsertExecutor):
    """
    Overwrite every non-key column of conflicting rows, insert the rest.
    """

    def apply_strategy(
        self,
        conn: sa.Connection,
        trans: sa.Transaction,
    ):
        self._replaced_rows = self.count_conflicts(conn)
        if self._raise_on_target_insert:  # Testing flag
            raise UpsertTestError("error on target insertion")
        stmt = pg_insert(self.table).from_select(
            list(self._temp_table.columns.keys()),
            sa.select(self._temp_table),
        )
        update_columns = [
            col.name for col in self.table.columns if col.name != self.pk_name
        ]
        if update_columns:
            stmt = stmt.on_conflict_do_update(
                index_elements=[self.pk_name],
                set_={name: stmt.excluded[name] for name in update_columns},
            )
        else:  # a table with only the key column: nothing to replace
            stmt = stmt.on_conflict_do_nothing(index_elements=[self.pk_name])
        # SQLAlchemy only keeps rowcount of UPDATE/DELETE by default;
        # ask it to keep the rowcount of this INSERT too.
        res = conn.execute(stmt.execution_options(preserve_rowcount=True))
        if update_columns:
            # DO UPDATE: rowcount counts both the inserted and the updated rows
            self._inserted_rows = res.rowcount - self._replaced_rows
        else:
            # DO NOTHING: rowcount counts only the inserted rows
            self._inserted_rows = res.rowcount


def insert_or_replace(
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
    Bulk insert records into a PostgreSQL table, overwriting every column of
    the rows whose primary key already exists.

    The records are loaded into a temporary staging table, then written with
    one ``INSERT ... SELECT ... ON CONFLICT DO UPDATE``. Unlike the SQLite
    version, conflicting rows are updated in place, not deleted and inserted
    again, so ``ON DELETE`` foreign key actions and delete triggers never fire.

    Supports the same two transaction modes as the SQLite version:
    auto-managed (``conn`` and ``trans`` are None) and user-managed
    (both are given, and the caller commits or rolls back).

    :param engine: SQLAlchemy engine for a PostgreSQL database
    :param table: Target table, with exactly one primary key column
    :param values: Records to insert or replace, each including the primary key
    :param metadata: Optional metadata instance that holds the staging table.
        If None, a new MetaData instance is created.
    :param temp_table_name: Optional name of the staging table.
        If None, a name with a timestamp is generated.
    :param conn: Connection of a user-managed transaction, given together with ``trans``
    :param trans: Transaction of a user-managed transaction, given together with ``conn``

    :returns: Tuple of (replaced_rows, inserted_rows)

    .. note::

        Parameters prefixed with ``_raise_on_`` are exclusively for testing error
        handling and cleanup behavior. Never use these in production code.
    """
    if not values:  # pragma: no cover
        return 0, 0  # No-op for empty data
    executor = InsertOrReplaceExecutor.new(
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
    return executor._replaced_rows, executor._inserted_rows
