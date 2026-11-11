# -*- coding: utf-8 -*-

"""
PostgreSQL bulk INSERT-OR-MERGE: ``INSERT ... SELECT ... ON CONFLICT DO UPDATE``
of selected columns only, from a temporary staging table.
"""

import typing as T
import dataclasses

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..exc import UpsertTestError

from .executor import PostgresUpsertExecutor


@dataclasses.dataclass
class InsertOrMergeExecutor(PostgresUpsertExecutor):
    """
    Update only ``columns`` of conflicting rows, insert the rest.
    """

    def apply_strategy(
        self,
        conn: sa.Connection,
        trans: sa.Transaction,
    ):
        self._updated_rows = self.count_conflicts(conn)
        if self._raise_on_target_insert:  # Testing flag
            raise UpsertTestError("error on target insertion")
        stmt = pg_insert(self.table).from_select(
            list(self._temp_table.columns.keys()),
            sa.select(self._temp_table),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[self.pk_name],
            set_={name: stmt.excluded[name] for name in self.columns},
        )
        # SQLAlchemy only keeps rowcount of UPDATE/DELETE by default;
        # ask it to keep the rowcount of this INSERT too.
        res = conn.execute(stmt.execution_options(preserve_rowcount=True))
        # rowcount counts both the inserted and the updated rows
        self._inserted_rows = res.rowcount - self._updated_rows


def insert_or_merge(
    engine: sa.Engine,
    table: sa.Table,
    values: list[dict[str, T.Any]],
    columns: list[str],
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
    Bulk insert records into a PostgreSQL table; for rows whose primary key
    already exists, update only ``columns`` and keep the other columns.

    The records are loaded into a temporary staging table, then written with
    one ``INSERT ... SELECT ... ON CONFLICT DO UPDATE SET <columns>``. Unlike
    the SQLite version, conflicting rows are updated in place, not deleted and
    inserted again, so ``ON DELETE`` foreign key actions and delete triggers
    never fire.

    Supports the same two transaction modes as the SQLite version:
    auto-managed (``conn`` and ``trans`` are None) and user-managed
    (both are given, and the caller commits or rolls back).

    :param engine: SQLAlchemy engine for a PostgreSQL database
    :param table: Target table, with exactly one primary key column
    :param values: Records to merge, each including the primary key
    :param columns: Columns to update on conflicting rows. Must not be empty:
        use :func:`~sqlalchemy_upsert_kit.postgres.insert_or_replace.insert_or_replace`
        to update every column, or
        :func:`~sqlalchemy_upsert_kit.postgres.insert_or_ignore.insert_or_ignore`
        to update none.
    :param metadata: Optional metadata instance that holds the staging table.
        If None, a new MetaData instance is created.
    :param temp_table_name: Optional name of the staging table.
        If None, a name with a timestamp is generated.
    :param conn: Connection of a user-managed transaction, given together with ``trans``
    :param trans: Transaction of a user-managed transaction, given together with ``conn``

    :returns: Tuple of (updated_rows, inserted_rows)

    :raises ValueError: When ``columns`` is empty

    .. note::

        Parameters prefixed with ``_raise_on_`` are exclusively for testing error
        handling and cleanup behavior. Never use these in production code.
    """
    if not values:  # pragma: no cover
        return 0, 0  # No-op for empty data
    if not columns:  # pragma: no cover
        raise ValueError(
            "columns parameter cannot be empty. Use insert_or_replace for full "
            "updates or insert_or_ignore for conflict-free inserts."
        )
    executor = InsertOrMergeExecutor.new(
        engine=engine,
        table=table,
        values=values,
        metadata=metadata,
        temp_table_name=temp_table_name,
        conn=conn,
        trans=trans,
        columns=columns,
        _raise_on_temp_table_create=_raise_on_temp_table_create,
        _raise_on_temp_data_insert=_raise_on_temp_data_insert,
        _raise_on_target_insert=_raise_on_target_insert,
        _raise_on_temp_table_drop=_raise_on_temp_table_drop,
    )
    executor.run()
    return executor._updated_rows, executor._inserted_rows
