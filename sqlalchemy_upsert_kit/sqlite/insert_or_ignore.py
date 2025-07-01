# -*- coding: utf-8 -*-

import typing as T
import dataclasses

import sqlalchemy as sa

from ..exc import UpsertTestError

from .executor import UpsertExecutor


@dataclasses.dataclass
class InsertOrIgnoreExecutor(UpsertExecutor):
    def apply_strategy(
        self,
        conn: sa.Connection,
        trans: sa.Transaction,
    ):
        if self._raise_on_target_insert:  # Testing flag
            raise UpsertTestError("error on target insertion")
        stmt = self.table.insert().from_select(
            list(self._temp_table.columns.keys()),
            sa.select(self._temp_table)
            .select_from(
                self._temp_table.outerjoin(  # LEFT JOIN to find non-matches
                    self.table,
                    self._temp_table.c[self.pk_name] == self.table.c[self.pk_name],
                )
            )
            .where(
                self.table.c[self.pk_name].is_(None)
            ),  # Only insert where no match exists
        )
        res = conn.execute(stmt)
        try:
            self._inserted_rows = res.rowcount if res.rowcount is not None else 0
            self._ignored_rows = len(self.values) - self._inserted_rows
        except:  # pragma: no cover
            self._inserted_rows = 0
            self._ignored_rows = len(self.values)


def insert_or_ignore(
    engine: sa.Engine,
    table: sa.Table,
    values: list[dict[str, T.Any]],
    metadata: T.Optional[sa.MetaData] = None,
    temp_table_name: T.Optional[str] = None,
    conn: T.Optional[sa.Connection] = None,
    trans: T.Optional[sa.Transaction] = None,
    _raise_on_temp_table_create: bool = False,
    _raise_on_temp_data_insert: bool = False,
    _raise_on_target_insert: bool = False,
    _raise_on_temp_table_drop: bool = False,
) -> tuple[int, int]:
    """
    Perform high-performance bulk INSERT-IF-NOT-EXISTS operation using temporary table.

    This function performs conditional bulk insertion: only inserts records whose
    primary keys don't already exist in the target table. This is equivalent to
    "INSERT IGNORE" or "INSERT ... ON CONFLICT DO NOTHING" but works more
    efficiently.

    **Algorithm**:

    1. Creates temporary table and loads all candidate data
    2. Uses LEFT JOIN to identify records not in target table
    3. Bulk inserts only the non-conflicting records
    4. Cleans up temporary resources

    This approach is ideal for:

    - Incremental data loading where duplicates should be ignored
    - ETL processes that need idempotent behavior
    - Syncing data from external sources

    **Transaction Management**:

    This function supports both auto-managed and user-managed transaction modes.
    See the module-level documentation for detailed explanations of each mode.

    :param engine: SQLAlchemy engine for database connection
    :param table: Target table for conditional insertion
    :param values: Records to insert if they don't exist.
        Must include primary key values for conflict detection.
    :param metadata: Optional metadata instance for temporary table isolation.
        If None, a new MetaData instance is created for clean separation.
    :param temp_table_name: Optional custom name for temporary table.
        If None, generates unique name with timestamp to avoid conflicts.
    :param conn: Optional database connection for user-managed transaction mode.
        Must be provided together with ``trans`` parameter.
    :param trans: Optional transaction for user-managed transaction mode.
        Must be provided together with ``conn`` parameter.

    :returns: Tuple of (ignored_rows, inserted_rows):
        - ignored_rows: Number of records that were not inserted (already existed)
        - inserted_rows: Number of new records successfully inserted

    :raises ValueError: When conn and trans parameters are provided inconsistently
        (one is None while the other is not)
    :raises UpsertTestError: When testing flags are enabled and corresponding operations fail

    **Examples**:

        Auto-managed transaction (default mode)::

            # Function manages its own transaction
            ignored, inserted = insert_or_ignore(engine, users_table, new_data)

        User-managed transaction mode::

            # Operation is part of larger transaction
            with engine.connect() as conn:
                with conn.begin() as trans:
                    # Other operations...
                    ignored, inserted = insert_or_ignore(
                        engine, users_table, new_data, conn=conn, trans=trans
                    )
                    # More operations...

        Conflict detection example::

            # Target table has records with id=1,2,3
            new_data = [
                {'id': 2, 'name': 'Bob'},      # Exists - will be ignored
                {'id': 4, 'name': 'Charlie'},  # New - will be inserted
                {'id': 5, 'name': 'David'},    # New - will be inserted
            ]
            ignored, inserted = insert_or_ignore(engine, users_table, new_data)
            # Result: ignored=1, inserted=2

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
    ignored_rows, inserted_rows = executor._ignored_rows, executor._inserted_rows
    return ignored_rows, inserted_rows
