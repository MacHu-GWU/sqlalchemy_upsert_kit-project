# -*- coding: utf-8 -*-

import typing as T
import dataclasses

import sqlalchemy as sa

from ..exc import UpsertTestError

from .executor import UpsertExecutor


@dataclasses.dataclass
class InsertOrMergeExecutor(UpsertExecutor):
    def apply_strategy(
        self,
        conn: sa.Connection,
        trans: sa.Transaction,
    ):
        """
        Apply merge strategy: update existing records with selected columns, insert new records.

        This implementation follows the merge algorithm:
        1. Create temp table t3 with merged data (existing records with selective updates)
        2. Delete conflicting records from target table t1
        3. Insert both updated records (from t3) and new records (from t2) into t1
        """
        if self._raise_on_target_delete:  # Testing flag
            raise UpsertTestError("error on target deletion")

        # Step 1: Create merge temp table (t3) with updated data for existing records
        merge_temp_table_name = f"{self.temp_table_name}_merge"
        merge_temp_table = sa.Table(
            merge_temp_table_name,
            self.metadata,
            *[
                sa.Column(col.name, col.type, nullable=col.nullable)
                for col in self.table.columns
            ],
            prefixes=["TEMPORARY"],
        )
        merge_temp_table.create(conn)

        try:
            if self._raise_on_merge_update:  # Testing flag
                raise UpsertTestError("error on merge update")

            # Build the merge query: existing records with selective column updates
            # For columns in self.columns: use values from temp table (t2)
            # For columns not in self.columns: use values from target table (t1)
            select_cols = []
            for col in self.table.columns:
                if col.name in self.columns:
                    # Use new value from temp table
                    select_cols.append(self._temp_table.c[col.name].label(col.name))
                else:
                    # Preserve existing value from target table
                    select_cols.append(self.table.c[col.name].label(col.name))

            # Create merged records for existing/conflicting records
            merge_stmt = merge_temp_table.insert().from_select(
                [col.name for col in self.table.columns],
                sa.select(*select_cols).select_from(
                    self.table.join(
                        self._temp_table,
                        self.table.c[self.pk_name] == self._temp_table.c[self.pk_name],
                    )
                ),
            )
            merge_result = conn.execute(merge_stmt)
            self._updated_rows = (
                merge_result.rowcount if merge_result.rowcount is not None else 0
            )

            # Step 2: Delete existing records that will be replaced with merged data
            inner = sa.select(self.table.c[self.pk_name]).join(
                self._temp_table,
                self.table.c[self.pk_name] == self._temp_table.c[self.pk_name],
            )
            delete_stmt = self.table.delete().where(
                self.table.c[self.pk_name].in_(inner)
            )
            conn.execute(delete_stmt)

            # Step 3: Insert merged records (updated existing records)
            if self._raise_on_target_insert:  # Testing flag
                raise UpsertTestError("error on target insertion")

            if self._updated_rows > 0:
                insert_merged_stmt = self.table.insert().from_select(
                    list(merge_temp_table.columns.keys()),
                    sa.select(*list(merge_temp_table.columns.values())),
                )
                conn.execute(insert_merged_stmt)

            # Step 4: Insert new records (records that don't exist in target table)
            # Use LEFT JOIN to find records in temp table that don't exist in target
            new_records_stmt = self.table.insert().from_select(
                list(self._temp_table.columns.keys()),
                sa.select(*list(self._temp_table.columns.values()))
                .select_from(
                    self._temp_table.outerjoin(
                        self.table,
                        self._temp_table.c[self.pk_name] == self.table.c[self.pk_name],
                    )
                )
                .where(self.table.c[self.pk_name].is_(None)),
            )
            insert_result = conn.execute(new_records_stmt)
            self._inserted_rows = (
                insert_result.rowcount if insert_result.rowcount is not None else 0
            )

        finally:
            # Clean up merge temp table
            merge_temp_table.drop(conn)
            self.metadata.remove(merge_temp_table)


def insert_or_merge(
    engine: sa.Engine,
    table: sa.Table,
    values: list[dict[str, T.Any]],
    columns: list[str],
    metadata: T.Optional[sa.MetaData] = None,
    temp_table_name: T.Optional[str] = None,
    conn: T.Optional[sa.Connection] = None,
    trans: T.Optional[sa.Transaction] = None,
    _raise_on_temp_table_create: bool = False,
    _raise_on_temp_data_insert: bool = False,
    _raise_on_target_delete: bool = False,
    _raise_on_target_insert: bool = False,
    _raise_on_temp_table_drop: bool = False,
    _raise_on_merge_update: bool = False,
) -> tuple[int, int]:
    """
    Perform high-performance bulk INSERT-OR-MERGE operation using temporary table.

    This function performs bulk merge operations: updates specific columns of existing
    records while preserving other columns, and inserts records that don't exist.
    This is ideal for incremental data updates where only certain fields need updating.

    **Algorithm**:

    1. Creates temporary table (t2) and loads all candidate data
    2. Creates merge temporary table (t3) with selective column updates
    3. Generates merged records by combining existing data with new values for specified columns
    4. Deletes conflicting records from target table (t1)
    5. Inserts merged records and new records into target table
    6. Cleans up temporary resources

    This approach is ideal for:

    - Selective column updates (e.g., updating timestamps while preserving descriptions)
    - Incremental data synchronization
    - Preserving historical data in non-updated columns

    **Transaction Management**:

    This function supports both auto-managed and user-managed transaction modes.
    See the module-level documentation for detailed explanations of each mode.

    :param engine: SQLAlchemy engine for database connection
    :param table: Target table for upsert operation
    :param values: Records to merge.
        Must include primary key values for conflict detection.
    :param columns: List of columns to update with new values. Other columns will
        remain unchanged for existing records. This is required, otherwise user should use
        insert_or_ignore or insert_or_replace strategy.
    :param metadata: Optional metadata instance for temporary table isolation.
        If None, a new MetaData instance is created for clean separation.
    :param temp_table_name: Optional custom name for temporary table.
        If None, generates unique name with timestamp to avoid conflicts.
    :param conn: Optional database connection for user-managed transaction mode.
        Must be provided together with ``trans`` parameter.
    :param trans: Optional transaction for user-managed transaction mode.
        Must be provided together with ``conn`` parameter.

    :returns: Tuple of (updated_rows, inserted_rows):
        - updated_rows: Number of existing records that were updated with new column values
        - inserted_rows: Number of new records that were inserted

    :raises ValueError: When conn and trans parameters are provided inconsistently
        or when columns parameter is empty
    :raises UpsertTestError: When testing flags are enabled and corresponding operations fail

    **Examples**:

        Auto-managed transaction (default mode)::

            # Function manages its own transaction
            updated, inserted = merge(engine, users_table, new_data, columns=["update_at"])

        User-managed transaction mode::

            # Operation is part of larger transaction
            with engine.connect() as conn:
                with conn.begin() as trans:
                    # Other operations...
                    updated, inserted = merge(
                        engine, users_table, new_data, columns=["update_at"],
                        conn=conn, trans=trans
                    )
                    # More operations...

        Selective column update example::

            # Target table has records with id=1,2,3
            new_data = [
                {'id': 2, 'name': 'Bob', 'update_at': '2024-01-02'},    # Exists - will be merged
                {'id': 4, 'name': 'Charlie', 'update_at': '2024-01-02'}, # New - will be inserted
            ]
            updated, inserted = merge(engine, users_table, new_data, columns=["update_at"])
            # Result: updated=1, inserted=1
            # Record id=2 will have new update_at but original name preserved

    .. note::

        Parameters prefixed with ``_raise_on_`` are exclusively for testing error
        handling and cleanup behavior. Never use these in production code.

    .. warning::

        The columns parameter must not be empty. If you want to update all columns,
        use insert_or_replace instead. If you want to ignore conflicts, use
        insert_or_ignore instead.
    """
    if not values:  # pragma: no cover
        return 0, 0  # No-op for empty data

    if not columns:  # pragma: no cover
        raise ValueError(
            "columns parameter cannot be empty. Use insert_or_replace for full updates or insert_or_ignore for conflict-free inserts."
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
        _raise_on_target_delete=_raise_on_target_delete,
        _raise_on_target_insert=_raise_on_target_insert,
        _raise_on_temp_table_drop=_raise_on_temp_table_drop,
        _raise_on_merge_update=_raise_on_merge_update,
    )
    executor.run()
    updated_rows, inserted_rows = executor._updated_rows, executor._inserted_rows
    return updated_rows, inserted_rows
