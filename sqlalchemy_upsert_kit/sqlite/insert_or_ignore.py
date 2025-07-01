# -*- coding: utf-8 -*-

import typing as T

import sqlalchemy as sa

from ..exc import UpsertTestError
from ..utils import get_pk_name, clone_temp_table


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
    :param _raise_on_temp_table_create: **Testing only** - Simulate temp table creation failure
    :param _raise_on_temp_data_insert: **Testing only** - Simulate temp data insertion failure
    :param _raise_on_target_insert: **Testing only** - Simulate target insertion failure
    :param _raise_on_temp_table_drop: **Testing only** - Simulate temp table cleanup failure

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

    # Validate transaction mode parameters
    user_managed = conn is not None and trans is not None
    auto_managed = conn is None and trans is None

    if not (user_managed or auto_managed):  # pragma: no cover
        raise ValueError(
            "Either both conn and trans must be provided (user-managed mode), "
            "or both must be None (auto-managed mode)"
        )

    # Use separate metadata for clean isolation
    if metadata is None:
        metadata = sa.MetaData()
    temp_table = clone_temp_table(
        original_table=table,
        metadata=metadata,
        temp_table_name=temp_table_name,
    )
    pk_name = get_pk_name(table)

    ignored_rows = 0
    inserted_rows = 0
    temp_table_created = False

    def _execute_upsert_logic(
        connection: sa.Connection,
        transaction: sa.Transaction,
    ):
        """
        Execute the core insert-or-ignore logic within the provided transaction context.

        This function implements the temporary table strategy for bulk conditional
        insertion. It operates within either a user-managed or auto-managed transaction
        depending on how the parent function was called.
        """
        nonlocal temp_table_created, ignored_rows, inserted_rows

        try:
            # Step 1: Create temporary staging table for bulk data processing
            if _raise_on_temp_table_create:  # Testing flag
                raise UpsertTestError("error on temp table creation")
            temp_table.create(connection)
            temp_table_created = True

            # Step 2: Bulk load all candidate records into staging area
            # This is much faster than individual row processing
            if _raise_on_temp_data_insert:  # Testing flag
                raise UpsertTestError("error on temp data insertion")
            connection.execute(temp_table.insert(), values)

            # Step 3: Insert only records that don't exist in target table
            # Uses LEFT JOIN to efficiently identify non-conflicting records
            if _raise_on_target_insert:  # Testing flag
                raise UpsertTestError("error on target insertion")
            stmt = table.insert().from_select(
                list(temp_table.columns.keys()),
                sa.select(temp_table)
                .select_from(
                    temp_table.outerjoin(  # LEFT JOIN to find non-matches
                        table,
                        temp_table.c[pk_name] == table.c[pk_name],
                    )
                )
                .where(table.c[pk_name].is_(None)),  # Only insert where no match exists
            )
            res = connection.execute(stmt)
            try:
                inserted_rows = res.rowcount if res.rowcount is not None else 0
                ignored_rows = len(values) - inserted_rows
            except:  # pragma: no cover
                inserted_rows = 0
                ignored_rows = len(values)

            # Step 4: Clean up temporary table in normal success path
            if temp_table_created:
                if _raise_on_temp_table_drop:  # Testing flag
                    raise UpsertTestError("error on temp table cleanup")
                # Normal cleanup - drop temp table within the same connection
                temp_table.drop(connection)
                metadata.remove(temp_table)
                temp_table_created = False

            return ignored_rows, inserted_rows

        except Exception as e:
            # Handle testing flag for temp table cleanup errors
            if temp_table_created and _raise_on_temp_table_drop:
                raise UpsertTestError("error on temp table cleanup")
            # Re-raise original exception - cleanup handled by caller based on transaction mode
            raise e

    def _cleanup_temp_table():
        """
        Clean up temporary table using a fresh connection.

        This function is called when cleanup needs to happen outside the main
        transaction context, typically in error scenarios. It uses a fresh
        connection to avoid SQLite database lock issues that can occur when
        the original transaction has been rolled back.
        """
        if temp_table_created:
            try:
                # Use fresh connection to avoid database locks from rolled-back transactions
                with engine.connect() as cleanup_conn:
                    temp_table.drop(cleanup_conn)
                    cleanup_conn.commit()
                metadata.remove(temp_table)
            except Exception:
                # Cleanup failures should not mask the original exception
                # This can happen if temp table was already dropped or doesn't exist
                try:
                    # Try to remove from metadata anyway to prevent resource leaks
                    metadata.remove(temp_table)
                except Exception:  # pragma: no cover
                    pass

    if user_managed:
        # User-managed transaction mode: operate within caller's transaction context
        try:
            return _execute_upsert_logic(conn, trans)
        except Exception:
            # Clean up temp table but don't manage transaction - caller is responsible
            _cleanup_temp_table()
            raise
    else:
        # Auto-managed transaction mode: create and manage our own transaction
        try:
            with engine.connect() as connection:
                with connection.begin() as transaction:
                    result = _execute_upsert_logic(connection, transaction)
                    # Transaction automatically committed on successful exit
                    return result
        except Exception as e:
            # Transaction automatically rolled back by context manager
            # Clean up temp tables after all connections are properly closed
            _cleanup_temp_table()
            raise e
