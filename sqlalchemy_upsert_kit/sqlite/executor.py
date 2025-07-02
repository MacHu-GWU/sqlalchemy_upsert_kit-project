# -*- coding: utf-8 -*-

"""
Abstract Base Class for High-Performance SQLAlchemy Bulk Upsert Operations

This module provides a Template Method pattern implementation for bulk upsert operations
using temporary table staging. The abstraction eliminates code duplication while allowing
different upsert strategies to implement their specific logic.

**Abstraction Strategy**:

The ``UpsertExecutor`` class abstracts the common workflow shared by all bulk upsert
operations while requiring subclasses to implement only their strategy-specific logic.
This design provides:

1. **Code Reuse**: ~80% of upsert logic is common (transaction management, temp table
   lifecycle, error handling, cleanup)
2. **Strategy Isolation**: Each upsert strategy (INSERT OR IGNORE, INSERT OR REPLACE,
   UPSERT/MERGE) implements only its core database operations
3. **Consistent Behavior**: All strategies share the same transaction modes, error
   handling, and cleanup patterns
4. **Easy Extension**: Adding new upsert strategies requires minimal code

**Template Method Pattern**:

The execution flow follows this pattern::

    UpsertExecutor.run()
    ├── Transaction Management (auto vs user-managed)
    ├── UpsertExecutor.execute_operation()
    │   ├── Create temporary staging table
    │   ├── Bulk load candidate data into temp table
    │   ├── Subclass.apply_strategy() ← **Strategy-specific logic**
    │   └── Cleanup temporary table
    └── Error handling and rollback cleanup

**Supported Strategies**:

- **INSERT OR IGNORE**: Inserts only new records, ignores conflicts
- **INSERT OR REPLACE**: Replaces entire conflicting records with new data
- **UPSERT/MERGE**: Updates specific fields of existing records, inserts new ones

**Transaction Management**:

All strategies support dual-mode operation:

- **Auto-managed**: Function creates and manages its own transaction
- **User-managed**: Function operates within caller's existing transaction

**Database Compatibility**:

This module is designed for SQLite but the abstraction pattern can be extended
to other database systems by implementing database-specific executors.

**Implementation Notes**:

- Uses dataclasses for clean state management (eliminates nonlocal variables)
- Cached properties for efficient mode detection
- Comprehensive error handling with proper cleanup in all scenarios
- Testing infrastructure with controlled failure injection
"""

import typing as T
import abc

import sqlalchemy as sa

from ..exc import UpsertTestError
from ..utils import get_pk_name, get_temp_table_name, clone_temp_table

import dataclasses
from functools import cached_property


@dataclasses.dataclass
class UpsertExecutor(abc.ABC):
    """
    Abstract base class for high-performance bulk upsert operations using temporary tables.

    This class implements the Template Method pattern, providing a common framework
    for all upsert strategies while requiring subclasses to implement only their
    strategy-specific database operations.

    **Template Method Flow**:

    1. **Setup**: Create temporary table and validate parameters
    2. **Data Loading**: Bulk insert candidate records into staging table
    3. **Strategy Execution**: Subclass implements specific upsert logic
    4. **Cleanup**: Remove temporary resources and handle errors

    **Subclass Requirements**:

    Subclasses must implement :meth:`apply_strategy` to define their specific
    upsert behavior (INSERT OR IGNORE, INSERT OR REPLACE, UPSERT/MERGE, etc.).

    **State Management**:

    All operation state is managed through dataclass fields, eliminating the need
    for nonlocal variables and providing clean, testable state tracking.

    **Transaction Modes**:

    - **Auto-managed**: ``conn=None, trans=None`` - Creates own transaction
    - **User-managed**: ``conn=Connection, trans=Transaction`` - Uses caller's transaction

    **Example Usage**::

        @dataclasses.dataclass
        class MyUpsertExecutor(UpsertExecutor):
            def apply_strategy(self, conn, trans):
                # Implement specific upsert logic here
                pass

        executor = MyUpsertExecutor.new(engine, table, data)
        ignored, inserted = executor.run()

    :param engine: SQLAlchemy engine for database connection
    :param table: Target table for upsert operation
    :param values: Records to insert or replace.
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
    :param _raise_on_target_delete: **Testing only** - Simulate target deletion failure
    :param _raise_on_target_insert: **Testing only** - Simulate target insertion failure
    :param _raise_on_temp_table_drop: **Testing only** - Simulate temp table cleanup failure
    :param _ignored_rows: Number of records ignored during operation (INSERT OR IGNORE).
    :param _replaced_rows: Number of records replaced during operation (INSERT OR REPLACE).
    :param _updated_rows: Number of records updated during operation (UPSERT/MERGE).
    :param _inserted_rows: Number of new records inserted during operation
    :param _temp_table: Temporary staging table created during operation.
    :param _temp_table_created: Flag tracking whether temporary table was successfully created.
    """

    # --- Input Parameters ---
    engine: sa.Engine = dataclasses.field()
    table: sa.Table = dataclasses.field()
    values: list[dict[str, T.Any]] = dataclasses.field()
    metadata: T.Optional[sa.MetaData] = dataclasses.field()
    temp_table_name: T.Optional[str] = dataclasses.field()
    conn: T.Optional[sa.Connection] = dataclasses.field()
    trans: T.Optional[sa.Transaction] = dataclasses.field()
    columns: T.Optional[list[str]] = dataclasses.field()
    # --- Testing Parameters (Testing Only) ---
    _raise_on_temp_table_create: bool = dataclasses.field()
    _raise_on_temp_data_insert: bool = dataclasses.field()
    _raise_on_target_delete: bool = dataclasses.field()
    _raise_on_target_insert: bool = dataclasses.field()
    _raise_on_temp_table_drop: bool = dataclasses.field()
    _raise_on_merge_update: bool = dataclasses.field()
    # --- Operation Results ---
    _ignored_rows: int = dataclasses.field(default=0)
    _replaced_rows: int = dataclasses.field(default=0)
    _updated_rows: int = dataclasses.field(default=0)
    _inserted_rows: int = dataclasses.field(default=0)
    # --- Internal State ---
    _temp_table: sa.Table = dataclasses.field(init=False)
    _temp_table_created: bool = dataclasses.field(default=False)

    @classmethod
    def new(
        cls,
        engine: sa.Engine,
        table: sa.Table,
        values: list[dict[str, T.Any]],
        metadata: T.Optional[sa.MetaData] = None,
        temp_table_name: T.Optional[str] = None,
        conn: T.Optional[sa.Connection] = None,
        trans: T.Optional[sa.Transaction] = None,
        columns: T.Optional[list[str]] = None,
        _raise_on_temp_table_create: bool = False,
        _raise_on_temp_data_insert: bool = False,
        _raise_on_target_delete: bool = False,
        _raise_on_target_insert: bool = False,
        _raise_on_temp_table_drop: bool = False,
        _raise_on_merge_update: bool = False,
    ):
        """
        Factory method to create UpsertExecutor instances with sensible defaults.

        This method provides default values for optional parameters and creates
        a properly configured executor instance ready for operation.
        """
        # Validate required parameters
        if metadata is None:
            metadata = sa.MetaData()
        if temp_table_name is None:
            temp_table_name = get_temp_table_name(original_table_name=table.name)
        return cls(
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

    @cached_property
    def user_managed(self) -> bool:
        """
        Check if executor is operating in user-managed transaction mode.
        """
        return (self.conn is not None) and (self.trans is not None)

    @cached_property
    def auto_managed(self) -> bool:
        """
        Check if executor is operating in auto-managed transaction mode.
        """
        return (self.conn is None) and (self.trans is None)

    def __post_init__(self):
        """
        Validate transaction mode parameters after dataclass initialization.
        """
        if not (self.user_managed or self.auto_managed):  # pragma: no cover
            raise ValueError(
                "Either both conn and trans must be provided (user-managed mode), "
                "or both must be None (auto-managed mode)"
            )

    @cached_property
    def pk_name(self) -> str:
        """
        Get the primary key column name from the target table.
        """
        return get_pk_name(self.table)

    def clone_temp_table(self):
        """
        Create a temporary table with the same structure as the target table.

        This method clones the target table's structure into a temporary table
        that will be used for staging data during the upsert operation.
        """
        self._temp_table = clone_temp_table(
            original_table=self.table,
            metadata=self.metadata,
            temp_table_name=self.temp_table_name,
        )

    def create_temp_table(self, conn: sa.Connection):
        """
        Create the temporary staging table for bulk data operations.

        :param conn: Database connection to use for table creation
        """
        if self._raise_on_temp_table_create:  # Testing flag
            raise UpsertTestError("error on temp table creation")
        self._temp_table.create(conn)
        self._temp_table_created = True

    def insert_temp_data(self, conn: sa.Connection):
        """
        Bulk insert candidate records into temporary staging table.

        This operation loads all candidate records into the staging area,
        enabling efficient bulk processing through JOIN operations.

        :param conn: Database connection to use for data insertion
        """
        if self._raise_on_temp_data_insert:  # Testing flag
            raise UpsertTestError("error on temp data insertion")
        conn.execute(self._temp_table.insert(), self.values)

    def cleanup_temp_table_on_success(self, conn: sa.Connection):
        """
        Clean up temporary table after successful operation.

        This method removes the temporary table and cleans up metadata
        within the same connection context, ensuring proper cleanup
        in the success path.

        :param conn: Database connection to use for cleanup
        """
        if self._temp_table_created:
            if self._raise_on_temp_table_drop:  # Testing flag
                raise UpsertTestError("error on temp table cleanup")
            # Normal cleanup - drop temp table within the same connection
            self._temp_table.drop(conn)
            self.metadata.remove(self._temp_table)
            self._temp_table_created = False

    def cleanup_temp_table_on_failure(self):
        """
        Clean up temporary table using a fresh connection after transaction failure.

        This method is called when cleanup needs to happen outside the main
        transaction context, typically in error scenarios. It uses a fresh
        connection to avoid SQLite database lock issues that can occur when
        the original transaction has been rolled back.

        **Why Fresh Connection**:

        SQLite DDL operations (CREATE/DROP TABLE) are not transactional and commit
        immediately. When the main transaction rolls back, the temporary table may
        still exist but the original connection may be locked. A fresh connection
        ensures we can clean up properly.

        **Error Handling**:

        Cleanup failures are suppressed to avoid masking the original exception
        that caused the operation to fail. This prevents cleanup issues from
        hiding the root cause of problems.
        """
        if self._temp_table_created:
            try:
                # Use fresh connection to avoid database locks from rolled-back transactions
                with self.engine.connect() as cleanup_conn:
                    self._temp_table.drop(cleanup_conn)
                    cleanup_conn.commit()
                self.metadata.remove(self._temp_table)
            except Exception:
                # Cleanup failures should not mask the original exception
                # This can happen if temp table was already dropped or doesn't exist
                try:
                    # Try to remove from metadata anyway to prevent resource leaks
                    self.metadata.remove(self._temp_table)
                except Exception:  # pragma: no cover
                    pass

    @abc.abstractmethod
    def apply_strategy(
        self,
        conn: sa.Connection,
        trans: sa.Transaction,
    ):
        """
        Apply the upsert strategy-specific logic.

        This abstract method must be implemented by subclasses to define their
        specific upsert behavior. The method is called after the temporary table
        has been created and populated with candidate data.

        **Implementation Requirements**:

        Subclasses should implement the core database operations that define
        their upsert strategy:

        - **INSERT OR IGNORE**: Use LEFT JOIN to insert only non-conflicting records
        - **INSERT OR REPLACE**: Delete conflicting records, then insert all records
        - **UPSERT/MERGE**: Update existing records, insert new ones

        **State Management**:

        Implementations should update the appropriate result counters:

        - ``self._ignored_rows`` - Records ignored (INSERT OR IGNORE)
        - ``self._replaced_rows`` - Records replaced (INSERT OR REPLACE)
        - ``self._updated_rows`` - Records updated (UPSERT/MERGE)
        - ``self._inserted_rows`` - New records inserted

        **Error Handling**:

        Implementations can use testing flags for controlled failure simulation:

        - ``self._raise_on_target_delete`` - Simulate deletion failures
        - ``self._raise_on_target_insert`` - Simulate insertion failures

        :param conn: Database connection within active transaction
        :param trans: Active transaction context

        :raises UpsertTestError: When testing flags are enabled
        :raises NotImplementedError: If subclass doesn't implement this method

        **Example Implementation**::

            def apply_strategy(self, conn, trans):
                # INSERT OR IGNORE strategy
                stmt = self.table.insert().from_select(
                    list(self._temp_table.columns.keys()),
                    sa.select(self._temp_table).select_from(
                        self._temp_table.outerjoin(self.table, ...)
                    ).where(self.table.c[self.pk_name].is_(None))
                )
                result = conn.execute(stmt)
                self._inserted_rows = result.rowcount or 0
                self._ignored_rows = len(self.values) - self._inserted_rows
        """
        raise NotImplementedError

    def execute_operation(
        self,
        conn: sa.Connection,
        trans: sa.Transaction,
    ):
        """
        Execute the complete upsert operation within the provided transaction context.

        This method implements the Template Method pattern, orchestrating the
        common workflow while delegating strategy-specific logic to subclasses.
        It operates within either a user-managed or auto-managed transaction
        depending on how the executor was configured.

        **Execution Flow**:

        1. **Create Staging**: Create temporary table for bulk data processing
        2. **Load Data**: Bulk insert all candidate records into staging area
        3. **Apply Strategy**: Execute subclass-specific upsert logic
        4. **Cleanup**: Remove temporary resources

        **Error Handling**:

        If any step fails, the method re-raises the original exception.
        Cleanup of temporary tables is handled by the caller based on
        transaction mode (auto vs user-managed).

        **Performance Notes**:

        The temporary table approach provides significant performance benefits
        over row-by-row operations by enabling efficient bulk SQL operations
        and leveraging database JOIN optimizations.

        :param conn: Database connection within active transaction
        :param trans: Active transaction context

        :returns: Tuple of operation results (varies by strategy)

        :raises UpsertTestError: When testing flags are enabled
        :raises Exception: Any database or application errors during operation
        """
        try:
            # Step 1: Create temporary staging table for bulk data processing
            self.create_temp_table(conn)

            # Step 2: Bulk load all candidate records into staging area
            # This is much faster than individual row processing
            self.insert_temp_data(conn)

            # Step 3: Execute strategy-specific upsert logic
            # Subclasses implement their specific database operations here
            self.apply_strategy(conn, trans)

            # Step 4: Clean up temporary table in normal success path
            self.cleanup_temp_table_on_success(conn)

            return self._ignored_rows, self._inserted_rows

        except Exception as e:
            # Handle testing flag for temp table cleanup errors
            if self._temp_table_created and self._raise_on_temp_table_drop:
                raise UpsertTestError("error on temp table cleanup")
            # Re-raise original exception - cleanup handled by caller based on transaction mode
            raise e

    def run(self):
        """
        Execute the complete upsert operation with appropriate transaction management.

        This is the main entry point for upsert operations. It handles both
        auto-managed and user-managed transaction modes, ensuring proper
        error handling and cleanup in all scenarios.

        **Transaction Modes**:

        - **Auto-managed** (``conn=None, trans=None``): Creates and manages
          its own database transaction. Automatically commits on success
          and rolls back on failure.

        - **User-managed** (``conn=Connection, trans=Transaction``): Operates
          within the caller's existing transaction. The caller is responsible
          for committing or rolling back the transaction.

        **Error Handling**:

        In both modes, temporary table cleanup is handled appropriately:
        - Auto-managed: Uses fresh connection after transaction rollback
        - User-managed: Cleans up but preserves caller's transaction state

        **Usage Examples**::

            # Auto-managed transaction
            executor = MyUpsertExecutor.new(engine, table, data)
            result = executor.run()

            # User-managed transaction
            with engine.connect() as conn:
                with conn.begin() as trans:
                    executor = MyUpsertExecutor.new(
                        engine, table, data, conn=conn, trans=trans
                    )
                    result = executor.run()
                    # Additional operations...
                    # trans.commit() handled by context manager

        :returns: Tuple of operation results (varies by strategy):
            - INSERT OR IGNORE: (_ignored_rows, _inserted_rows)
            - INSERT OR REPLACE: (_replaced_rows, _inserted_rows)
            - UPSERT/MERGE: (_updated_rows, _inserted_rows)

        :raises ValueError: When transaction mode parameters are inconsistent
        :raises UpsertTestError: When testing flags are enabled
        :raises Exception: Any database or application errors during operation
        """
        self.clone_temp_table()

        if self.user_managed:
            # User-managed transaction mode: operate within caller's transaction context
            try:
                return self.execute_operation(self.conn, self.trans)
            except Exception:
                # Clean up temp table but don't manage transaction - caller is responsible
                self.cleanup_temp_table_on_failure()
                raise
        elif self.auto_managed:
            # Auto-managed transaction mode: create and manage our own transaction
            try:
                with self.engine.connect() as conn:
                    with conn.begin() as trans:
                        result = self.execute_operation(conn, trans)
                        # Transaction automatically committed on successful exit
                        return result
            except Exception as e:
                # Transaction automatically rolled back by context manager
                # Clean up temp tables after all connections are properly closed
                self.cleanup_temp_table_on_failure()
                raise e
        # should never reach here
        else:  # pragma: no cover
            raise NotImplementedError
