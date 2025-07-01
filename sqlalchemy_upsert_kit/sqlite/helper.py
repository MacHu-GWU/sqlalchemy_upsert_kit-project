# -*- coding: utf-8 -*-

import typing as T
import abc

import sqlalchemy as sa

from ..exc import UpsertTestError
from ..utils import get_pk_name, get_temp_table_name, clone_temp_table

import dataclasses
from functools import cached_property


@dataclasses.dataclass
class UpsertHelper(abc.ABC):  # need a better name
    # --- raw fields ---
    engine: sa.Engine = dataclasses.field()
    table: sa.Table = dataclasses.field()
    values: list[dict[str, T.Any]] = dataclasses.field()
    metadata: T.Optional[sa.MetaData] = dataclasses.field()
    temp_table_name: T.Optional[str] = dataclasses.field()
    conn: T.Optional[sa.Connection] = dataclasses.field()
    trans: T.Optional[sa.Transaction] = dataclasses.field()
    _raise_on_temp_table_create: bool = dataclasses.field()
    _raise_on_temp_data_insert: bool = dataclasses.field()
    _raise_on_target_delete: bool = dataclasses.field()
    _raise_on_target_insert: bool = dataclasses.field()
    _raise_on_temp_table_drop: bool = dataclasses.field()
    # --- generated fields ---
    ignored_rows: int = dataclasses.field(default=0)
    replaced_rows: int = dataclasses.field(default=0)
    updated_rows: int = dataclasses.field(default=0)
    inserted_rows: int = dataclasses.field(default=0)
    temp_table: sa.Table = dataclasses.field(init=False)
    temp_table_created: bool = dataclasses.field(default=False)

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
        _raise_on_temp_table_create: bool = False,
        _raise_on_temp_data_insert: bool = False,
        _raise_on_target_delete: bool = False,
        _raise_on_target_insert: bool = False,
        _raise_on_temp_table_drop: bool = False,
    ):
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
            _raise_on_temp_table_create=_raise_on_temp_table_create,
            _raise_on_temp_data_insert=_raise_on_temp_data_insert,
            _raise_on_target_delete=_raise_on_target_delete,
            _raise_on_target_insert=_raise_on_target_insert,
            _raise_on_temp_table_drop=_raise_on_temp_table_drop,
        )

    @cached_property
    def user_managed(self) -> bool:
        return (self.conn is not None) and (self.trans is not None)

    @cached_property
    def auto_managed(self) -> bool:
        return (self.conn is None) and (self.trans is None)

    def __post_init__(self):
        if not (self.user_managed or self.auto_managed):  # pragma: no cover
            raise ValueError(
                "Either both conn and trans must be provided (user-managed mode), "
                "or both must be None (auto-managed mode)"
            )

    @cached_property
    def pk_name(self) -> str:
        return get_pk_name(self.table)

    def clone_temp_table(self):
        self.temp_table = clone_temp_table(
            original_table=self.table,
            metadata=self.metadata,
            temp_table_name=self.temp_table_name,
        )

    def prepare_temp_table(self) -> sa.Table:
        pass

    def cleanup_temp_table(self):
        """
        Clean up temporary table using a fresh connection.

        This function is called when cleanup needs to happen outside the main
        transaction context, typically in error scenarios. It uses a fresh
        connection to avoid SQLite database lock issues that can occur when
        the original transaction has been rolled back.
        """
        if self.temp_table_created:
            try:
                # Use fresh connection to avoid database locks from rolled-back transactions
                with self.engine.connect() as cleanup_conn:
                    self.temp_table.drop(cleanup_conn)
                    cleanup_conn.commit()
                self.metadata.remove(self.temp_table)
            except Exception:
                # Cleanup failures should not mask the original exception
                # This can happen if temp table was already dropped or doesn't exist
                try:
                    # Try to remove from metadata anyway to prevent resource leaks
                    self.metadata.remove(self.temp_table)
                except Exception:  # pragma: no cover
                    pass

    @abc.abstractmethod
    def execute_core_upsert_logic(
        self,
        conn: sa.Connection,
        trans: sa.Transaction,
    ):
        raise NotImplementedError

    def execute_upsert_logic(
        self,
        conn: sa.Connection,
        trans: sa.Transaction,
    ):
        """
        Execute the core insert-or-ignore logic within the provided transaction context.

        This function implements the temporary table strategy for bulk conditional
        insertion. It operates within either a user-managed or auto-managed transaction
        depending on how the parent function was called.
        """
        try:
            # Step 1: Create temporary staging table for bulk data processing
            if self._raise_on_temp_table_create:  # Testing flag
                raise UpsertTestError("error on temp table creation")
            self.temp_table.create(conn)
            self.temp_table_created = True

            # Step 2: Bulk load all candidate records into staging area
            # This is much faster than individual row processing
            if self._raise_on_temp_data_insert:  # Testing flag
                raise UpsertTestError("error on temp data insertion")
            conn.execute(self.temp_table.insert(), self.values)

            # Step 3: Insert only records that don't exist in target table
            # Uses LEFT JOIN to efficiently identify non-conflicting records
            self.execute_core_upsert_logic(conn, trans)

            # Step 4: Clean up temporary table in normal success path
            if self.temp_table_created:
                if self._raise_on_temp_table_drop:  # Testing flag
                    raise UpsertTestError("error on temp table cleanup")
                # Normal cleanup - drop temp table within the same connection
                self.temp_table.drop(conn)
                self.metadata.remove(self.temp_table)
                self.temp_table_created = False

            return self.ignored_rows, self.inserted_rows

        except Exception as e:
            # Handle testing flag for temp table cleanup errors
            if self.temp_table_created and self._raise_on_temp_table_drop:
                raise UpsertTestError("error on temp table cleanup")
            # Re-raise original exception - cleanup handled by caller based on transaction mode
            raise e

    def run(self):  # need a better name
        self.clone_temp_table()

        if self.user_managed:
            # User-managed transaction mode: operate within caller's transaction context
            try:
                return self.execute_upsert_logic(self.conn, self.trans)
            except Exception:
                # Clean up temp table but don't manage transaction - caller is responsible
                self.cleanup_temp_table()
                raise
        elif self.auto_managed:
            # Auto-managed transaction mode: create and manage our own transaction
            try:
                with self.engine.connect() as conn:
                    with conn.begin() as trans:
                        result = self.execute_upsert_logic(conn, trans)
                        # Transaction automatically committed on successful exit
                        return result
            except Exception as e:
                # Transaction automatically rolled back by context manager
                # Clean up temp tables after all connections are properly closed
                self.cleanup_temp_table()
                raise e
        # should never reach here
        else:  # pragma: no cover
            raise NotImplementedError
