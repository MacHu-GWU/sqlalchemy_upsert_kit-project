# -*- coding: utf-8 -*-

"""
The dialect-agnostic upsert workflow, shared by every backend.

:class:`UpsertExecutor` runs one upsert as these steps, all inside a SAVEPOINT
on one connection::

    begin_transaction()            # backend hook: make sure a real transaction is open
    SAVEPOINT
      create the staging table     # TEMPORARY, private to the connection
      load the records into it
      check_unique_keys()          # backend hook: nothing by default
      count the conflicting rows   # when rowcount cannot tell them apart
      INSERT ... SELECT ... ON CONFLICT   # backend hook: build_upsert()
      check_after_upsert()         # backend hook: nothing by default
      drop_staging()               # backend hook
    RELEASE SAVEPOINT              # on error: ROLLBACK TO SAVEPOINT,
                                   # cleanup_after_rollback(), then re-raise

Rolling back to the savepoint undoes everything the upsert did, and leaves the
caller's transaction usable. In auto-managed mode the executor opens and
commits its own connection and transaction; in user-managed mode it runs on
the caller's connection and never commits.

A backend subclasses :class:`UpsertExecutor` and implements :meth:`build_upsert`.
It overrides the other hooks only where its database differs: a driver that
delays ``BEGIN`` (:meth:`begin_transaction`), DDL that commits or is not undone
by a rollback (:meth:`drop_staging`, :meth:`cleanup_after_rollback`), a wrong
identifier limit (:meth:`max_identifier_length`), a row count that does not
tell inserted rows apart (:attr:`rowcount_counts_inserts`), or an upsert that
does not reject other unique key conflicts (:meth:`check_unique_keys`,
:meth:`check_after_upsert`).
"""

import typing as T
import abc
import dataclasses

import sqlalchemy as sa

from ..exc import UpsertTestError

from .schema import get_pk_column
from .schema import make_staging_name
from .schema import make_staging_table
from .validation import validate_records
from .validation import resolve_update_keys

#: The steps where the private ``_fail_at`` test hook can inject a failure.
FAIL_STEPS = ("create_staging", "load_staging", "upsert", "drop_staging")


@dataclasses.dataclass
class UpsertExecutor(abc.ABC):
    """
    One bulk upsert of ``values`` into ``table`` with one strategy.

    All inputs are validated when the executor is created, before any SQL runs.
    :meth:`run` returns ``(conflicting_rows, inserted_rows)``: the number of
    records whose primary key already existed (ignored, replaced or updated,
    depending on the strategy), and the number of new rows.

    :param engine: Engine used in auto-managed mode
    :param table: Target table, with exactly one primary key column
    :param values: Records keyed by column key, all with the same keys
    :param strategy: ``"ignore"``, ``"replace"`` or ``"merge"``
    :param columns: Column keys that ``"merge"`` updates; unused otherwise
    :param metadata: Metadata that holds the staging table definition while
        the upsert runs. A new one when None.
    :param temp_table_name: Staging table name. Generated when None.
    :param conn: Caller's connection, for user-managed mode, with ``trans``
    :param trans: Caller's transaction, for user-managed mode, with ``conn``
    :param fail_at: **Testing only**: raise :class:`UpsertTestError` just
        before this step, one of :data:`FAIL_STEPS`.
    """

    # fmt: off
    engine: sa.Engine
    table: sa.Table
    values: list[dict[str, T.Any]]
    strategy: str
    columns: list[str] | None = None
    metadata: sa.MetaData | None = None
    temp_table_name: str | None = None
    conn: sa.Connection | None = None
    trans: sa.Transaction | None = None
    fail_at: str | None = None
    # set by __post_init__
    pk_column: sa.Column = dataclasses.field(init=False)
    update_keys: list[str] = dataclasses.field(init=False)
    # fmt: on

    #: True when the upsert's row count is exact: inserted rows only for
    #: ``DO NOTHING``, inserted plus updated rows for ``DO UPDATE``. A backend
    #: whose row count is not sets it to False; the conflicting rows are then
    #: always counted first, and the rest of the records are the inserted rows.
    rowcount_counts_inserts: T.ClassVar[bool] = True

    def __post_init__(self):
        if (self.conn is None) != (self.trans is None):
            raise ValueError(
                "Give both conn and trans (user-managed mode) or neither "
                "(auto-managed mode)"
            )
        if self.fail_at is not None and self.fail_at not in FAIL_STEPS:
            raise ValueError(
                f"fail_at must be one of {FAIL_STEPS}, got {self.fail_at!r}"
            )
        self.pk_column = get_pk_column(self.table)
        if self.values:
            validate_records(self.table, self.values)
        record_keys = self.values[0].keys() if self.values else []
        self.update_keys = resolve_update_keys(
            table=self.table,
            strategy=self.strategy,
            record_keys=record_keys,
            columns=self.columns,
        )
        if self.metadata is None:
            self.metadata = sa.MetaData()
        if self.temp_table_name is None:
            self.temp_table_name = make_staging_name(
                table=self.table,
                max_length=self.max_identifier_length(),
            )
        if self.temp_table_name == self.table.name:
            raise ValueError("temp_table_name must differ from the target table name")

    @property
    def user_managed(self) -> bool:
        """
        True when the upsert runs in the caller's transaction.
        """
        return self.conn is not None

    def max_identifier_length(self) -> int:
        """
        The longest table name the database accepts, which the generated staging
        name is cut to. The default is the dialect's limit.
        """
        return self.engine.dialect.max_identifier_length

    def begin_transaction(self, conn: sa.Connection) -> None:
        """
        Make sure ``conn`` is inside a real database transaction before the
        SAVEPOINT. The default does nothing: most drivers begin one on
        ``Connection.begin()``.
        """

    @abc.abstractmethod
    def build_upsert(self, staging: sa.Table) -> sa.Insert:
        """
        Build the one statement that copies every staged row into the target
        table, overwriting :attr:`update_keys` on conflicting rows (or nothing,
        when it is empty).
        """
        raise NotImplementedError

    def check_unique_keys(self, conn: sa.Connection, staging: sa.Table) -> None:
        """
        Raise before the upsert if the staged records would break a unique key
        other than the primary key. The default does nothing: an
        ``ON CONFLICT (<pk>)`` upsert raises such errors itself.
        """

    def check_after_upsert(self, conn: sa.Connection, staging: sa.Table) -> None:
        """
        Raise after the upsert, still inside the SAVEPOINT, if it did not write
        every record as it should; the rollback then undoes it. The default
        does nothing.
        """

    def drop_staging(self, conn: sa.Connection, staging: sa.Table) -> None:
        """
        Drop the staging table after the upsert, inside the SAVEPOINT, without
        committing the transaction.
        """
        staging.drop(conn)

    def cleanup_after_rollback(self, conn: sa.Connection, staging: sa.Table) -> None:
        """
        Called after a failure rolled back to the SAVEPOINT, before the error is
        re-raised. The default does nothing: the rollback removes the staging
        table on databases whose DDL is transactional.
        """

    def maybe_fail(self, step: str) -> None:
        """
        Raise the test failure if ``fail_at`` names this step.
        """
        if self.fail_at == step:
            raise UpsertTestError(f"injected failure before step {step!r}")

    def count_conflicts(self, conn: sa.Connection, staging: sa.Table) -> int:
        """
        Count the staged rows whose primary key already exists in the target.
        """
        pk_key = self.pk_column.key
        stmt = (
            sa.select(sa.func.count())
            .select_from(staging)
            .join(self.table, staging.c[pk_key] == self.table.c[pk_key])
        )
        return conn.execute(stmt).scalar_one()

    def run_on(self, conn: sa.Connection) -> tuple[int, int]:
        """
        Run the upsert on ``conn``, inside a SAVEPOINT of its open transaction.
        """
        self.begin_transaction(conn)
        staging = make_staging_table(
            table=self.table,
            metadata=self.metadata,
            name=self.temp_table_name,
        )
        try:
            with conn.begin_nested():
                self.maybe_fail("create_staging")
                staging.create(conn)
                self.maybe_fail("load_staging")
                conn.execute(staging.insert(), self.values)
                self.check_unique_keys(conn, staging)
                if self.update_keys or not self.rowcount_counts_inserts:
                    # DO UPDATE's rowcount counts inserted and updated rows
                    # together, so count the conflicts first. If another
                    # transaction writes the same keys in between, the split
                    # can be off; the sum is always len(values).
                    conflicts = self.count_conflicts(conn, staging)
                self.maybe_fail("upsert")
                stmt = self.build_upsert(staging)
                # SQLAlchemy keeps the rowcount of an INSERT only when asked to.
                res = conn.execute(stmt.execution_options(preserve_rowcount=True))
                if not self.rowcount_counts_inserts:
                    inserted = len(self.values) - conflicts
                elif self.update_keys:
                    inserted = res.rowcount - conflicts
                else:
                    # DO NOTHING's rowcount is exactly the inserted rows.
                    inserted = res.rowcount
                    conflicts = len(self.values) - inserted
                self.check_after_upsert(conn, staging)
                self.maybe_fail("drop_staging")
                self.drop_staging(conn, staging)
        except Exception as e:
            try:
                self.cleanup_after_rollback(conn, staging)
            except Exception as cleanup_error:
                # Keep the error that made the call fail; a broken connection
                # can make the cleanup fail too.
                e.add_note(f"cleanup after the rollback also failed: {cleanup_error!r}")
            raise
        finally:
            self.metadata.remove(staging)
        return conflicts, inserted

    def run(self) -> tuple[int, int]:
        """
        Run the upsert and return ``(conflicting_rows, inserted_rows)``.

        Empty ``values`` is a no-op that returns ``(0, 0)``.
        """
        if not self.values:
            return 0, 0
        if self.user_managed:
            return self.run_on(self.conn)
        with self.engine.connect() as conn:
            with conn.begin():
                return self.run_on(conn)
