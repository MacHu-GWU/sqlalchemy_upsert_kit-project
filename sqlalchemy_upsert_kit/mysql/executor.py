# -*- coding: utf-8 -*-

"""
MySQL executor: what MySQL needs beyond the shared
:class:`~sqlalchemy_upsert_kit.core.executor.UpsertExecutor`.

- **The upsert statement** is ``INSERT ... SELECT ... ON DUPLICATE KEY UPDATE``,
  whose update reads the staged values straight from the staging table
  (``VALUES()`` is deprecated). MySQL has no ``DO NOTHING``: ignore sets the
  primary key to itself.
- **Counts.** SQLAlchemy's MySQL drivers always set ``CLIENT_FOUND_ROWS``, so the
  row count is 1 for an inserted row, 2 for a changed row, and 1 for an
  unchanged one: it cannot tell inserted rows apart. The conflicts are counted
  first for every strategy, and the rest of the records are the inserted rows.
- **Temporary table DDL.** ``CREATE TEMPORARY TABLE`` does not commit, but is not
  undone by a rollback, so a failed call drops the staging table itself. Plain
  ``DROP TABLE``, which SQLAlchemy emits, commits the transaction, so the staging
  table is dropped with ``DROP TEMPORARY TABLE``.
- **Identifiers** are limited to 64 characters; the dialect reports 255.
- **Other unique keys.** ``ON DUPLICATE KEY UPDATE`` fires on any unique key, not
  only the primary key: a new record whose unique value is already taken would
  silently update that other row. :meth:`MysqlUpsertExecutor.check_unique_keys`
  rejects such records before the upsert, as the other databases do, for the
  unique keys the ``Table`` declares. :meth:`MysqlUpsertExecutor.check_after_upsert`
  catches every other case afterwards: a unique index the ``Table`` leaves out,
  or a row the check could not see in the transaction's old snapshot.
"""

import typing as T

import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert as mysql_insert

from ..exc import UniqueKeyConflictError
from ..core.executor import UpsertExecutor
from ..core.schema import get_unique_keys

#: The longest identifier MySQL accepts.
MAX_IDENTIFIER_LENGTH = 64


class MysqlUpsertExecutor(UpsertExecutor):
    """
    The upsert workflow on MySQL.
    """

    rowcount_counts_inserts = False

    def max_identifier_length(self) -> int:
        """
        64, MySQL's limit, instead of the 255 that the dialect reports.
        """
        return MAX_IDENTIFIER_LENGTH

    def _drop_temporary(
        self,
        conn: sa.Connection,
        staging: sa.Table,
        sql: str,
    ) -> None:
        name = conn.dialect.identifier_preparer.format_table(staging)
        conn.exec_driver_sql(f"{sql} {name}")

    def drop_staging(self, conn: sa.Connection, staging: sa.Table) -> None:
        """
        ``DROP TEMPORARY TABLE``, which, unlike ``DROP TABLE``, does not commit.
        """
        self._drop_temporary(conn, staging, "DROP TEMPORARY TABLE")

    def cleanup_after_rollback(self, conn: sa.Connection, staging: sa.Table) -> None:
        """
        Drop the staging table, if it was created: rolling back does not remove it.
        """
        self._drop_temporary(conn, staging, "DROP TEMPORARY TABLE IF EXISTS")

    def check_after_upsert(self, conn: sa.Connection, staging: sa.Table) -> None:
        """
        Raise :class:`sqlalchemy.exc.IntegrityError` if a record's primary key is
        not in the table after the upsert: ``ON DUPLICATE KEY UPDATE`` updated
        another row on a unique key instead of inserting the record. The upsert
        never changes a primary key, so every record's key must be there.

        :meth:`check_unique_keys` rejects such records before the upsert, with a
        clearer message, but only for the unique keys the ``Table`` declares,
        and its plain reads see the transaction's snapshot, which in a
        user-managed call can miss rows committed since. This check catches the
        rest. It is a locking read (``LOCK IN SHARE MODE``), so it sees the
        latest rows; it reads only rows the upsert has just written and locked,
        so it waits for no other transaction.
        """
        pk_key = self.pk_column.key
        stmt = (
            sa.select(sa.func.count())
            .select_from(
                staging.outerjoin(self.table, self.table.c[pk_key] == staging.c[pk_key])
            )
            .where(self.table.c[pk_key].is_(None))
            .with_for_update(read=True)
        )
        n = conn.execute(stmt).scalar_one()
        if n:
            raise sa.exc.IntegrityError(
                statement=str(stmt.compile(self.engine)),
                params=None,
                orig=UniqueKeyConflictError(
                    f"{n} record(s) were not written to {self.table.name!r}: "
                    f"MySQL updated another row with the same value on a unique "
                    f"key other than the primary key"
                ),
            )

    def check_unique_keys(self, conn: sa.Connection, staging: sa.Table) -> None:
        """
        Raise :class:`sqlalchemy.exc.IntegrityError` if, on a unique key other
        than the primary key (from :func:`~sqlalchemy_upsert_kit.core.schema.get_unique_keys`):

        - a record with a new primary key has the value of an existing row,
          which ``ON DUPLICATE KEY UPDATE`` would update instead of raising; or
        - several records would write the same value: the records with a new
          primary key, plus the conflicting records when the strategy updates a
          column of the key. A conflicting record is compared by the value its
          row will have: the updated columns from the record, the others from
          the row.

        A conflicting record whose row would end up with the value of another
        existing row needs no check: MySQL raises on that update itself.
        Values are compared as they are before the call, so a value that a
        conflicting record moves away from is still taken. NULL values never
        conflict. Each query reads the staging table once: MySQL cannot open a
        temporary table twice in one statement.
        """
        pk_key = self.pk_column.key
        # The row of each record's own primary key, if it exists.
        own = self.table.alias("own_row")
        with_own = staging.outerjoin(own, own.c[pk_key] == staging.c[pk_key])
        is_new = own.c[pk_key].is_(None)
        for cols in get_unique_keys(self.table):
            keys = [col.key for col in cols]
            taken = (
                sa.select(sa.func.count())
                .select_from(
                    with_own.join(
                        self.table,
                        sa.and_(*[staging.c[key] == self.table.c[key] for key in keys]),
                    )
                )
                .where(is_new)
            )
            n = conn.execute(taken).scalar_one()
            if n:
                self._raise_conflict(
                    taken,
                    cols,
                    f"{n} record(s) with a new primary key have the value of an "
                    f"existing row",
                )
            # The value each record's row will have.
            values = [
                staging.c[key]
                if key in self.update_keys
                else sa.case((is_new, staging.c[key]), else_=own.c[key])
                for key in keys
            ]
            shared = (
                sa.select(*values)
                .select_from(with_own)
                .where(*[value.is_not(None) for value in values])
                .group_by(*values)
                .having(sa.func.count() > 1)
            )
            if not set(keys) & set(self.update_keys):
                shared = shared.where(is_new)
            n = len(conn.execute(shared).all())
            if n:
                self._raise_conflict(
                    shared,
                    cols,
                    f"{n} value(s) would be written by several records",
                )

    def _raise_conflict(
        self,
        stmt: sa.Select,
        cols: list[sa.Column],
        problem: str,
    ) -> T.NoReturn:
        names = ", ".join(col.name for col in cols)
        message = (
            f"On the unique key ({names}) of {self.table.name!r}, which is not "
            f"the primary key: {problem}"
        )
        raise sa.exc.IntegrityError(
            statement=str(stmt.compile(self.engine)),
            params=None,
            orig=UniqueKeyConflictError(message),
        )

    def build_upsert(self, staging: sa.Table) -> sa.Insert:
        """
        ``INSERT INTO <table> SELECT ... FROM <staging> ON DUPLICATE KEY UPDATE
        <pk> = <table>.<pk> | <update_keys> = <staging>.<update_keys>``.
        """
        keys = list(staging.columns.keys())
        stmt = mysql_insert(self.table).from_select(
            [self.table.c[key] for key in keys],
            sa.select(*[staging.c[key] for key in keys]),
        )
        if not self.update_keys:
            # A no-op update: MySQL has no DO NOTHING.
            return stmt.on_duplicate_key_update({self.pk_column: self.pk_column})
        return stmt.on_duplicate_key_update(
            {self.table.c[key]: staging.c[key] for key in self.update_keys}
        )
