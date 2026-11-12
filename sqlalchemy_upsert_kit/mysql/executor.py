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
  rejects such records before the upsert, as the other databases do.
"""

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

    def _drop_temporary(self, conn: sa.Connection, staging: sa.Table, sql: str):
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

    def check_unique_keys(self, conn: sa.Connection, staging: sa.Table) -> None:
        """
        Raise :class:`sqlalchemy.exc.IntegrityError` if, on a unique key other
        than the primary key (from :func:`~sqlalchemy_upsert_kit.core.schema.get_unique_keys`):

        - a record with a new primary key has the same value as an existing row,
          which ``ON DUPLICATE KEY UPDATE`` would update instead of raising; or
        - records that write the key share a value: the records with a new
          primary key, and, when the strategy updates a column of the key, the
          conflicting records too.

        A conflicting record that updates the key to a value an existing row has
        needs no check: MySQL raises on that update itself. NULL values never
        conflict. Each query reads the staging table once: MySQL cannot open a
        temporary table twice in one statement.
        """
        pk_key = self.pk_column.key
        other = self.table.alias("existing")
        is_new = ~sa.exists().where(other.c[pk_key] == staging.c[pk_key])
        for cols in get_unique_keys(self.table):
            keys = [col.key for col in cols]
            taken = (
                sa.select(sa.func.count())
                .select_from(
                    staging.join(
                        self.table,
                        sa.and_(*[staging.c[key] == self.table.c[key] for key in keys]),
                    )
                )
                .where(is_new)
            )
            shared = sa.select(*[staging.c[key] for key in keys]).where(
                *[staging.c[key].is_not(None) for key in keys]
            )
            if not set(keys) & set(self.update_keys):
                shared = shared.where(is_new)
            shared = shared.group_by(*[staging.c[key] for key in keys]).having(
                sa.func.count() > 1
            )
            shared = sa.select(sa.func.count()).select_from(shared.subquery())
            for stmt, problem in [
                (taken, "have the same value as an existing row"),
                (shared, "share a value with another record"),
            ]:
                n = conn.execute(stmt).scalar_one()
                if n:
                    names = ", ".join(col.name for col in cols)
                    message = (
                        f"{n} record(s) {problem} on the unique key ({names}) of "
                        f"{self.table.name!r}, which is not the primary key"
                    )
                    raise sa.exc.IntegrityError(
                        statement=str(stmt.compile(conn)),
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
