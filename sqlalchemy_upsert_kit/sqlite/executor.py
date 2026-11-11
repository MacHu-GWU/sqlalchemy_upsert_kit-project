# -*- coding: utf-8 -*-

"""
SQLite executor: what SQLite needs beyond the shared
:class:`~sqlalchemy_upsert_kit.core.executor.UpsertExecutor`.

- **A real transaction first.** Python's ``sqlite3`` driver sends ``BEGIN``
  only before the first data-changing statement. Without one, the SAVEPOINT
  would itself be the outermost transaction, and releasing it would commit the
  upsert even if the caller later rolls back. So :meth:`begin_transaction`
  sends ``BEGIN IMMEDIATE`` when no transaction is open. ``IMMEDIATE`` takes
  the write lock up front, so concurrent upserts wait for each other instead
  of failing with "database is locked".
- **The upsert statement** is SQLite's ``INSERT ... SELECT ... ON CONFLICT``
  (SQLite 3.24+). Its ``SELECT`` needs a ``WHERE`` clause, or SQLite parses
  ``ON CONFLICT`` as part of the join.
"""

import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..core.executor import UpsertExecutor


class SqliteUpsertExecutor(UpsertExecutor):
    """
    The upsert workflow on SQLite.
    """

    def begin_transaction(self, conn: sa.Connection) -> None:
        """
        Send ``BEGIN IMMEDIATE`` if the driver has not opened a transaction yet.
        """
        if not conn.connection.dbapi_connection.in_transaction:
            conn.exec_driver_sql("BEGIN IMMEDIATE")

    def build_upsert(self, staging: sa.Table) -> sa.Insert:
        """
        ``INSERT INTO <table> SELECT ... FROM <staging> WHERE true ON CONFLICT (<pk>)
        DO NOTHING | DO UPDATE SET <update_keys>``.
        """
        keys = list(staging.columns.keys())
        stmt = sqlite_insert(self.table).from_select(
            [self.table.c[key] for key in keys],
            sa.select(*[staging.c[key] for key in keys]).where(sa.true()),
        )
        if not self.update_keys:
            return stmt.on_conflict_do_nothing(index_elements=[self.pk_column])
        return stmt.on_conflict_do_update(
            index_elements=[self.pk_column],
            set_={self.table.c[key]: stmt.excluded[key] for key in self.update_keys},
        )
