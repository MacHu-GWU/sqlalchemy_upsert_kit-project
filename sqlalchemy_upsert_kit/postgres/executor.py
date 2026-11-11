# -*- coding: utf-8 -*-

"""
PostgreSQL executor: the parts of the shared upsert workflow that PostgreSQL
does differently from the dialect-agnostic
:class:`~sqlalchemy_upsert_kit.core.executor.UpsertExecutor`.

- **Staging table**: a ``TEMPORARY`` table with ``ON COMMIT DROP``. It is
  private to the session, so concurrent upserts never collide on its name,
  and PostgreSQL drops it at commit even if the explicit drop never runs.
- **Columns only**: the staging table copies each column's name, type, primary
  key and defaults, but not indexes, unique constraints or foreign keys. Index
  names are unique per schema, and a temporary table may not reference a
  permanent one, so cloning them would fail.
- **Failure cleanup**: PostgreSQL DDL is transactional. The staging table is
  created inside the operation's transaction, so the rollback removes it, and
  no fresh connection is needed.
"""

import typing as T
import dataclasses

import sqlalchemy as sa

from ..core.executor import UpsertExecutor


def clone_staging_table(
    original_table: sa.Table,
    metadata: sa.MetaData,
    temp_table_name: str,
) -> sa.Table:
    """
    Create the ``TEMPORARY ... ON COMMIT DROP`` staging table definition for
    ``original_table``, copying its columns but none of its indexes or constraints.

    :param original_table: The target table to clone
    :param metadata: Metadata object that holds the staging table
    :param temp_table_name: Name of the staging table

    :returns: The staging table, not yet created in the database
    """
    columns = list()
    for col in original_table.columns:
        kwargs: dict[str, T.Any] = dict(
            primary_key=col.primary_key,
            autoincrement=False,  # values always come from the input rows
        )
        if isinstance(col.server_default, sa.DefaultClause):
            kwargs["server_default"] = sa.DefaultClause(col.server_default.arg)
        if col.default is not None and not col.default.is_sequence:
            kwargs["default"] = col.default.arg
        columns.append(sa.Column(col.name, col.type, **kwargs))
    return sa.Table(
        temp_table_name,
        metadata,
        *columns,
        prefixes=["TEMPORARY"],
        postgresql_on_commit="DROP",
    )


@dataclasses.dataclass
class PostgresUpsertExecutor(UpsertExecutor):
    """
    Base class of the PostgreSQL upsert strategies.

    See the module docstring for how it differs from
    :class:`~sqlalchemy_upsert_kit.core.executor.UpsertExecutor`.
    """

    def clone_temp_table(self):
        """
        Build the temporary staging table definition, see :func:`clone_staging_table`.
        """
        self._temp_table = clone_staging_table(
            original_table=self.table,
            metadata=self.metadata,
            temp_table_name=self.temp_table_name,
        )

    def cleanup_temp_table_on_failure(self):
        """
        Forget the staging table after a failure, without touching the database.

        The staging table was created inside the operation's transaction:
        a rollback removes it, and a commit drops it (``ON COMMIT DROP``).
        """
        if self._temp_table.name in self.metadata.tables:
            self.metadata.remove(self._temp_table)
        self._temp_table_created = False

    def count_conflicts(self, conn: sa.Connection) -> int:
        """
        Count the staged rows whose primary key already exists in the target table.

        :param conn: Database connection that sees the staging table
        """
        stmt = (
            sa.select(sa.func.count())
            .select_from(self._temp_table)
            .join(
                self.table,
                self._temp_table.c[self.pk_name] == self.table.c[self.pk_name],
            )
        )
        return conn.execute(stmt).scalar_one()
