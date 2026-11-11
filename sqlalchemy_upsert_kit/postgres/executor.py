# -*- coding: utf-8 -*-

"""
PostgreSQL executor: what PostgreSQL needs beyond the shared
:class:`~sqlalchemy_upsert_kit.core.executor.UpsertExecutor`.

PostgreSQL begins a real transaction on ``Connection.begin()``, and its DDL is
transactional, so only the upsert statement is its own: PostgreSQL's
``INSERT ... SELECT ... ON CONFLICT``.
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..core.executor import UpsertExecutor


class PostgresUpsertExecutor(UpsertExecutor):
    """
    The upsert workflow on PostgreSQL.
    """

    def build_upsert(self, staging: sa.Table) -> sa.Insert:
        """
        ``INSERT INTO <table> SELECT ... FROM <staging> ON CONFLICT (<pk>)
        DO NOTHING | DO UPDATE SET <update_keys>``.
        """
        keys = list(staging.columns.keys())
        stmt = pg_insert(self.table).from_select(
            [self.table.c[key] for key in keys],
            sa.select(*[staging.c[key] for key in keys]),
        )
        if not self.update_keys:
            return stmt.on_conflict_do_nothing(index_elements=[self.pk_column])
        return stmt.on_conflict_do_update(
            index_elements=[self.pk_column],
            set_={self.table.c[key]: stmt.excluded[key] for key in self.update_keys},
        )
