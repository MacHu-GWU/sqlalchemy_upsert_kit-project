# -*- coding: utf-8 -*-

"""
Table introspection and the staging table definition, shared by every backend.
"""

import uuid

import sqlalchemy as sa


def get_pk_column(table: sa.Table) -> sa.Column:
    """
    Return the single primary key column of ``table``.

    :raises ValueError: If the table has zero or several primary key columns.
    """
    pks = list(table.primary_key.columns)
    if len(pks) != 1:
        raise ValueError(
            f"Table {table.name!r} must have exactly one primary key column, "
            f"found {[col.name for col in pks]}"
        )
    return pks[0]


def get_writable_columns(table: sa.Table) -> list[sa.Column]:
    """
    Return the columns an upsert may write: every column except generated
    (``Computed``) ones.
    """
    return [col for col in table.columns if col.computed is None]


def make_staging_name(
    table: sa.Table,
    max_length: int,
) -> str:
    """
    Generate a staging table name: ``temp_<8 hex>_<table name>``, cut to
    ``max_length`` (the dialect's identifier limit, 63 on PostgreSQL).

    The random part keeps names unique across retries and concurrent calls.
    """
    return f"temp_{uuid.uuid4().hex[:8]}_{table.name}"[:max_length]


def make_staging_table(
    table: sa.Table,
    metadata: sa.MetaData,
    name: str,
) -> sa.Table:
    """
    Define the ``TEMPORARY`` staging table for ``table``.

    It copies each writable column's name, key, type and defaults, and marks the
    primary key, but nothing else: no indexes, unique constraints, foreign keys,
    sequences or identities, which could collide with the target's names or
    reference tables a temporary table may not reference. A temporary table is
    private to its connection, so concurrent upserts never see each other's.

    :param table: The target table
    :param metadata: Metadata that holds the staging table definition
    :param name: Name of the staging table
    """
    columns = list()
    for col in get_writable_columns(table):
        kwargs = dict(
            key=col.key,
            primary_key=col.primary_key,
            autoincrement=False,  # the key always comes from the input records
        )
        if isinstance(col.server_default, sa.DefaultClause):
            kwargs["server_default"] = sa.DefaultClause(col.server_default.arg)
        if col.default is not None and not col.default.is_sequence:
            kwargs["default"] = col.default.arg
        columns.append(sa.Column(col.name, col.type, **kwargs))
    return sa.Table(name, metadata, *columns, prefixes=["TEMPORARY"])
