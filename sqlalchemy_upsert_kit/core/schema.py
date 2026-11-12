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


def get_unique_keys(table: sa.Table) -> list[list[sa.Column]]:
    """
    Return the column lists of every unique key of ``table`` other than its
    primary key: each ``UniqueConstraint`` (``Column(unique=True)`` makes one)
    and each unique ``Index``, in table definition order, without repeats.

    Only keys whose parts are all writable columns are returned: an index on an
    expression, or a key that includes a generated column, has no values in the
    staging table to check.
    """
    pk_keys = {col.key for col in table.primary_key.columns}
    writable = set(get_writable_columns(table))
    candidates = [
        list(con.columns)
        for con in table.constraints
        if isinstance(con, sa.UniqueConstraint)
        and not isinstance(con, sa.PrimaryKeyConstraint)
    ]
    candidates += [
        list(index.expressions)
        for index in sorted(table.indexes, key=lambda index: index.name or "")
        if index.unique
    ]
    keys = list()
    seen = set()
    for cols in candidates:
        if not all(isinstance(col, sa.Column) and col in writable for col in cols):
            continue
        key_set = frozenset(col.key for col in cols)
        if key_set == pk_keys or key_set in seen:
            continue
        seen.add(key_set)
        keys.append(cols)
    return keys


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


#: Table options that decide how values compare, copied to the staging table:
#: ``<dialect>_<option>``, such as ``mysql_collate``.
COMPARISON_OPTIONS = (
    "charset",
    "default_charset",
    "character_set",
    "default_character_set",
    "collate",
    "default_collate",
)


def make_staging_table(
    table: sa.Table,
    metadata: sa.MetaData,
    name: str,
) -> sa.Table:
    """
    Define the ``TEMPORARY`` staging table for ``table``.

    It copies each writable column's name, key, type and defaults, marks the
    primary key, and copies the table's character set and collation options
    (:data:`COMPARISON_OPTIONS`), so staged values compare as in the target. It
    copies nothing else: no indexes, unique constraints, foreign keys, sequences
    or identities, which could collide with the target's names or reference
    tables a temporary table may not reference. A temporary table is private to
    its connection, so concurrent upserts never see each other's.

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
    options = {
        key: value
        for key, value in table.kwargs.items()
        if key.split("_", 1)[-1] in COMPARISON_OPTIONS
    }
    return sa.Table(name, metadata, *columns, prefixes=["TEMPORARY"], **options)
