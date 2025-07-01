# -*- coding: utf-8 -*-

import typing as T
from datetime import datetime, timezone

import sqlalchemy as sa


def get_utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def get_pk_name(table: sa.Table) -> str:
    """
    Extract the primary key column name from a SQLAlchemy table.

    This function ensures the table has exactly one primary key column,
    which is required for the bulk operations to work correctly.

    Args:
        table: SQLAlchemy table object

    Returns:
        Name of the primary key column

    Raises:
        ValueError: If table has zero or multiple primary key columns

    Example:
        >>> table = sa.Table(
        ...     'users',
        ...     metadata,
        ...     sa.Column('id', sa.Integer, primary_key=True)
        ... )
        >>> get_pk_name(table)
        'id'
    """
    pks = list(table.primary_key)
    if len(pks) != 1:  # pragma: no cover
        pk_names = [pk.name for pk in pks]
        raise ValueError(
            f"Table must have exactly one primary key, but found: {pk_names}"
        )
    pk_name = pks[0].name
    return pk_name


def get_temp_table_name(original_table_name: str) -> str:
    dt = get_utc_now().strftime("%Y%m%d%H%M%S")
    temp_table_name = f"temp_{dt}_" + original_table_name
    return temp_table_name


def clone_temp_table(
    original_table: sa.Table,
    metadata: sa.MetaData,
    temp_table_name: T.Optional[str] = None,
) -> sa.Table:
    """
    Create a temporary table with the same schema as the original table.

    This function clones the structure of an existing table to create a temporary
    table for bulk operations. The temporary table inherits all columns, types,
    and constraints from the original table.

    Args:
        original_table: The table to clone
        metadata: Metadata object for the temporary table.
            Should be a separate instance from the original table's metadata
            to avoid conflicts and enable proper cleanup.
        temp_table_name: Custom name for the temporary table.
            If None, generates a unique name with timestamp to avoid conflicts
            in concurrent environments.

    Returns:
        New temporary table with identical schema

    .. note::

        - Use a separate MetaData instance to isolate the temporary table
        - In high-concurrency scenarios, consider providing unique temp_table_name
        - The temporary table is not automatically bound to any engine

    Example:
        >>> metadata = sa.MetaData()
        >>> _temp_table = clone_temp_table(users_table, metadata)
        >>> # _temp_table has same columns as users_table but different metadata
    """
    if temp_table_name is None:
        temp_table_name = get_temp_table_name(original_table.name)
    temp_table = original_table.to_metadata(metadata, name=temp_table_name)
    return temp_table
