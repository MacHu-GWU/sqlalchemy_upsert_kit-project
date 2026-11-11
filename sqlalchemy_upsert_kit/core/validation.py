# -*- coding: utf-8 -*-

"""
Up-front checks of the upsert inputs, shared by every backend, so that bad
input fails the same way everywhere, before any SQL runs.
"""

import typing as T

import sqlalchemy as sa

from .schema import get_pk_column
from .schema import get_writable_columns

STRATEGIES = ("ignore", "replace", "merge")


def validate_records(
    table: sa.Table,
    values: list[dict[str, T.Any]],
) -> None:
    """
    Check that the records can be staged without silent data loss.

    Every record must have the same keys, each a writable column key of
    ``table``, including the primary key, whose values must be non-None and unique.

    :raises ValueError: On the first record that breaks a rule.
    """
    pk_key = get_pk_column(table).key
    keys = set(values[0])
    writable = {col.key for col in get_writable_columns(table)}
    unknown = keys - writable
    if unknown:
        raise ValueError(
            f"Records have keys that are not writable columns of {table.name!r}: "
            f"{sorted(unknown)}"
        )
    if pk_key not in keys:
        raise ValueError(f"Records must include the primary key {pk_key!r}")
    seen = set()
    for i, record in enumerate(values):
        if record.keys() != keys:
            raise ValueError(
                f"Record {i} has keys {sorted(record)}, "
                f"but record 0 has {sorted(keys)}; all records need the same keys"
            )
        pk = record[pk_key]
        if pk is None:
            raise ValueError(f"Record {i} has no value for the primary key {pk_key!r}")
        if pk in seen:
            raise ValueError(f"Record {i} repeats the primary key {pk_key}={pk!r}")
        seen.add(pk)


def resolve_update_keys(
    table: sa.Table,
    strategy: str,
    record_keys: T.Iterable[str],
    columns: list[str] | None,
) -> list[str]:
    """
    Return the column keys a strategy overwrites on conflicting rows.

    - ``ignore``: none.
    - ``replace``: every column the records stage, except the primary key.
    - ``merge``: ``columns``, except the primary key.

    :param record_keys: The keys of the records (all records share them)
    :param columns: The ``columns`` argument of ``insert_or_merge``

    :raises ValueError: On an unknown strategy, or merge ``columns`` that are
        empty or name a column the records do not have.
    """
    pk_key = get_pk_column(table).key
    if strategy == "ignore":
        return []
    staged = [col.key for col in get_writable_columns(table)]
    if strategy == "replace":
        return [key for key in staged if key != pk_key]
    if strategy == "merge":
        if not columns:
            raise ValueError(
                "columns cannot be empty: use insert_or_replace to update every "
                "column, or insert_or_ignore to update none"
            )
        unknown = set(columns) - set(staged)
        if unknown:
            raise ValueError(
                f"columns {sorted(unknown)} are not writable columns of {table.name!r}"
            )
        record_keys = set(record_keys)
        missing = set(columns) - record_keys
        if record_keys and missing:
            raise ValueError(
                f"columns {sorted(missing)} are not keys of the records, "
                f"so there is no new value to merge"
            )
        return [key for key in staged if key in set(columns) and key != pk_key]
    raise ValueError(f"strategy must be one of {STRATEGIES}, got {strategy!r}")
