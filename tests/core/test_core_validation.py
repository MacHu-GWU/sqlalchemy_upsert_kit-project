# -*- coding: utf-8 -*-

from sqlalchemy_upsert_kit.core.validation import validate_records
from sqlalchemy_upsert_kit.core.validation import resolve_update_keys

import pytest

from sqlalchemy_upsert_kit.tests.contract import t_record
from sqlalchemy_upsert_kit.tests.contract import t_rich


def test_validate_records():
    validate_records(t_record, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])


@pytest.mark.parametrize(
    "values, message",
    [
        ([{"id": 1, "nope": 1}], "not writable"),
        ([{"name": "a"}], "must include the primary key"),
        ([{"id": 1, "name": "a"}, {"id": 2}], "same keys"),
        ([{"id": None}], "no value for the primary key"),
        ([{"id": 1}, {"id": 1}], "repeats the primary key"),
    ],
)
def test_validate_records_rejects(values, message):
    with pytest.raises(ValueError, match=message):
        validate_records(t_record, values)


@pytest.mark.parametrize(
    "strategy, columns, expected",
    [
        ("ignore", None, []),
        ("replace", None, ["sel", "status", "score", "label"]),
        ("merge", ["label", "sel"], ["sel", "label"]),  # table order
        ("merge", ["code", "sel"], ["sel"]),  # never the primary key
    ],
)
def test_resolve_update_keys(strategy, columns, expected):
    record_keys = ["code", "sel", "label"]
    assert resolve_update_keys(t_rich, strategy, record_keys, columns) == expected


@pytest.mark.parametrize(
    "strategy, columns, record_keys, message",
    [
        ("merge", None, ["id"], "cannot be empty"),
        ("merge", ["nope"], ["id"], "not writable"),
        ("merge", ["note"], ["id", "name"], "not keys of the records"),
        ("upsert", None, ["id"], "strategy must be one of"),
    ],
)
def test_resolve_update_keys_rejects(strategy, columns, record_keys, message):
    with pytest.raises(ValueError, match=message):
        resolve_update_keys(t_record, strategy, record_keys, columns)


def test_resolve_update_keys_without_records():
    """
    With no records, merge columns are only checked against the table.
    """
    assert resolve_update_keys(t_record, "merge", [], ["note"]) == ["note"]


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.core.validation",
        preview=False,
    )
