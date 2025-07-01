# -*- coding: utf-8 -*-

from datetime import timezone

from sqlalchemy_upsert_kit.sqlite import (
    UpsertTestError,
    insert_or_replace,
    insert_or_ignore,
)

import pytest
import sqlalchemy as sa
import sqlalchemy_mate.pt as pt
from sqlalchemy_upsert_kit.paths import dir_project_root
from sqlalchemy_upsert_kit.tests.data import (
    get_utc_now,
    Base,
    t_record,
    DataFaker,
)

dir_tmp = dir_project_root / "tmp"
path_sqlite = dir_tmp / "test.sqlite"


# def pytest_runtest_setup(item):
#     """
#     Hook that runs before each test function.
#     Automatically prints the test function name.
#     """
#     print(f"\n========== {item.name} ==========")


@pytest.fixture
def data_faker():
    """
    Fixture providing a standard DataFaker configuration for tests.
    """
    return DataFaker(
        n_existing=4,
        n_input=3,
        n_conflict=2,
    )


@pytest.fixture
def clean_database(data_faker):
    """
    Fixture to ensure clean database state for each test.
    """
    dir_tmp.mkdir(parents=True, exist_ok=True)
    path_sqlite.unlink(missing_ok=True)
    engine = sa.create_engine(f"sqlite:///{path_sqlite}")
    Base.metadata.create_all(engine)
    data_faker.prepare_existing_data(engine)
    yield engine

    pass


def test_insert_or_ignore_success(
    clean_database,
    data_faker,
):
    """
    Test successful insert_or_ignore operation.
    """
    engine = clean_database
    print("========== BEFORE ==========")
    rows = data_faker.get_all_records(engine)
    print("Existing records:")
    print(pt.from_dict_list(rows))
    print("Input records:")
    rows = data_faker.input_data
    print(pt.from_dict_list(rows))

    print("========== AFTER ==========")
    ignored_rows, inserted_rows = insert_or_ignore(
        engine=engine,
        table=t_record,
        values=data_faker.input_data,
    )
    print(f"{ignored_rows} rows ignored, {inserted_rows} rows inserted")
    assert ignored_rows == data_faker.n_conflict
    assert inserted_rows == data_faker.n_incremental

    data_faker.check_no_temp_tables(engine)

    rows = data_faker.get_all_records(engine)
    print(pt.from_dict_list(rows))
    data_faker.check_all_data(rows)
    print("  ✅Validation Passed.")

    rows = data_faker.get_conflict_records(engine)
    print(pt.from_dict_list(rows))
    data_faker.check_conflict_data(rows)
    for row in rows:
        assert row["desc"] == "v1"
        assert row["update_at"].replace(tzinfo=timezone.utc) == data_faker.create_time
    print("  ✅Validation Passed.")

    rows = data_faker.get_incremental_records(engine)
    print(pt.from_dict_list(rows))
    data_faker.check_incremental_data(rows)
    print("  ✅Validation Passed.")


def test_insert_or_ignore_rollback_data_integrity(
    clean_database,
    data_faker,
):
    """
    Test comprehensive rollback behavior and data integrity.

    This test verifies that when errors occur at various points,
    the original data remains completely intact.
    """
    engine = clean_database

    # Test each error scenario and verify complete rollback
    error_scenarios = [
        ("_raise_on_temp_table_create", "temp_create_test"),
        ("_raise_on_temp_data_insert", "temp_data_test"),
        ("_raise_on_target_insert", "temp_target_test"),
        ("_raise_on_temp_table_drop", "temp_drop_test"),
    ]

    for flag_name, temp_table_name in error_scenarios:
        print(f"Testing rollback with {flag_name}...")

        kwargs = {
            flag_name: True,
        }

        with pytest.raises(UpsertTestError):
            insert_or_ignore(
                engine=engine,
                table=t_record,
                values=data_faker.input_data,
                temp_table_name=temp_table_name,
                **kwargs,
            )

        data_faker.check_no_temp_tables(engine)
        data_faker.check_rollback(engine)

    print("✅ All rollback scenarios maintain data integrity")


def test_long_transaction(
    clean_database,
    data_faker,
):
    """
    Test if
    """
    engine = clean_database

    # Test each error scenario and verify complete rollback
    error_scenarios = [
        ("_raise_on_temp_table_create", "temp_create_test"),
        ("_raise_on_temp_data_insert", "temp_data_test"),
        ("_raise_on_target_insert", "temp_target_test"),
        ("_raise_on_temp_table_drop", "temp_drop_test"),
        ("_raise_on_post_operation", "temp_post_test"),
    ]

    for flag_name, temp_table_name in error_scenarios:
        print(f"Testing rollback with {flag_name}...")
        if flag_name == "_raise_on_post_operation":
            kwargs = {}
        else:
            kwargs = {flag_name: True}

        with pytest.raises(UpsertTestError):
            with engine.connect() as conn:
                with conn.begin() as trans:
                    # Insert initial records with proper timestamps
                    now = get_utc_now()
                    values = [
                        {"id": 6, "desc": "v1", "create_at": now, "update_at": now},
                        {"id": 7, "desc": "v1", "create_at": now, "update_at": now},
                    ]
                    conn.execute(t_record.insert(), values)

                    insert_or_ignore(
                        engine=engine,
                        table=t_record,
                        values=data_faker.input_data,
                        conn=conn,
                        trans=trans,
                        temp_table_name=temp_table_name,
                        **kwargs,
                    )

                    # Insert more records after upsert
                    values = [
                        {"id": 8, "desc": "v1", "create_at": now, "update_at": now},
                        {"id": 9, "desc": "v1", "create_at": now, "update_at": now},
                    ]
                    conn.execute(t_record.insert(), values)

                    if flag_name == "_raise_on_post_operation":
                        raise UpsertTestError("Simulated error in post-operation")

                    # Note: trans.commit() should not be called in user-managed mode
                    # The transaction should be managed externally

        data_faker.check_no_temp_tables(engine)
        data_faker.check_rollback(engine)


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.sqlite",
        preview=False,
    )
