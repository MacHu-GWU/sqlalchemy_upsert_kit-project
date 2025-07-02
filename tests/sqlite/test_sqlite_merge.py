# -*- coding: utf-8 -*-

from sqlalchemy_upsert_kit.sqlite.merge import merge

from datetime import timezone

import pytest
from sqlalchemy_upsert_kit.exc import UpsertTestError
from sqlalchemy_upsert_kit.tests.utils import pt_from_many_dict
from sqlalchemy_upsert_kit.tests.data import (
    get_utc_now,
    t_record,
)


def test_success(
    clean_database,
    data_faker,
):
    """
    Test successful merge operation with selective column updates.

    This test verifies that merge operation:
    1. Updates only specified columns for existing records
    2. Preserves non-specified columns (like desc) for existing records
    3. Inserts new records with all specified data
    """
    engine = clean_database
    print("========== BEFORE ==========")
    rows = data_faker.get_all_records(engine)
    print("Existing records:")
    print(pt_from_many_dict(rows))
    print("Input records:")
    rows = data_faker.input_data
    print(pt_from_many_dict(rows))

    print("========== AFTER ==========")
    # Only update the "update_at" column, preserve "desc" column
    updated_rows, inserted_rows = merge(
        engine=engine,
        table=t_record,
        values=data_faker.input_data,
        columns=["update_at"],  # Only update update_at column
    )
    print(f"{updated_rows} rows updated, {inserted_rows} rows inserted")
    assert updated_rows == data_faker.n_conflict
    assert inserted_rows == data_faker.n_incremental

    data_faker.check_no_temp_tables(engine)

    rows = data_faker.get_all_records(engine)
    print(pt_from_many_dict(rows))
    data_faker.check_all_data(rows)
    print("  ✅Validation Passed.")

    # For merge, conflict records should have ORIGINAL desc (v1) but UPDATED update_at
    rows = data_faker.get_conflict_records(engine)
    print(pt_from_many_dict(rows))
    data_faker.check_conflict_data(rows)
    for row in rows:
        assert (
            row["desc"] == "v1"
        )  # Should preserve original value (not in columns list)
        assert (
            row["update_at"].replace(tzinfo=timezone.utc) == data_faker.update_time
        )  # Should be updated (in columns list)
    print("  ✅Merge-specific Validation Passed.")

    # Incremental records should have all new data
    rows = data_faker.get_incremental_records(engine)
    print(pt_from_many_dict(rows))
    data_faker.check_incremental_data(rows)
    print("  ✅Validation Passed.")


def test_rollback_with_auto_managed_transaction(
    clean_database,
    data_faker,
    error_scenarios,
):
    """
    Test comprehensive rollback behavior and data integrity in auto-managed mode.

    This test verifies that when errors occur at various points in auto-managed
    transaction mode, the original data remains completely intact.
    """
    engine = clean_database

    # Add merge specific error scenarios
    error_scenarios_with_merge = error_scenarios + [
        ("_raise_on_target_delete", "temp_delete_test"),
        ("_raise_on_merge_update", "temp_merge_update_test"),
    ]

    for flag_name, temp_table_name in error_scenarios_with_merge:
        print(f"Testing auto-managed rollback with {flag_name}...")
        kwargs = {flag_name: True}
        with pytest.raises(UpsertTestError):
            merge(
                engine=engine,
                table=t_record,
                values=data_faker.input_data,
                columns=["update_at"],
                temp_table_name=temp_table_name,
                **kwargs,
            )
        data_faker.check_no_temp_tables(engine)
        data_faker.check_rollback(engine)

    print("✅ All auto-managed rollback scenarios maintain data integrity")


def test_rollback_with_user_managed_transaction(
    clean_database,
    data_faker,
    error_scenarios,
):
    """
    Test user-managed transaction mode with various error scenarios.
    """
    engine = clean_database

    # Test each error scenario and verify complete rollback
    error_scenarios_with_merge_and_post = error_scenarios + [
        ("_raise_on_target_delete", "temp_delete_test"),
        ("_raise_on_merge_update", "temp_merge_update_test"),
        ("_raise_on_post_operation", "temp_post_test"),
    ]

    for flag_name, temp_table_name in error_scenarios_with_merge_and_post:
        print(f"Testing user-managed rollback with {flag_name}...")
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

                    merge(
                        engine=engine,
                        table=t_record,
                        values=data_faker.input_data,
                        columns=["update_at"],
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

        data_faker.check_no_temp_tables(engine)
        data_faker.check_rollback(engine)

    print("✅ All user-managed rollback scenarios maintain data integrity")


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.sqlite.merge",
        preview=False,
    )
