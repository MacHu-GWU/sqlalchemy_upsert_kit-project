# -*- coding: utf-8 -*-

from datetime import timezone

from sqlalchemy_upsert_kit.sqlite import (
    insert_or_replace,
    insert_or_ignore,
    UpsertTestError,
)

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.pt as pt
from sqlalchemy_upsert_kit.paths import dir_project_root
from sqlalchemy_upsert_kit.tests.data import (
    Base,
    Record,
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


def test_insert_or_ignore_error_on_temp_table_create(
    clean_database,
    data_faker,
):
    """
    Test error handling when temp table creation fails.

    Verifies that:

    - UpsertTestError is raised
    - Database state is unchanged (rollback worked)
    - No temp tables are left behind
    """
    engine = clean_database

    # Test error scenario
    with pytest.raises(UpsertTestError):
        insert_or_ignore(
            engine=engine,
            table=t_record,
            values=data_faker.input_data,
            temp_table_name="test_temp_create",
            _raise_on_temp_table_create=True,
        )

    # Verify no temp tables left behind
    data_faker.check_no_temp_tables(engine)
    print("  ✅Validation Passed.")

    # Verify database state is unchanged
    data_faker.check_rollback(engine)
    print("  ✅Validation Passed.")


def test_insert_or_ignore_error_on_temp_data_insert(
    clean_database,
    data_faker,
):
    """
    Test error handling when temp data insertion fails.

    Verifies that:

    - UpsertTestError is raised
    - Database state is unchanged (rollback worked)
    - Temp table is properly cleaned up
    """
    engine = clean_database

    # Test error scenario
    with pytest.raises(UpsertTestError):
        insert_or_ignore(
            engine=engine,
            table=t_record,
            values=data_faker.input_data,
            temp_table_name="test_temp_data",
            _raise_on_temp_data_insert=True,
        )

    # Verify no temp tables left behind
    data_faker.check_no_temp_tables(engine)
    print("  ✅Validation Passed.")

    # Verify database state is unchanged
    data_faker.check_rollback(engine)
    print("  ✅Validation Passed.")


def test_insert_or_ignore_error_on_target_insert(
    clean_database,
    data_faker,
):
    """
    Test error handling when target insertion fails.

    Verifies that:

    - UpsertTestError is raised
    - Database state is unchanged (rollback worked)
    - Temp table is properly cleaned up
    """
    engine = clean_database

    # Test error scenario
    with pytest.raises(UpsertTestError):
        insert_or_ignore(
            engine=engine,
            table=t_record,
            values=data_faker.input_data,
            temp_table_name="test_temp_target",
            _raise_on_target_insert=True,
        )

    # Verify no temp tables left behind
    data_faker.check_no_temp_tables(engine)
    print("  ✅Validation Passed.")

    # Verify database state is unchanged
    data_faker.check_rollback(engine)
    print("  ✅Validation Passed.")


def test_insert_or_ignore_error_on_temp_table_drop(
    clean_database,
    data_faker,
):
    """
    Test error handling when temp table cleanup fails.

    Verifies that:

    - UpsertTestError is raised
    - Database changes are rolled back
    - Original data integrity is maintained
    """
    engine = clean_database

    # Test error scenario
    with pytest.raises(UpsertTestError):
        insert_or_ignore(
            engine=engine,
            table=t_record,
            values=data_faker.input_data,
            temp_table_name="test_temp_cleanup",
            _raise_on_temp_table_drop=True,
        )

    # Verify no temp tables left behind
    data_faker.check_no_temp_tables(engine)
    print("  ✅Validation Passed.")

    # Verify database state is unchanged
    data_faker.check_rollback(engine)
    print("  ✅Validation Passed.")


def test_insert_or_ignore_empty_values(
    clean_database,
    data_faker,
):
    """
    Test insert_or_ignore with empty values list.

    Verifies that:

    - Function returns (0, 0) for empty input
    - No database changes occur
    - No temp tables are created
    """
    engine = clean_database

    # Test with empty values
    ignored_rows, inserted_rows = insert_or_ignore(
        engine=engine,
        table=t_record,
        values=[],
    )

    assert ignored_rows == 0, "Should ignore 0 rows for empty input"
    assert inserted_rows == 0, "Should insert 0 rows for empty input"

    data_faker.check_no_temp_tables(engine)
    data_faker.check_rollback(engine)


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


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.sqlite",
        preview=False,
    )
