# -*- coding: utf-8 -*-

from datetime import timezone

from sqlalchemy_upsert_kit.sqlite import (
    insert_or_replace,
    insert_or_ignore,
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


@pytest.fixture(scope="module")
def engine() -> sa.Engine:
    dir_tmp.mkdir(parents=True, exist_ok=True)
    path_sqlite.unlink(missing_ok=True)
    engine = sa.create_engine(f"sqlite:///{path_sqlite}")
    Base.metadata.create_all(engine)
    return engine


def test_insert_or_ignore(engine):
    data_faker = DataFaker(
        n_existing=4,
        n_input=3,
        n_conflict=2,
    )
    data_faker.prepare_existing_data(engine)

    print("")
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

    print("Check if temp tables were cleaned up:")
    metadata = sa.MetaData()
    metadata.reflect(bind=engine)
    for table in metadata.sorted_tables:
        if table.name.startswith("temp"):
            raise RuntimeError(
                f"Temporary table {table.name} was not cleaned up properly."
            )
    print("  ✅Temporary tables cleaned up successfully.")

    with engine.connect() as conn:
        print("--- Check all records:")
        rows = data_faker.get_all_records(engine)
        print(pt.from_dict_list(rows))
        data_faker.check_all_data(rows)
        print("  ✅Validation Passed.")

        lower = data_faker.conflict_range_lower
        upper = data_faker.conflict_range_upper
        print(f"--- Check records in conflict range ({lower} to {upper}):")
        rows = data_faker.get_conflict_records(engine)
        print(pt.from_dict_list(rows))
        data_faker.check_conflict_data(rows)
        for row in rows:
            assert row["desc"] == "v1"
            assert (
                row["update_at"].replace(tzinfo=timezone.utc) == data_faker.create_time
            )
        print("  ✅Validation Passed.")

        lower = data_faker.incremental_range_lower
        upper = data_faker.incremental_range_upper
        print(f"--- Check records in incremental range ({lower} to {upper}):")
        rows = data_faker.get_incremental_records(engine)
        print(pt.from_dict_list(rows))
        data_faker.check_incremental_data(rows)
        print("  ✅Validation Passed.")


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.sqlite",
        preview=False,
    )
