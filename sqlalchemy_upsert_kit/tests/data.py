# -*- coding: utf-8 -*-

"""
Test Data Models and Utilities

This module provides SQLAlchemy models, test data generation utilities, and helper
functions for testing upsert operations. It defines database schemas and creates
fake data scenarios that simulate real-world upsert conflicts and incremental data.

The module is designed to support comprehensive testing of database upsert operations
by providing controlled test datasets with predictable conflict patterns.
"""

import typing as T
import dataclasses
import random
from functools import cached_property
from datetime import datetime, timezone, timedelta

import sqlalchemy as sa
import sqlalchemy.orm as orm


class Base(orm.DeclarativeBase):
    """
    Base class for all SQLAlchemy models in the test suite.

    Provides common functionality for converting model instances to dictionary
    representations, which is useful for assertions and data comparison in tests.
    """

    def to_dict(self):
        """
        Convert a model instance to dictionary representation.

        Extracts all column values from the model instance and returns them
        as a dictionary with column names as keys.

        :returns: Dictionary mapping column names to their values
        """
        return dict(
            [(c.name, getattr(self, c.name, None)) for c in self.__table__.columns]
        )


class Record(Base):
    """
    Test database model representing a record with timestamps.

    This model is used throughout the test suite to simulate real-world database
    operations including upserts, conflicts, and incremental data updates.

    :param id: Primary key identifier for the record
    :param desc: Optional description field that changes between versions
    :param create_at: Timestamp when the record was initially created
    :param update_at: Timestamp when the record was last modified

    Example:
        Record used in upsert testing::

            {
                "id": 1,
                "desc": "v1",
                "create_at": "2024-01-01T10:00:00Z",
                "update_at": "2024-01-01T10:01:00Z"
            }
    """

    __tablename__ = "records"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    desc: orm.Mapped[T.Optional[str]] = orm.mapped_column()
    create_at: orm.Mapped[datetime] = orm.mapped_column(
        server_default=sa.func.now(),
        nullable=False,
    )
    update_at: orm.Mapped[datetime] = orm.mapped_column(
        server_default=sa.func.now(),
        nullable=False,
    )


t_record: sa.Table = Record.__table__


def get_utc_now() -> datetime:
    """
    Get the current UTC timestamp.

    :returns: Current datetime in UTC timezone
    """
    return datetime.now(timezone.utc)


@dataclasses.dataclass
class DataFaker:
    """
    Test data generator for upsert operation testing.

    Creates controlled test datasets with predictable patterns of existing data,
    new data, and conflicts between them. This enables comprehensive testing of
    upsert behaviors including conflict resolution and incremental data insertion.

    The data generator creates three categories of records:

    1. **Existing Records**: Already present in the database
    2. **Conflict Records**: Input records that conflict with existing ones
    3. **Incremental Records**: New records that don't conflict with existing data

    :param n_existing: Number of existing records to create in the database
    :param n_input: Total number of input records to generate for upsert
    :param n_conflict: Number of input records that will conflict with existing data

    **Examples**:
        Basic test scenario::

            {
                "n_existing": 10,
                "n_input": 5,
                "n_conflict": 2
            }

        This creates:
        - 10 existing records (IDs 1-10)
        - 5 input records total
        - 2 input records conflict with existing (IDs 9-10)
        - 3 input records are new/incremental (IDs 11-13)
        - Final total: 13 records after upsert

    .. note::

        The conflict records are generated from the end of the existing range
        to ensure predictable conflict patterns for testing.
    """

    n_existing: int = dataclasses.field()
    n_input: int = dataclasses.field()
    n_conflict: int = dataclasses.field()

    def __post_init__(self):  # pragma: no cover
        if self.n_existing <= 0:
            raise ValueError("n_existing must be greater than 0")
        if self.n_input <= 0:
            raise ValueError("n_input must be greater than 0")
        if self.n_conflict < 0:
            raise ValueError("n_conflict must be greater than or equal to 0")
        if self.n_conflict > self.n_input:
            raise ValueError("n_conflict cannot be greater than n_input")

    @cached_property
    def n_incremental(self) -> int:
        """
        Calculate the number of incremental (non-conflicting) input records.

        :returns: Count of input records that don't conflict with existing data
        """
        return self.n_input - self.n_conflict

    @cached_property
    def n_total(self) -> int:
        """
        Calculate the total number of records after upsert operation.

        This is the sum of existing records plus incremental records, since
        conflict records update existing ones rather than adding new ones.

        :returns: Total count of records that will exist after upsert
        """
        return self.n_existing + self.n_input - self.n_conflict

    @cached_property
    def create_time(self) -> datetime:
        """
        Base timestamp for existing record creation.

        :returns: UTC timestamp used for existing record timestamps
        """
        return get_utc_now()

    @cached_property
    def update_time(self) -> datetime:
        """
        Timestamp for record updates and new record creation.

        Set to 1 minute after create_time to ensure clear temporal separation
        between existing and updated/new records in tests.

        :returns: UTC timestamp used for updated and new record timestamps
        """
        return self.create_time + timedelta(minutes=1)

    @cached_property
    def existing_data(self) -> list[dict[str, T.Any]]:
        """
        Generate existing records for testing.
        """
        return [
            dict(
                id=i,
                desc=f"v1",
                create_at=self.create_time,
                update_at=self.create_time,
            )
            for i in range(1, 1 + self.n_existing)
        ]

    def prepare_existing_data(self, engine: sa.Engine):
        """
        Insert existing test data into the database.

        Creates the initial dataset that will be used as the baseline for
        testing upsert operations.

        :param engine: SQLAlchemy engine for database operations
        """
        with engine.connect() as conn:
            conn.execute(t_record.insert(), self.existing_data)
            conn.commit()

    @cached_property
    def conflict_range_lower(self) -> int:
        """
        Calculate the lower bound ID for conflict records.

        Conflict records are taken from the end of the existing range to
        ensure predictable conflict patterns.

        :returns: Lowest ID that will be included in conflict records
        """
        return self.n_existing - self.n_conflict + 1

    @cached_property
    def conflict_range_upper(self) -> int:
        """
        Calculate the upper bound ID for conflict records.

        :returns: Highest ID that will be included in conflict records
        """
        return self.conflict_range_lower + self.n_conflict - 1

    @cached_property
    def incremental_range_lower(self) -> int:
        """
        Calculate the lower bound ID for incremental records.

        Incremental records start immediately after the existing range
        to avoid any conflicts.

        :returns: Lowest ID that will be used for new incremental records
        """
        return self.n_existing + 1

    @cached_property
    def incremental_range_upper(self) -> int:
        """
        Calculate the upper bound ID for incremental records.

        :returns: Highest ID that will be used for new incremental records
        """
        return self.incremental_range_lower + self.n_incremental - 1

    @cached_property
    def input_data(self) -> list[dict[str, T.Any]]:
        """
        Generate input data for testing.

        This includes both new records and conflicts with existing records.
        """
        values = list()

        for i in range(self.conflict_range_lower, self.conflict_range_upper + 1):
            values.append(
                dict(
                    id=i,
                    desc=f"v2",
                    create_at=self.create_time,
                    update_at=self.update_time,
                )
            )

        for i in range(self.incremental_range_lower, self.incremental_range_upper + 1):
            values.append(
                dict(
                    id=i,
                    desc=f"v2",
                    create_at=self.update_time,
                    update_at=self.update_time,
                )
            )

        random.shuffle(values)  # Randomize order to simulate real-world data

        return values

    def get_all_records(self, engine: sa.Engine) -> T.Sequence[sa.RowMapping]:
        """
        Retrieve all records from the database after upsert operation.

        :param engine: SQLAlchemy engine for database operations

        :returns: All records ordered by ID for consistent test assertions
        """
        with engine.connect() as conn:
            stmt = sa.select(Record).order_by(Record.id)
            rows = conn.execute(stmt).mappings().fetchall()
            return rows

    def get_conflict_records(
        self,
        engine: sa.Engine,
        limit: int = 5,
    ) -> T.Sequence[sa.RowMapping]:
        """
        Retrieve records that were involved in conflict resolution.

        Returns records whose IDs fall within the conflict range, which should
        have been updated during the upsert operation.

        :param engine: SQLAlchemy engine for database operations
        :param limit: Maximum number of conflict records to return

        :returns: Records that were updated due to conflicts during upsert
        """
        with engine.connect() as conn:
            stmt = (
                sa.select(Record)
                .where(
                    Record.id.between(
                        self.conflict_range_lower,
                        self.conflict_range_upper,
                    )
                )
                .order_by(Record.id)
                .limit(limit)
            )
            rows = conn.execute(stmt).mappings().all()
            return rows

    def get_incremental_records(
        self,
        engine: sa.Engine,
        limit: int = 5,
    ) -> T.Sequence[sa.RowMapping]:
        """
        Retrieve records that were inserted as new data.

        Returns records whose IDs fall within the incremental range, which should
        have been inserted during the upsert operation without conflicts.

        :param engine: SQLAlchemy engine for database operations
        :param limit: Maximum number of incremental records to return

        :returns: Records that were inserted as new data during upsert
        """
        with engine.connect() as conn:
            stmt = (
                sa.select(Record)
                .where(
                    Record.id.between(
                        self.incremental_range_lower,
                        self.incremental_range_upper,
                    )
                )
                .order_by(Record.id)
                .limit(limit)
            )
            rows = conn.execute(stmt).mappings().all()
            return rows

    def check_all_data(self, rows: T.Sequence[sa.RowMapping]):
        """
        Validate that the total number of records matches expectations.

        Asserts that the result set contains exactly the expected number of
        records after the upsert operation.

        Do this check only in happy path, not sad path.

        :param rows: Query result set to validate

        :raises AssertionError: If record count doesn't match expected total
        """
        print("--- Check all records:")
        assert len(rows) == self.n_total

    def check_conflict_data(self, rows: T.Sequence[sa.RowMapping]):
        """
        Validate that conflict records have expected properties.

        Verifies that conflict records:

        - Have IDs within the expected conflict range
        - Retain their original creation timestamp (not updated)

        Do this check only in happy path, not sad path.

        :param rows: Conflict records to validate

        :raises AssertionError: If any conflict record has unexpected properties
        """
        lower, upper = self.conflict_range_lower, self.conflict_range_upper
        print(f"--- Check records in conflict range ({lower} to {upper}):")
        assert len(rows) == self.n_conflict
        for row in rows:
            assert self.conflict_range_lower <= row["id"] <= self.conflict_range_upper
            assert row["create_at"].replace(tzinfo=timezone.utc) == self.create_time

    def check_incremental_data(self, rows: T.Sequence[sa.RowMapping]):
        """
        Validate that incremental records have expected properties.

        Verifies that incremental records:

        - Have IDs within the expected incremental range
        - Have the updated description value ("v2")
        - Have creation and update timestamps set to update_time

        Do this check only in happy path, not sad path.

        :param rows: Incremental records to validate

        :raises AssertionError: If any incremental record has unexpected properties
        """
        lower, upper = self.incremental_range_lower, self.incremental_range_upper
        print(f"--- Check records in incremental range ({lower} to {upper}):")
        assert len(rows) == self.n_incremental
        for row in rows:
            assert (
                self.incremental_range_lower
                <= row["id"]
                <= self.incremental_range_upper
            )
            assert row["desc"] == "v2"
            assert row["create_at"].replace(tzinfo=timezone.utc) == self.update_time
            assert row["update_at"].replace(tzinfo=timezone.utc) == self.update_time

    def check_no_temp_tables(self, engine: sa.Engine) -> list[str]:
        """
        Helper function to verify no temporary tables exist.

        Do this check in both happy path and sad path.
        """
        print("--- Check if temp tables were cleaned up:")
        metadata = sa.MetaData()
        metadata.reflect(bind=engine)
        temp_tables = [
            table.name
            for table in metadata.sorted_tables
            if table.name.startswith("temp")
        ]
        assert not temp_tables, f"Temporary tables not cleaned up: {temp_tables}"
        return temp_tables

    def check_rollback(self, engine: sa.Engine) -> T.Sequence[sa.RowMapping]:
        """
        Verify that the database state is unchanged after a rollback.

        Do this check only in sad path, not happy path.
        """
        print("--- Check rollback state:")
        final_rows = self.get_all_records(engine)
        msg = "Wrong number of records after rollback"
        assert len(final_rows) == self.n_existing, msg
        return final_rows
