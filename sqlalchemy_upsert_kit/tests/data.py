# -*- coding: utf-8 -*-

import typing as T
import dataclasses
import random
from functools import cached_property
from datetime import datetime, timezone, timedelta

import sqlalchemy as sa
import sqlalchemy.orm as orm


class Base(orm.DeclarativeBase):
    def to_dict(self):
        """
        Convert a model instance to dictionary representation.
        """
        return dict(
            [(c.name, getattr(self, c.name, None)) for c in self.__table__.columns]
        )


class Record(Base):
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


def prepare_existing_records(engine: sa.Engine):
    """
    Prepare existing records for testing.
    """
    with engine.connect() as conn:
        conn.execute(Record.__table__.insert(), existing_records)
        conn.commit()


def get_utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclasses.dataclass
class DataFaker:
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
        return self.n_input - self.n_conflict

    @cached_property
    def n_total(self) -> int:
        return self.n_existing + self.n_input - self.n_conflict

    @cached_property
    def create_time(self) -> datetime:
        return get_utc_now()

    @cached_property
    def update_time(self) -> datetime:
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
        with engine.connect() as conn:
            conn.execute(t_record.insert(), self.existing_data)
            conn.commit()

    @cached_property
    def conflict_range_lower(self) -> int:
        return self.n_existing - self.n_conflict + 1

    @cached_property
    def conflict_range_upper(self) -> int:
        return self.conflict_range_lower + self.n_conflict - 1

    @cached_property
    def incremental_range_lower(self) -> int:
        return self.n_existing + 1

    @cached_property
    def incremental_range_upper(self) -> int:
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
        Get all records including existing and input data.
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
        Get records that are in conflict with existing data.
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
        Get records that are incremental and not in conflict with existing data.
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
        Check that all rows.
        """
        assert len(rows) == self.n_total

    def check_conflict_data(self, rows: T.Sequence[sa.RowMapping]):
        """
        Check that the rows are in the conflict range.
        """
        for row in rows:
            assert self.conflict_range_lower <= row["id"] <= self.conflict_range_upper
            assert row["create_at"].replace(tzinfo=timezone.utc) == self.create_time

    def check_incremental_data(self, rows: T.Sequence[sa.RowMapping]):
        """
        Check that the rows are in the incremental range.
        """
        for row in rows:
            assert (
                self.incremental_range_lower
                <= row["id"]
                <= self.incremental_range_upper
            )
            assert row["desc"] == "v2"
            assert row["create_at"].replace(tzinfo=timezone.utc) == self.update_time
            assert row["update_at"].replace(tzinfo=timezone.utc) == self.update_time
