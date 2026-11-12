# -*- coding: utf-8 -*-

"""
The behavior contract every backend's upsert strategies must keep, as one
test suite that each backend runs.

A backend's ``tests/<engine>/test_<engine>_insert_or_<strategy>.py`` subclasses
:class:`StrategyContract`, setting ``strategy`` and ``func``; pytest collects the
inherited tests there. They need an ``engine`` fixture (a fresh database with
:func:`reset` applied, a pool of one connection so a test can inspect the very
connection an auto-managed call used) and a ``db_url`` fixture (for engines of
its own, in the concurrency test).

Every expected result is a literal, so a test fails if any backend's result differs.
"""

import typing as T
import threading

import pytest
import sqlalchemy as sa

from ..exc import UpsertTestError
from ..core.executor import FAIL_STEPS

metadata = sa.MetaData()

# fmt: off
t_record = sa.Table(
    "records",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
    sa.Column("name", sa.String(50)),
    sa.Column("note", sa.String(50)),
)

# Everything that trips a naive staging table clone: a mixed-case table name,
# a string key whose column name differs from its key, a reserved-word column,
# server and Python defaults, a generated column, a unique constraint, a named index.
t_rich = sa.Table(
    "RichItems",
    metadata,
    sa.Column("Code", sa.String(20), key="code", primary_key=True),
    sa.Column("select", sa.String(20), key="sel"),
    sa.Column("status", sa.String(20), server_default="new"),
    sa.Column("score", sa.Integer, default=7),
    sa.Column("total", sa.Integer, sa.Computed("score * 2")),
    sa.Column("label", sa.String(20), unique=True),
)
sa.Index("ix_rich_items_select", t_rich.c.sel)

t_parent = sa.Table(
    "parents",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
    sa.Column("name", sa.String(50)),
)

t_child = sa.Table(
    "children",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
    sa.Column("parent_id", sa.Integer, sa.ForeignKey("parents.id", ondelete="CASCADE")),
    sa.Column("name", sa.String(50)),
)

# A composite unique key, and a unique index (not a constraint) on another column.
t_pair = sa.Table(
    "pairs",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
    sa.Column("a", sa.Integer),
    sa.Column("b", sa.Integer),
    sa.Column("tag", sa.String(20)),
    sa.UniqueConstraint("a", "b"),
)
sa.Index("ux_pairs_tag", t_pair.c.tag, unique=True)

t_pk_only = sa.Table(
    "pk_only",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
)

# 61 characters: a staging name built from it must be cut to fit PostgreSQL's 63
# and MySQL's 64.
t_long = sa.Table(
    "t" + "_long_name" * 6,
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
    sa.Column("name", sa.String(50)),
)
# fmt: on

SEEDS = {
    t_record: [
        {"id": 1, "name": "a", "note": "n1"},
        {"id": 2, "name": "b", "note": "n2"},
        {"id": 3, "name": "c", "note": "n3"},
    ],
    t_rich: [
        {"code": "A", "sel": "s1", "status": "old", "score": 1, "label": "la"},
        {"code": "B", "sel": "s2", "status": "old", "score": 2, "label": "lb"},
    ],
    t_parent: [
        {"id": 1, "name": "p1"},
        {"id": 2, "name": "p2"},
    ],
    t_child: [
        {"id": 10, "parent_id": 1, "name": "c10"},
        {"id": 11, "parent_id": 2, "name": "c11"},
    ],
    t_pair: [
        {"id": 1, "a": 1, "b": 1, "tag": "t1"},
    ],
    t_pk_only: [
        {"id": 1},
    ],
    t_long: [
        {"id": 1, "name": "a"},
    ],
}

#: The ``columns`` argument of merge, per table.
MERGE_COLUMNS = {
    t_record.name: ["name"],
    t_rich.name: ["sel"],
    t_parent.name: ["name"],
    t_child.name: ["name"],
    t_pair.name: ["b"],
    t_pk_only.name: ["id"],
    t_long.name: ["name"],
}

# The main scenario: ids 2 and 3 conflict, id 4 is new; records in no particular order.
INPUT = [
    {"id": 3, "name": "C", "note": "N3"},
    {"id": 4, "name": "d", "note": "n4"},
    {"id": 2, "name": "B", "note": "N2"},
]
EXISTING_ROWS = [(1, "a", "n1"), (2, "b", "n2"), (3, "c", "n3")]
EXPECTED_ROWS = {
    "ignore": [(1, "a", "n1"), (2, "b", "n2"), (3, "c", "n3"), (4, "d", "n4")],
    "replace": [(1, "a", "n1"), (2, "B", "N2"), (3, "C", "N3"), (4, "d", "n4")],
    "merge": [(1, "a", "n1"), (2, "B", "n2"), (3, "C", "n3"), (4, "d", "n4")],
}

#: Row count of each side of the large scenario: N existing rows, N records, N/2 conflicting.
LARGE_N = 10_000


def reset(engine: sa.Engine) -> None:
    """
    Recreate every contract table and load the seed rows.
    """
    metadata.drop_all(engine)
    metadata.create_all(engine)
    with engine.begin() as conn:
        for table, rows in SEEDS.items():
            conn.execute(table.insert(), rows)


def read_rows(
    bind: sa.Engine | sa.Connection,
    table: sa.Table = t_record,
) -> list[tuple]:
    """
    All rows of ``table``, ordered by primary key, as plain tuples.
    """
    stmt = sa.select(table).order_by(*table.primary_key.columns)
    if isinstance(bind, sa.Engine):
        with bind.connect() as conn:
            return [tuple(row) for row in conn.execute(stmt)]
    return [tuple(row) for row in bind.execute(stmt)]


def staging_tables(conn: sa.Connection) -> list[str]:
    """
    Staging tables visible on ``conn``: its own temporary tables, plus any
    permanent table whose name starts with ``temp_``.

    MySQL cannot list one connection's temporary tables: its branch lists every
    connection's, under internal names (``#sql...``). That is the same as long
    as no other connection to the test database holds one, which is true while
    the tests run one at a time.
    """
    if conn.dialect.name == "mysql":
        sql = (
            "SELECT NAME FROM information_schema.INNODB_TEMP_TABLE_INFO "
            "UNION ALL SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'temp\\_%'"
        )
    elif conn.dialect.name == "postgresql":
        sql = (
            "SELECT relname FROM pg_class WHERE relkind = 'r' AND ("
            "relnamespace = pg_my_temp_schema() OR relname LIKE 'temp\\_%')"
        )
    else:
        sql = (
            "SELECT name FROM sqlite_temp_master WHERE type = 'table' "
            "UNION ALL SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name LIKE 'temp\\_%' ESCAPE '\\'"
        )
    return list(conn.execute(sa.text(sql)).scalars())


def assert_no_staging(engine: sa.Engine) -> None:
    """
    No staging table is left on the engine's (single) pooled connection.
    """
    with engine.connect() as conn:
        assert staging_tables(conn) == []


class StrategyContract:
    """
    The tests every strategy of every backend must pass.
    """

    strategy: str  # "ignore", "replace" or "merge"
    func: T.Callable[..., tuple[int, int]]  # set with staticmethod(...)

    def call(
        self,
        engine: sa.Engine,
        table: sa.Table = t_record,
        values: list[dict[str, T.Any]] | None = None,
        **kwargs,
    ) -> tuple[int, int]:
        if self.strategy == "merge":
            kwargs.setdefault("columns", MERGE_COLUMNS[table.name])
        return self.func(
            engine=engine,
            table=table,
            values=INPUT if values is None else values,
            **kwargs,
        )

    # --- the main scenario -----------------------------------------------------
    @pytest.mark.parametrize("mode", ["auto", "user"])
    def test_happy_path(self, engine, mode):
        if mode == "auto":
            counts = self.call(engine)
        else:
            with engine.connect() as conn:
                with conn.begin() as trans:
                    counts = self.call(engine, conn=conn, trans=trans)
        assert counts == (2, 1)
        assert read_rows(engine) == EXPECTED_ROWS[self.strategy]
        assert_no_staging(engine)

    def test_empty_values(self, engine):
        assert self.call(engine, values=[]) == (0, 0)
        assert read_rows(engine) == EXISTING_ROWS

    def test_call_twice_in_one_transaction(self, engine):
        with engine.connect() as conn:
            with conn.begin() as trans:
                assert self.call(engine, conn=conn, trans=trans) == (2, 1)
                assert self.call(engine, conn=conn, trans=trans) == (3, 0)
        assert read_rows(engine) == EXPECTED_ROWS[self.strategy]

    @pytest.mark.parametrize("prior_write", [False, True])
    def test_caller_rollback_undoes_the_upsert(self, engine, prior_write):
        """
        In user-managed mode the caller decides: a rollback after a successful
        call undoes it, even when the caller wrote nothing before the call.
        """
        with engine.connect() as conn:
            trans = conn.begin()
            if prior_write:
                conn.execute(t_record.insert(), [{"id": 100, "name": "x"}])
            assert self.call(engine, conn=conn, trans=trans) == (2, 1)
            trans.rollback()
        assert read_rows(engine) == EXISTING_ROWS

    # --- failures ----------------------------------------------------------------
    @pytest.mark.parametrize("fail_at", FAIL_STEPS)
    def test_failure_in_auto_mode(self, engine, fail_at):
        with pytest.raises(UpsertTestError):
            self.call(engine, _fail_at=fail_at)
        assert read_rows(engine) == EXISTING_ROWS
        assert_no_staging(engine)

    @pytest.mark.parametrize("prior_write", [False, True])
    @pytest.mark.parametrize("fail_at", FAIL_STEPS)
    def test_failure_in_user_mode(self, engine, fail_at, prior_write):
        """
        A failure undoes only the call: the caller's transaction keeps its own
        writes, has no staging table, and can retry and commit.
        """
        caller_rows = [(100, "x", None)] if prior_write else []
        with engine.connect() as conn:
            with conn.begin() as trans:
                if prior_write:
                    conn.execute(t_record.insert(), [{"id": 100, "name": "x"}])
                with pytest.raises(UpsertTestError):
                    self.call(engine, conn=conn, trans=trans, _fail_at=fail_at)
                assert staging_tables(conn) == []
                assert read_rows(conn) == EXISTING_ROWS + caller_rows
                assert self.call(engine, conn=conn, trans=trans) == (2, 1)
        assert read_rows(engine) == EXPECTED_ROWS[self.strategy] + caller_rows
        assert_no_staging(engine)

    # --- invalid input -----------------------------------------------------------
    @pytest.mark.parametrize(
        "values",
        [
            pytest.param([{"id": 5, "name": "x", "nope": 1}], id="unknown_key"),
            pytest.param([{"name": "x", "note": "y"}], id="missing_pk"),
            pytest.param([{"id": None, "name": "x", "note": "y"}], id="none_pk"),
            pytest.param(
                [{"id": 5, "name": "x", "note": "y"}, {"id": 6, "name": "z"}],
                id="mixed_keys",
            ),
            pytest.param(
                [
                    {"id": 5, "name": "x", "note": "y"},
                    {"id": 5, "name": "z", "note": "w"},
                ],
                id="duplicate_pk",
            ),
        ],
    )
    def test_invalid_records(self, engine, values):
        with pytest.raises(ValueError):
            self.call(engine, values=values)
        assert read_rows(engine) == EXISTING_ROWS

    def test_generated_column_is_not_writable(self, engine):
        with pytest.raises(ValueError):
            self.call(engine, table=t_rich, values=[{"code": "Z", "total": 1}])

    def test_conn_without_trans(self, engine):
        with engine.connect() as conn:
            with pytest.raises(ValueError):
                self.call(engine, values=[], conn=conn)

    def test_temp_table_name_equal_to_target(self, engine):
        with pytest.raises(ValueError):
            self.call(engine, temp_table_name=t_record.name)

    # --- arguments ---------------------------------------------------------------
    def test_explicit_temp_table_name(self, engine):
        assert self.call(engine, temp_table_name="my_staging") == (2, 1)
        assert_no_staging(engine)

    def test_target_metadata_as_metadata(self, engine):
        """
        Passing the target's own MetaData works, and the staging table
        definition is removed from it afterwards.
        """
        tables_before = set(metadata.tables)
        assert self.call(engine, metadata=metadata) == (2, 1)
        assert set(metadata.tables) == tables_before

    # --- schemas -----------------------------------------------------------------
    def test_rich_table(self, engine):
        """
        Key differs from name, reserved word, mixed case, defaults, generated
        column, unique constraint and named index. Records omit ``status`` and
        ``score``, so new rows (and replaced rows) get their defaults.
        """
        values = [
            {"code": "B", "sel": "S2", "label": "LB"},
            {"code": "C", "sel": "s3", "label": "lc"},
        ]
        assert self.call(engine, table=t_rich, values=values) == (1, 1)
        row_a = ("A", "s1", "old", 1, 2, "la")
        row_c = ("C", "s3", "new", 7, 14, "lc")
        row_b = {
            "ignore": ("B", "s2", "old", 2, 4, "lb"),
            "replace": ("B", "S2", "new", 7, 14, "LB"),
            "merge": ("B", "S2", "old", 2, 4, "lb"),
        }[self.strategy]
        assert read_rows(engine, t_rich) == [row_a, row_b, row_c]
        assert_no_staging(engine)

    # --- unique keys other than the primary key -----------------------------------
    @pytest.mark.parametrize("mode", ["auto", "user"])
    def test_unique_key_taken_by_existing_row(self, engine, mode):
        """
        A new record whose ``label`` (a unique key) an existing row already has
        is an error, never an update of that row, and the call changes nothing.
        In user mode the caller's transaction stays usable.
        """
        values = [
            {"code": "C", "sel": "s3", "label": "lc"},
            {"code": "Z", "sel": "sz", "label": "la"},
        ]
        rows_before = read_rows(engine, t_rich)
        if mode == "auto":
            with pytest.raises(sa.exc.IntegrityError):
                self.call(engine, table=t_rich, values=values)
        else:
            with engine.connect() as conn:
                with conn.begin() as trans:
                    with pytest.raises(sa.exc.IntegrityError):
                        self.call(
                            engine, table=t_rich, values=values, conn=conn, trans=trans
                        )
                    assert staging_tables(conn) == []
                    assert read_rows(conn, t_rich) == rows_before
                    assert self.call(engine, conn=conn, trans=trans) == (2, 1)
            assert read_rows(engine) == EXPECTED_ROWS[self.strategy]
        assert read_rows(engine, t_rich) == rows_before
        assert_no_staging(engine)

    def test_unique_key_shared_by_records(self, engine):
        """
        Two new records with the same ``label`` are an error.
        """
        values = [
            {"code": "Y", "sel": "sy", "label": "lz"},
            {"code": "Z", "sel": "sz", "label": "lz"},
        ]
        rows_before = read_rows(engine, t_rich)
        with pytest.raises(sa.exc.IntegrityError):
            self.call(engine, table=t_rich, values=values)
        assert read_rows(engine, t_rich) == rows_before
        assert_no_staging(engine)

    def test_unique_key_of_conflicting_record(self, engine):
        """
        A conflicting record whose ``label`` another row has is an error only if
        the strategy writes ``label`` (replace); ignore leaves the row alone,
        and merge updates only ``sel``.
        """
        values = [{"code": "B", "sel": "S2", "label": "la"}]
        rows_before = read_rows(engine, t_rich)
        if self.strategy == "replace":
            with pytest.raises(sa.exc.IntegrityError):
                self.call(engine, table=t_rich, values=values)
            assert read_rows(engine, t_rich) == rows_before
        else:
            assert self.call(engine, table=t_rich, values=values) == (1, 0)
            row_b = {
                "ignore": ("B", "s2", "old", 2, 4, "lb"),
                "merge": ("B", "S2", "old", 2, 4, "lb"),
            }[self.strategy]
            assert read_rows(engine, t_rich) == [rows_before[0], row_b]
        assert_no_staging(engine)

    def test_unique_key_shared_with_conflicting_record(self, engine):
        """
        A conflicting record and a new record with the same ``label``: an error
        only if the strategy writes ``label`` on the conflicting row (replace).
        Otherwise the conflicting row keeps its own ``label``, and the new
        record is inserted with it.
        """
        values = [
            {"code": "B", "sel": "S2", "label": "lz"},
            {"code": "Z", "sel": "sz", "label": "lz"},
        ]
        rows_before = read_rows(engine, t_rich)
        if self.strategy == "replace":
            with pytest.raises(sa.exc.IntegrityError):
                self.call(engine, table=t_rich, values=values)
            assert read_rows(engine, t_rich) == rows_before
        else:
            assert self.call(engine, table=t_rich, values=values) == (1, 1)
            row_b = {
                "ignore": ("B", "s2", "old", 2, 4, "lb"),
                "merge": ("B", "S2", "old", 2, 4, "lb"),
            }[self.strategy]
            row_z = ("Z", "sz", "new", 7, 14, "lz")
            assert read_rows(engine, t_rich) == [rows_before[0], row_b, row_z]
        assert_no_staging(engine)

    def test_composite_unique_key_partly_written(self, engine):
        """
        Merge writes only ``b`` of the unique key ``(a, b)``: row 1 keeps its
        ``a`` and becomes ``(1, 2)``, the value the new record 2 has, so merge
        is an error. Replace writes ``a`` too (row 1 becomes ``(9, 2)``), and
        ignore writes nothing, so both succeed.
        """
        values = [
            {"id": 1, "a": 9, "b": 2, "tag": "t1"},
            {"id": 2, "a": 1, "b": 2, "tag": "t2"},
        ]
        rows_before = read_rows(engine, t_pair)
        if self.strategy == "merge":
            with pytest.raises(sa.exc.IntegrityError):
                self.call(engine, table=t_pair, values=values)
            assert read_rows(engine, t_pair) == rows_before
        else:
            assert self.call(engine, table=t_pair, values=values) == (1, 1)
            row_1 = {
                "ignore": (1, 1, 1, "t1"),
                "replace": (1, 9, 2, "t1"),
            }[self.strategy]
            assert read_rows(engine, t_pair) == [row_1, (2, 1, 2, "t2")]
        assert_no_staging(engine)

    def test_unique_index(self, engine):
        """
        A unique index counts as a unique key, like a unique constraint.
        """
        values = [{"id": 2, "a": 5, "b": 5, "tag": "t1"}]
        rows_before = read_rows(engine, t_pair)
        with pytest.raises(sa.exc.IntegrityError):
            self.call(engine, table=t_pair, values=values)
        assert read_rows(engine, t_pair) == rows_before

    def test_unique_key_nulls(self, engine):
        """
        NULLs never conflict: records whose unique columns are all NULL, or
        partly NULL, are inserted.
        """
        values = [
            {"id": 2, "a": None, "b": None, "tag": None},
            {"id": 3, "a": None, "b": None, "tag": None},
            {"id": 4, "a": 1, "b": None, "tag": None},
            {"id": 5, "a": 1, "b": None, "tag": None},
        ]
        assert self.call(engine, table=t_pair, values=values) == (0, 4)
        assert len(read_rows(engine, t_pair)) == 5

    def test_foreign_keys(self, engine):
        """
        Upserting a parent never deletes its children (ON DELETE CASCADE),
        and a table with a foreign key can be upserted.
        """
        values = [{"id": 1, "name": "P1"}, {"id": 3, "name": "p3"}]
        assert self.call(engine, table=t_parent, values=values) == (1, 1)
        assert len(read_rows(engine, t_child)) == 2
        values = [
            {"id": 10, "parent_id": 2, "name": "C10"},
            {"id": 12, "parent_id": 3, "name": "c12"},
        ]
        assert self.call(engine, table=t_child, values=values) == (1, 1)
        assert len(read_rows(engine, t_child)) == 3

    def test_pk_only_table(self, engine):
        values = [{"id": 1}, {"id": 2}]
        assert self.call(engine, table=t_pk_only, values=values) == (1, 1)
        assert read_rows(engine, t_pk_only) == [(1,), (2,)]

    def test_long_table_name(self, engine):
        values = [{"id": 1, "name": "A"}, {"id": 2, "name": "b"}]
        assert self.call(engine, table=t_long, values=values) == (1, 1)
        expected_1 = (1, "a") if self.strategy == "ignore" else (1, "A")
        assert read_rows(engine, t_long) == [expected_1, (2, "b")]

    # --- scale and concurrency ---------------------------------------------------
    def test_large(self, engine):
        n = LARGE_N
        with engine.begin() as conn:
            conn.execute(t_record.delete())
            conn.execute(
                t_record.insert(),
                [{"id": i, "name": "old", "note": "old"} for i in range(n)],
            )
        values = [
            {"id": i, "name": "new", "note": "new"} for i in range(n // 2, n // 2 + n)
        ]
        assert self.call(engine, values=values) == (n // 2, n // 2)
        # count(CASE ...), not count(*) FILTER (...): MySQL has no FILTER.
        stmt = sa.select(
            sa.func.count(),
            sa.func.count(sa.case((t_record.c.name == "new", 1))),
            sa.func.count(sa.case((t_record.c.note == "new", 1))),
        )
        with engine.connect() as conn:
            total, new_name, new_note = conn.execute(stmt).one()
        assert total == n + n // 2
        assert new_name == (n // 2 if self.strategy == "ignore" else n)
        assert new_note == (n if self.strategy == "replace" else n // 2)

    def test_concurrent_calls(self, engine, db_url):
        """
        Four auto-managed calls at once, on overlapping keys, all succeed and
        leave the union of their records.
        """
        concurrent_engine = sa.create_engine(db_url, pool_size=4)
        batches = [
            [
                {"id": i, "name": f"t{k}", "note": None}
                for i in range(k * 10, k * 10 + 20)
            ]
            for k in range(4)
        ]
        results, errors = list(), list()

        def work(batch):
            try:
                results.append(self.call(concurrent_engine, values=batch))
            except Exception as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=work, args=(batch,)) for batch in batches]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        concurrent_engine.dispose()
        assert errors == []
        assert all(sum(counts) == 20 for counts in results)
        ids = {row[0] for row in read_rows(engine)}
        assert ids == {1, 2, 3} | set(range(50))


class MergeContract(StrategyContract):
    """
    The contract, plus the checks of merge's ``columns`` argument.
    """

    strategy = "merge"

    @pytest.mark.parametrize(
        "columns",
        [
            pytest.param([], id="empty"),
            pytest.param(["nope"], id="unknown"),
            pytest.param(["name", "nope"], id="one_unknown"),
        ],
    )
    def test_invalid_columns(self, engine, columns):
        with pytest.raises(ValueError):
            self.call(engine, columns=columns)
        assert read_rows(engine) == EXISTING_ROWS

    def test_column_missing_from_records(self, engine):
        """
        A column the records do not have has no new value to merge.
        """
        values = [{"id": 2, "name": "B"}]
        with pytest.raises(ValueError):
            self.call(engine, values=values, columns=["note"])

    def test_primary_key_only_columns(self, engine):
        """
        Merging only the primary key updates nothing: conflicts are left alone.
        """
        assert self.call(engine, columns=["id"]) == (2, 1)
        assert read_rows(engine) == EXPECTED_ROWS["ignore"]
