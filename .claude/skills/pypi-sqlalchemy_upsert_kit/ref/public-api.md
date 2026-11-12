# sqlalchemy_upsert_kit: Public API Reference

Version 0.2.1. Every public name is reachable from `sqlalchemy_upsert_kit.api`:

| Name | What it is |
| :--- | :--- |
| `api.sqlite` | Module: the three strategies for SQLite engines |
| `api.sqlite.insert_or_ignore` | Insert new rows, keep existing ones |
| `api.sqlite.insert_or_replace` | Insert new rows, overwrite existing ones |
| `api.sqlite.insert_or_merge` | Insert new rows, update chosen columns of existing ones |
| `api.postgres` | Module: the same three for PostgreSQL engines (needs the `postgres` extra) |
| `api.postgres.insert_or_ignore` / `insert_or_replace` / `insert_or_merge` | As above |

```python
from sqlalchemy_upsert_kit.api import sqlite
from sqlalchemy_upsert_kit.api import postgres
import sqlalchemy_upsert_kit.api as sqlalchemy_upsert_kit  # sqlalchemy_upsert_kit.postgres.insert_or_merge
```

Importing `api` does not import psycopg; only connecting to a PostgreSQL engine needs it.

---

## Shared behavior

All six functions share these rules. The SQLite and PostgreSQL versions give the same rows and counts for the same input.

### How a call works

1. Validate `values` (and `columns`). Any problem raises `ValueError` before any SQL runs.
2. Open a SAVEPOINT inside the transaction (yours, or one the call opens).
3. Create a `TEMPORARY` staging table, private to the connection, holding the table's writable columns with their types and defaults (no indexes, foreign keys, or constraints besides the primary key).
4. Bulk insert `values` into it.
5. Run one `INSERT INTO <table> SELECT ... FROM <staging> ON CONFLICT (<pk>) DO NOTHING | DO UPDATE SET ...`.
6. Drop the staging table and release the SAVEPOINT.

### Parameters

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `engine` | `sa.Engine` | required | Engine for the database. Used to open a connection in auto-managed mode, and for its dialect. |
| `table` | `sa.Table` | required | Target table (SQLAlchemy Core). Exactly one primary key column. An ORM class's table is `Model.__table__`. |
| `values` | `list[dict[str, Any]]` | required | Records, keyed by **column key** (`Column(..., key=...)`; equals the column name unless set). |
| `columns` | `list[str]` | required | `insert_or_merge` only: keys of the columns to update on conflicting rows. |
| `metadata` | `sa.MetaData \| None` | `None` | Metadata that holds the staging table definition during the call; a fresh one when None. Passing the target's own metadata is fine: the staging table is removed from it afterwards. |
| `temp_table_name` | `str \| None` | `None` | Name of the staging table. Generated when None (`temp_<8 hex>_<table>`, cut to the dialect's identifier limit, unique per call). Must not equal `table.name`. |
| `conn` | `sa.Connection \| None` | `None` | Connection of your transaction (user-managed mode). Give with `trans`. |
| `trans` | `sa.Transaction \| None` | `None` | Your transaction (user-managed mode). Give with `conn`. |

Leave `temp_table_name` and `metadata` at None unless you have a reason; the generated name never collides.

### Return value

`tuple[int, int]`: `(conflicting_rows, inserted_rows)`.

- `conflicting_rows`: records whose primary key already existed (ignored, replaced, or merged, by strategy).
- `inserted_rows`: records that became new rows.
- `conflicting_rows + inserted_rows == len(values)`, always.
- Empty `values` returns `(0, 0)` without touching the database.

### Validation (`ValueError`)

- A record key that is not a column key of `table`, or is a generated (`sa.Computed`) column.
- A record without the primary key, with a `None` primary key, or two records with the same primary key.
- Records with different key sets (every record must have exactly the same keys).
- `table` without exactly one primary key column.
- `conn` given without `trans`, or `trans` without `conn`.
- `temp_table_name == table.name`.
- `insert_or_merge` only: `columns` empty, naming a column not in `table`, or naming a column the records do not have.

Nothing is written when one of these is raised.

### Transactions

**Auto-managed** (`conn=None, trans=None`): the call opens its own connection and transaction, and commits on success. On failure, everything is rolled back and the exception propagates.

**User-managed** (both given): the call runs inside your transaction and never commits it.

```python
with engine.connect() as conn:
    with conn.begin() as trans:
        a = sqlite.insert_or_ignore(engine, t_record, values, conn=conn, trans=trans)
        b = sqlite.insert_or_ignore(engine, t_record, values, conn=conn, trans=trans)
        # a == (2, 1), b == (3, 0): the second call sees the first call's rows
    # committed here, by the `with conn.begin()` block
```

- `trans.rollback()` after a successful call undoes the call, even if you wrote nothing before it.
- If the call fails, only the call is undone: your earlier writes in the same transaction remain, no staging table is left, and the transaction is usable. You can retry, then commit.
- On PostgreSQL, if your transaction is already aborted (an earlier statement failed), the call fails at once and changes nothing; roll back first.

### Behavior notes

- Conflicting rows are **updated in place**, never deleted and re-inserted. `UPDATE` triggers fire; `DELETE` triggers and `ON DELETE` foreign key actions (`CASCADE`, `SET NULL`) do not.
- Columns absent from the records get their Python `default=` or `server_default=` on newly inserted rows.
- Generated (`sa.Computed`) columns are computed by the database; you cannot write them.
- Concurrency: concurrent calls never see each other's staging table. On SQLite, the call takes the write lock at its start (`BEGIN IMMEDIATE` when no transaction is open yet), so concurrent writers wait for each other up to the driver's busy timeout (default 5 s) instead of failing with "database is locked". On PostgreSQL (`READ COMMITTED`), when another transaction inserts the same keys during `insert_or_replace` / `insert_or_merge`, the split between the two counts can be off; their sum and the final rows are always right.
- Size: tested with 10,000 records against 10,000 existing rows; the whole batch goes into one staging table, so split very large inputs into chunks if memory is a concern.
- Requirements: SQLAlchemy 2.0.28+; SQLite 3.24+ (`ON CONFLICT ... DO UPDATE`); PostgreSQL with psycopg 3 (`postgresql+psycopg://...`). Sync engines only.

---

## `insert_or_ignore`

```python
def insert_or_ignore(
    engine: sa.Engine,
    table: sa.Table,
    values: list[dict[str, T.Any]],
    metadata: sa.MetaData | None = None,
    temp_table_name: str | None = None,
    conn: sa.Connection | None = None,
    trans: sa.Transaction | None = None,
) -> tuple[int, int]
```

Insert the records whose primary key does not exist yet; leave existing rows exactly as they are. SQL: `ON CONFLICT (<pk>) DO NOTHING`.

Returns `(ignored_rows, inserted_rows)`.

```python
# table rows: (1, "a", "n1"), (2, "b", "n2"), (3, "c", "n3")
values = [
    {"id": 3, "name": "C", "note": "N3"},
    {"id": 4, "name": "d", "note": "n4"},
    {"id": 2, "name": "B", "note": "N2"},
]
assert sqlite.insert_or_ignore(engine, t_record, values) == (2, 1)
# rows: (1, a, n1), (2, b, n2), (3, c, n3), (4, d, n4)
```

---

## `insert_or_replace`

```python
def insert_or_replace(
    engine: sa.Engine,
    table: sa.Table,
    values: list[dict[str, T.Any]],
    metadata: sa.MetaData | None = None,
    temp_table_name: str | None = None,
    conn: sa.Connection | None = None,
    trans: sa.Transaction | None = None,
) -> tuple[int, int]
```

Insert new records, and overwrite each existing row so it equals its record. SQL: `ON CONFLICT (<pk>) DO UPDATE SET` every non-primary-key column the staging table holds.

**Gotcha:** a column you leave out of the records is not kept on existing rows: it is reset to its default (or NULL), because the staged record has the default there. To update only some columns and keep the rest, use `insert_or_merge`.

Returns `(replaced_rows, inserted_rows)`.

```python
assert postgres.insert_or_replace(engine, t_record, values) == (2, 1)
# rows: (1, a, n1), (2, B, N2), (3, C, N3), (4, d, n4)
```

A table whose only column is the primary key behaves like `insert_or_ignore`.

---

## `insert_or_merge`

```python
def insert_or_merge(
    engine: sa.Engine,
    table: sa.Table,
    values: list[dict[str, T.Any]],
    columns: list[str],
    metadata: sa.MetaData | None = None,
    temp_table_name: str | None = None,
    conn: sa.Connection | None = None,
    trans: sa.Transaction | None = None,
) -> tuple[int, int]
```

Insert new records in full; on existing rows update only the `columns` and keep every other column. SQL: `ON CONFLICT (<pk>) DO UPDATE SET` only `columns`.

- `columns`: column keys, not empty, each one a key of the records. Listing the primary key is allowed but updates nothing (`columns=["id"]` behaves like `insert_or_ignore`).
- New rows get every column the records hold, not only `columns`.

Returns `(updated_rows, inserted_rows)`.

```python
assert sqlite.insert_or_merge(engine, t_record, values, columns=["name"]) == (2, 1)
# rows: (1, a, n1), (2, B, n2), (3, C, n3), (4, d, n4): note kept on 2 and 3

sqlite.insert_or_merge(engine, t_record, [{"id": 2, "name": "B"}], columns=["note"])
# ValueError: "note" is not a key of the records
```

---

## Choosing a strategy

| You want, for rows that already exist... | Use |
| :--- | :--- |
| leave them alone (append only, idempotent loads) | `insert_or_ignore` |
| make them equal the incoming record (full sync from a source of truth) | `insert_or_replace` |
| refresh some fields, keep the others (such as a local status column) | `insert_or_merge` |
