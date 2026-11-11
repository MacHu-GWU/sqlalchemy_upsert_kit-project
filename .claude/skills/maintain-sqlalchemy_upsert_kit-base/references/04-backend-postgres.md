---
description: "How the three strategies behave on PostgreSQL (sqlalchemy_upsert_kit/postgres/): the temporary staging table and what it does not copy, in-place updates through ON CONFLICT and how they differ from SQLite, the transactional cleanup, the row counts, and the dropped test flags. Behavior only; the code is the source of truth for how. Read before changing the postgres backend, or when a PostgreSQL upsert behaves differently from the SQLite one."
---

# Backend: PostgreSQL

This file describes behavior, which does not change when the code is refactored. For how it is implemented, read `sqlalchemy_upsert_kit/postgres/executor.py` and the strategy modules beside it; that code is the source of truth, and nothing here repeats it.

The driver is psycopg 3 (the `postgres` extra). The backend is tested against PostgreSQL 18 locally and against the PostgreSQL preinstalled on the CI runner images (16 on Ubuntu, 17 on Windows).

---

## 1. Staging table

- A **`TEMPORARY` table with `ON COMMIT DROP`**: private to the session, so concurrent upserts never collide on its name, and PostgreSQL drops it at commit even if the explicit drop never runs.
- It copies each column's name, type, primary key, server default and Python default, but **no indexes, unique constraints, foreign keys or sequences**. Copying them would fail: index names are unique per schema, and a temporary table may not reference a permanent one. `test_clone_staging_table_creates_in_postgres` in `tests/postgres/test_postgres_executor.py` proves a table with all of these clones and creates cleanly.
- It lives in the session's `pg_temp` schema, so reflecting the default schema does not see it. To look for leftovers, query `pg_class` (as `DataFaker.check_no_temp_tables` does).

---

## 2. The three strategies

All three write the target with **one native `INSERT ... SELECT ... ON CONFLICT (<primary key>)`** statement:

| Strategy | Conflicting rows | Difference from SQLite |
| :--- | :--- | :--- |
| `insert_or_ignore` | left untouched (`DO NOTHING`) | none |
| `insert_or_replace` | **updated in place**: every non-key column (`DO UPDATE`) | no delete: delete triggers and `ON DELETE` foreign key actions never fire. A table with only the key column has nothing to update, so conflicts are left untouched |
| `insert_or_merge` | **updated in place**: only `columns` (`DO UPDATE`) | no delete, and no second staging table |

The returned counts: `rowcount` of `DO UPDATE` counts inserted plus updated rows, and that of `DO NOTHING` counts inserted rows only, so the conflicting rows are counted first with a join against the staging table. SQLAlchemy keeps `rowcount` of an `INSERT` only with `preserve_rowcount=True`; without it the count is -1.

---

## 3. Transactions and cleanup

- PostgreSQL DDL is transactional. The staging table is created inside the operation's transaction, so a rollback removes it, and the failure path only forgets it from the `MetaData` (`PostgresUpsertExecutor.cleanup_temp_table_on_failure`), without a fresh connection.
- In user-managed mode, if the caller catches the error and goes on in the same transaction, the staging table still exists until the commit (`ON COMMIT DROP`) or rollback. A second call with the same `temp_table_name` in that transaction would then fail on the name (this follows from `ON COMMIT DROP`; no test covers it).
- The known SQLite leak does not happen here.

---

## 4. Test flags

PostgreSQL has no delete step and no merge staging table, so its functions take only four `_raise_on_*` flags: `_raise_on_temp_table_create`, `_raise_on_temp_data_insert`, `_raise_on_target_insert`, `_raise_on_temp_table_drop`.
