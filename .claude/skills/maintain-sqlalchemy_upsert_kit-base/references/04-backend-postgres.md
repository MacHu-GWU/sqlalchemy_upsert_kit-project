---
description: "How the upsert behaves on PostgreSQL (sqlalchemy_upsert_kit/postgres/): transactional DDL and the SAVEPOINT, an already aborted caller transaction, the native upsert and its row counts under concurrency, identifier length, and what is untested. Behavior only; the code is the source of truth for how. Read before changing the postgres backend, or when a PostgreSQL upsert's counts or transaction state look wrong."
---

# Backend: PostgreSQL

This file describes behavior, which does not change when the code is refactored. For how it is implemented, read `sqlalchemy_upsert_kit/postgres/executor.py`; that code is the source of truth, and nothing here repeats it.

The driver is psycopg 3 (the `postgres` extra). The backend is tested against PostgreSQL 18 locally and against the PostgreSQL preinstalled on the CI runner images (16 on Ubuntu, 17 on Windows).

---

## 1. Transactions

- `Connection.begin()` opens a real transaction, and DDL is transactional, so `PostgresUpsertExecutor` keeps the default `begin_transaction` (nothing to do). Rolling back to the SAVEPOINT removes the staging table along with everything else.
- After a failed call in user-managed mode the caller's transaction is **usable again**: the rollback to the SAVEPOINT clears PostgreSQL's aborted state. The contract's `test_failure_in_user_mode` retries and commits in the same transaction.
- If the caller's transaction is **already aborted** before the call, the SAVEPOINT itself fails with `InFailedSqlTransaction`, and the call changes nothing (found in review; no test).

---

## 2. The upsert statement and its counts

- One native `INSERT ... SELECT ... ON CONFLICT (<pk>) DO NOTHING | DO UPDATE`. Conflicting rows are updated in place: update triggers fire, delete triggers and `ON DELETE` foreign key actions never do.
- `DO NOTHING`'s row count is exactly the inserted rows, so ignore's counts are always exact. `DO UPDATE` counts inserted and updated rows together, so replace and merge count the conflicts with a join first. Under PostgreSQL's default `READ COMMITTED`, **if another transaction inserts the same keys between the count and the upsert, the split between conflicting and inserted can be off**; their sum is always `len(values)`, and the resulting rows are always right. The contract's `test_concurrent_calls` asserts only the sum and the rows for that reason.

---

## 3. Names

- Identifiers are limited to 63 characters. The generated staging name, `temp_<8 hex>_<table>`, is cut to `engine.dialect.max_identifier_length`; `test_long_table_name` uses a 61-character table.
- A temporary table shadows a permanent one of the same name in `search_path`, which made a `temp_table_name` equal to the target's silently lose data. The executor now rejects that name on every backend.

---

## 4. Not tested

- Tables in a non-default schema, and connections with a custom `search_path` (a review probe found schemas work).
- Callers at the `REPEATABLE READ` or `SERIALIZABLE` isolation level.
