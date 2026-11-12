---
description: "How the upsert behaves on SQLite (sqlalchemy_upsert_kit/sqlite/): why a BEGIN IMMEDIATE is sent before the SAVEPOINT, how concurrent calls serialize, the native upsert and its WHERE true, the version floor, what triggers fire, and what is untested. Behavior only; the code is the source of truth for how. Read before changing the sqlite backend, or when a SQLite upsert commits, locks, or fires triggers unexpectedly."
---

# Backend: SQLite

This file describes behavior, which does not change when the code is refactored. For how it is implemented, read `sqlalchemy_upsert_kit/sqlite/executor.py`; that code is the source of truth, and nothing here repeats it.

---

## 1. A real transaction before the SAVEPOINT

- Python's `sqlite3` driver sends `BEGIN` only before the first data-changing statement, so after `Connection.begin()` there may be no database transaction yet.
- **The trap:** without one, the SAVEPOINT is itself the outermost transaction, and releasing it **commits** the upsert. A caller in user-managed mode who wrote nothing before the call, and then rolls back, would find the upsert committed anyway.
- So `SqliteUpsertExecutor.begin_transaction` sends `BEGIN IMMEDIATE` when the driver has not opened a transaction. Afterwards the caller's `commit()` or `rollback()` acts on it as usual.
- **Evidence:** `test_caller_rollback_undoes_the_upsert[False]` in the contract (`sqlalchemy_upsert_kit/tests/contract.py`) and `test_begin_transaction` in `tests/sqlite/test_sqlite_executor.py` fail without it.

---

## 2. Concurrency

- `IMMEDIATE` takes the database write lock at the start. Concurrent upserts therefore wait for each other, up to the driver's busy timeout (5 seconds by default), instead of one failing with "database is locked" when two deferred transactions both try to upgrade their read locks. `test_concurrent_calls` (four threads) depends on it.
- The staging table is `TEMPORARY`, so it lives in the connection's own `temp` schema: concurrent calls never see each other's.
- Counts are exact, because the write lock is held from before the conflict count until the commit.

---

## 3. The upsert statement

- One native `INSERT ... SELECT ... ON CONFLICT (<pk>) DO NOTHING | DO UPDATE`, which needs **SQLite 3.24** or later. The contract's generated column needs 3.31, for the tests only.
- The `SELECT` must have a `WHERE` clause (`WHERE true`, rendered `WHERE 1 = 1`); without one, SQLite's parser takes `ON CONFLICT` as part of the join and reports a syntax error.
- Conflicting rows are updated in place: **update triggers** fire, delete triggers and `ON DELETE` foreign key actions never do. `test_foreign_keys` runs with `PRAGMA foreign_keys = ON`, so a delete would cascade and fail it.

---

## 4. Not supported or not tested

- A `sqlite3` connection opened with Python 3.12's `autocommit=True`, and SQLAlchemy's `AUTOCOMMIT` isolation level: the `BEGIN IMMEDIATE` would not be committed the usual way. Neither is tested.
- `aiosqlite` and other SQLite drivers: `begin_transaction` reads the `sqlite3` connection's `in_transaction`.
