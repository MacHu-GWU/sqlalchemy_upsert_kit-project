---
description: "How the three strategies behave on SQLite (sqlalchemy_upsert_kit/sqlite/): the staging table, which strategies delete and re-insert rows and what that triggers, the DDL and transaction behavior, and the known staging-table leak. Behavior only; the code is the source of truth for how. Read before changing the sqlite backend, or when a SQLite upsert has a surprising side effect."
---

# Backend: SQLite

This file describes behavior, which does not change when the code is refactored. For how a strategy is implemented, read its module in `sqlalchemy_upsert_kit/sqlite/`; that code is the source of truth, and nothing here repeats it.

---

## 1. Staging table

- A **regular table** (not `TEMPORARY`), built by `clone_temp_table` in `sqlalchemy_upsert_kit/utils.py`, which copies the target with SQLAlchemy's `Table.to_metadata`, so indexes and constraints come along too.
- Named `temp_<YYYYmmddHHMMSS>_<table>` (`get_temp_table_name`) unless the caller passes `temp_table_name`. The timestamp has one-second resolution, and the table is visible to other connections while it exists, so two upserts into the same table in the same second can collide on the name.
- `sqlite/` has no executor subclass: it uses the default hooks of `UpsertExecutor`.

---

## 2. The three strategies

| Strategy | Conflicting rows | Side effects on conflicting rows |
| :--- | :--- | :--- |
| `insert_or_ignore` | left untouched | none |
| `insert_or_replace` | **deleted, then inserted again** from the staging table | delete triggers and `ON DELETE` foreign key actions fire (when `PRAGMA foreign_keys` is on), then insert triggers |
| `insert_or_merge` | **deleted, then inserted again** with the merged values (the `columns` from the input, the rest from the old row) | same as replace. It also uses a second, `TEMPORARY` staging table, `<temp_table_name>_merge`, dropped before the function returns |

So on SQLite, replace and merge are not in-place updates. A child table with `ON DELETE CASCADE` loses its rows when its parent is replaced or merged.

SQLite has all six `_raise_on_*` test flags; replace and merge add `_raise_on_target_delete`, and merge adds `_raise_on_merge_update`.

---

## 3. DDL and transactions

- Python's `sqlite3` driver opens a transaction only before the first data-changing statement. A `CREATE TABLE` that runs before any write in the transaction therefore **commits on its own**, and a later rollback does not remove the staging table.
- That is why the failure path drops the staging table through **a fresh connection** (`UpsertExecutor.cleanup_temp_table_on_failure`), and suppresses any error from that drop, so the original exception is the one raised.

---

## 4. Known bug: staging table left behind

**When:** user-managed mode, the caller's transaction has not written anything before the call, and the operation fails after the staging rows are loaded (for example at the target insert).

**What happens:** the staging table was created outside the transaction (section 3). The staged rows hold the caller's write lock, so the fresh cleanup connection waits for SQLite's busy timeout (about 5 seconds), its drop fails, the error is suppressed, and the staging table remains after the caller rolls back.

**Evidence:** the `_raise_on_target_insert` case of `test_run_user_managed_failure` in `tests/core/test_core_executor.py`, marked `xfail(strict=True)`. The sqlite strategy tests miss it because each one writes rows in the caller's transaction before calling the upsert. Not fixed yet; when it is fixed, the strict xfail fails, and the mark must be removed.
