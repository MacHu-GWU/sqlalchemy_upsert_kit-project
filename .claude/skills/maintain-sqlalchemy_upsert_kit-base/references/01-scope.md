---
description: "What sqlalchemy_upsert_kit is for and what it is not: the three upsert strategies, the behavior contract every backend must keep (signatures, return values, transaction modes, cleanup, rollback), and what stays out of scope. Read before adding a feature, changing a strategy's behavior, or deciding whether a request belongs in this library."
---

# Scope and Strategy Contract

---

## 1. One job

The library does one thing: **bulk upsert into a SQLAlchemy table, with one of three strategies, on several database backends.** Every feature is either a strategy, a backend, or the shared machinery behind them. A request that is none of these (a query builder, schema migration, a general bulk loader) does not belong here.

Every strategy works the same way: load the input records into a staging table, then write them to the target table with set-based SQL, never row by row. That is where the speed comes from (see `README.rst`).

---

## 2. The three strategies

Each backend exposes the same three functions in `sqlalchemy_upsert_kit/<engine>/api.py`:

| Function | On a primary key that already exists | On a new primary key | Returns |
| :--- | :--- | :--- | :--- |
| `insert_or_ignore` | leave the existing row untouched | insert | `(ignored_rows, inserted_rows)` |
| `insert_or_replace` | overwrite every column with the record; a column the records leave out is reset to its default, or NULL | insert | `(replaced_rows, inserted_rows)` |
| `insert_or_merge` | overwrite only the columns named in `columns`, keep the rest | insert | `(updated_rows, inserted_rows)` |

These semantics are the contract: the same resulting rows and counts on every backend. Conflicting rows are updated in place on every backend, so update triggers fire, and delete triggers and `ON DELETE` foreign key actions never do.

---

## 3. The contract every backend keeps

- **Public parameters** are the same in every backend: `engine`, `table`, `values`, `columns` (merge only, required), `metadata`, `temp_table_name`, `conn`, `trans`. A user switches backend by switching the `api.<engine>` namespace and nothing else.
- **One primary key column** (`get_pk_column` in `sqlalchemy_upsert_kit/core/schema.py`). Conflicts are detected on that key only.
- **Records are keyed by column key**, not column name, and all must have the same keys.
- **Bad input raises `ValueError` before any SQL runs** (`sqlalchemy_upsert_kit/core/validation.py` and `UpsertExecutor.__post_init__`): a key that is not a writable column (generated columns are not writable), a missing, None or repeated primary key, records with different keys, merge `columns` that are empty, unknown, or not keys of the records, a `conn` without `trans` or the reverse, and a `temp_table_name` equal to the target's name. Merge `columns` that name only the primary key update nothing.
- **Empty `values` is a no-op** that returns `(0, 0)`, after the argument checks.
- **Two transaction modes:**
  - auto-managed: `conn` and `trans` are both None; the function opens, commits, or rolls back its own transaction;
  - user-managed: both are given; the function runs inside the caller's transaction and never commits it. A later rollback by the caller undoes the call.
- **On failure, the call changes nothing and leaves no staging table,** in either mode. In user-managed mode the caller's transaction keeps its own earlier writes and stays usable. The tests check all of this after every failure.
- **Counts:** the two numbers always add up to `len(values)`. A backend may document a case where their split is approximate (concurrent writers on PostgreSQL).
- **`_fail_at` is for tests only.** It injects a failure before one of `FAIL_STEPS` (`sqlalchemy_upsert_kit/core/executor.py`), so the tests can prove the failure rules above.

---

## 4. Out of scope

- **Stores that SQLAlchemy does not drive,** such as MongoDB. Their upsert has nothing in common with this staging-table design; they belong in a separate library.
- **Not supported today:** composite primary keys and conflict targets other than the primary key (`get_pk_column`), and per-row results (the functions return counts only). Adding either changes the contract in section 3 for every backend, so ask the user first.
