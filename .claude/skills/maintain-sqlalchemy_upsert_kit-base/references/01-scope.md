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
| `insert_or_replace` | overwrite every column with the input | insert | `(replaced_rows, inserted_rows)` |
| `insert_or_merge` | overwrite only the columns named in `columns`, keep the rest | insert | `(updated_rows, inserted_rows)` |

These semantics are the contract. A backend may reach them with different SQL, and so with different side effects (such as whether delete triggers fire, listed in each backend's reference), but never with different resulting rows or counts.

---

## 3. The contract every backend keeps

- **Public parameters** are the same in every backend: `engine`, `table`, `values`, `columns` (merge only, required), `metadata`, `temp_table_name`, `conn`, `trans`. A user switches backend by switching the `api.<engine>` namespace and nothing else.
- **One primary key column.** `get_pk_name` in `sqlalchemy_upsert_kit/utils.py` rejects zero or several. Conflicts are detected on that key only.
- **Empty input is a no-op** that returns `(0, 0)`. `insert_or_merge` with empty `columns` raises `ValueError`.
- **Two transaction modes**, enforced by `UpsertExecutor.__post_init__` in `sqlalchemy_upsert_kit/core/executor.py`:
  - auto-managed: `conn` and `trans` are both None; the function opens, commits, or rolls back its own transaction;
  - user-managed: both are given; the function runs inside the caller's transaction and never commits it. Giving only one is a `ValueError`.
- **On failure, the target data is unchanged** once the transaction is rolled back, and **no staging table is left behind** in either mode. The tests check both after every failure. One known SQLite case breaks the second rule; the SQLite backend reference describes it.
- **`_raise_on_*` parameters are for tests only.** They inject a failure at one step, so the tests can prove the two rules above. They are private, and a backend keeps only the ones whose step it has (`postgres/` has no delete or merge-table step, so it drops `_raise_on_target_delete` and `_raise_on_merge_update`).

---

## 4. Out of scope

- **Stores that SQLAlchemy does not drive,** such as MongoDB. Their upsert has nothing in common with this staging-table design; they belong in a separate library.
- **Not supported today:** composite primary keys and conflict targets other than the primary key (`get_pk_name`), and per-row results (the functions return counts only). Adding either changes the contract in section 3 for every backend, so ask the user first.
