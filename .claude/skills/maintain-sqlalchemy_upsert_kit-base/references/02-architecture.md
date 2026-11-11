---
description: "The package architecture: what lives in sqlalchemy_upsert_kit/core/ and what in each sqlalchemy_upsert_kit/<engine>/ subpackage, the hooks a backend overrides, the api.<engine> namespaces and driver extras, and why the code is split per backend instead of one module branching on the dialect. Read before moving code between core and a backend, or adding shared logic."
---

# Architecture: Core and Backends

---

## 1. Layout

```text
sqlalchemy_upsert_kit/
├── core/executor.py    # UpsertExecutor: the dialect-agnostic workflow
├── utils.py            # get_pk_name, get_temp_table_name, clone_temp_table
├── exc.py              # UpsertTestError, raised by the _raise_on_* test flags
├── sqlite/             # one backend
│   ├── insert_or_ignore.py / insert_or_replace.py / insert_or_merge.py
│   └── api.py          # exposed as api.sqlite
├── postgres/           # another backend
│   ├── executor.py     # PostgresUpsertExecutor: what PostgreSQL does differently
│   ├── insert_or_ignore.py / insert_or_replace.py / insert_or_merge.py
│   └── api.py          # exposed as api.postgres
└── api.py              # one line per backend: from .<engine> import api as <engine>
```

The subpackage name is the backend name used everywhere else: the local database mise tasks, the test folder `tests/<engine>/`, and the `pyproject.toml` extra. PostgreSQL's is `postgres`; its mise tasks use the shorter `psql`.

---

## 2. What goes where

**`core/executor.py`** holds only the workflow, which is the same for every database: `UpsertExecutor.run()` picks the transaction mode, `execute_operation()` creates the staging table, loads it, calls the strategy, and drops it, and the failure path rolls back and cleans up. It holds **no strategy SQL** and imports no dialect.

**Each backend subpackage** holds everything its database does differently:

- a strategy class per function, each implementing `apply_strategy()`, the only abstract method;
- optionally its own executor subclass (`postgres/executor.py`), overriding the hooks:

  | Hook on `UpsertExecutor` | Override when the backend |
  | :--- | :--- |
  | `clone_temp_table()` | needs a different staging table definition |
  | `cleanup_temp_table_on_failure()` | cleans up differently after a rollback |
  | `create_temp_table()`, `insert_temp_data()`, `cleanup_temp_table_on_success()` | loads or drops the staging table differently |

  The default hooks fit SQLite, so `sqlite/` has no executor of its own.

**The rule: shared code is only the flow, never the SQL.** When two backends end up with the same helper, move it to `core/` only if it contains no SQL that some other database would need to write differently.

---

## 3. Public API and dependencies

- Users import a backend namespace: `from sqlalchemy_upsert_kit.api import postgres`, then `postgres.insert_or_merge(...)`. There is no function that dispatches on `engine.dialect`; one could be added later as a thin layer without changing the per-backend functions.
- The runtime dependency is SQLAlchemy alone (`sqlalchemy>=2.0.28`; 2.0.28 added the `preserve_rowcount` option that `postgres/` relies on). The SQLAlchemy dialect modules import without a driver.
- **Each backend that needs a driver gets its own extra** in `pyproject.toml` (`postgres = ["psycopg[binary]..."]`), and the same driver is added to the `test` extra, so CI, which installs only `--extra test`, can run that backend's tests.

---

## 4. Why one subpackage per backend

The user chose this design over one module that branches on the dialect, for these reasons:

- **A change for one database must not ripple into the others.** With one shared module, every fix for one backend risks the rest. With a subpackage each, a backend changes alone, and its tests prove it.
- **Databases differ in ways that would turn shared code into `if dialect == ...` everywhere.** Found while adding PostgreSQL, against the original SQLite design:
  - PostgreSQL DDL is transactional; SQLite's `CREATE TABLE` can commit on its own, outside the caller's transaction.
  - A PostgreSQL `TEMPORARY` table is visible only to its own session, so the SQLite cleanup through a fresh connection cannot see it.
  - PostgreSQL index names are unique per schema, so cloning a table's indexes into a staging table collides.
  - MySQL rejects `DELETE ... WHERE pk IN (SELECT ... FROM the same table)` (error 1093, confirmed against MySQL 9.7), which the SQLite replace and merge strategies use, and its `CREATE TABLE` commits the caller's transaction.
  - Each database has its own native upsert (`ON CONFLICT`, `ON DUPLICATE KEY UPDATE`), which is the right tool once the SQL is per backend.
- **Duplication now, abstraction later.** Separate backends can be refactored at any time: the user-facing import stays the same, so shared logic is extracted from the backends once it is clearly identical, not designed up front.
