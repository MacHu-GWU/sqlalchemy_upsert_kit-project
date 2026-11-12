---
description: "The package architecture: what lives in sqlalchemy_upsert_kit/core/ (validation, staging table, the SAVEPOINT workflow) and what in each sqlalchemy_upsert_kit/<engine>/ subpackage (the transaction hook and the upsert statement), the api.<engine> namespaces and driver extras, why backends are separate subpackages, and why the shared part grew. Read before moving code between core and a backend, or adding shared logic."
---

# Architecture: Core and Backends

---

## 1. Layout

```text
sqlalchemy_upsert_kit/
├── core/
│   ├── schema.py       # get_pk_column, get_writable_columns, get_unique_keys, make_staging_name, make_staging_table
│   ├── validation.py   # validate_records, resolve_update_keys
│   └── executor.py     # UpsertExecutor: the workflow; FAIL_STEPS
├── exc.py              # UpsertTestError (the _fail_at test hook), UniqueKeyConflictError
├── sqlite/
│   ├── executor.py     # SqliteUpsertExecutor
│   ├── insert_or_ignore.py / insert_or_replace.py / insert_or_merge.py   # the public functions
│   └── api.py          # exposed as api.sqlite
├── postgres/           # the same files: PostgresUpsertExecutor, api.postgres
├── mysql/              # the same files: MysqlUpsertExecutor, api.mysql
└── api.py              # one line per backend: from .<engine> import api as <engine>
```

- The subpackage name is the backend name used everywhere else: the local database mise tasks, the test folder `tests/<engine>/`, and the `pyproject.toml` extra. PostgreSQL's is `postgres`, and its mise tasks use the shorter `psql`; MySQL's is `mysql` everywhere.
- **A backend's `__init__.py` imports nothing**, and users import from `api`. Importing SQLAlchemy while coverage starts breaks SQLAlchemy 2.1 statement compilation (`'InternalTraversal' object is not callable`; 2.0.44 is fine). A per-module coverage run (`run_cov_test` with `module="sqlalchemy_upsert_kit.sqlite.insert_or_ignore"`) makes coverage import the parent package at start, so a parent that imports SQLAlchemy breaks that run.

---

## 2. What goes where

**`core/`** holds everything that is the same on every database:

- **Validation** (`validation.py`): records and `columns` are checked before any SQL runs, so bad input fails the same way on every backend.
- **Strategy semantics** (`resolve_update_keys`): a strategy is just the set of column keys it overwrites on conflict: none (ignore), every staged non-key column (replace), or `columns` minus the key (merge).
- **The staging table** (`schema.py`): `TEMPORARY`, writable columns only, keys kept, the table's character set and collation options copied (so values compare as in the target), a random name cut to the identifier limit.
- **The workflow** (`UpsertExecutor.run_on`): `begin_transaction()`, then inside one SAVEPOINT: create the staging table, load it, `check_unique_keys()`, count conflicts (when something is updated, or the row count cannot tell inserted rows apart), run the upsert, `check_after_upsert()`, `drop_staging()`. Any error rolls back to the SAVEPOINT, calls `cleanup_after_rollback()` (whose own failure is added to the original error as a note), and re-raises.

**Each backend** holds only what its database does differently, as overrides of `UpsertExecutor`:

| Hook | Purpose | Default | Overridden by |
| :--- | :--- | :--- | :--- |
| `build_upsert(staging)` (abstract) | the one statement that copies the staged rows into the target | none | sqlite: `INSERT ... SELECT ... WHERE true ON CONFLICT`; postgres: `INSERT ... SELECT ... ON CONFLICT`; mysql: `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE` |
| `begin_transaction(conn)` | make sure a real transaction is open before the SAVEPOINT | nothing | sqlite: `BEGIN IMMEDIATE` when the driver has none |
| `max_identifier_length()` | the limit the generated staging name is cut to | the dialect's | mysql: 64 (the dialect says 255) |
| `check_unique_keys(conn, staging)` | before the upsert, reject records that break a unique key other than the primary key | nothing (the upsert raises) | mysql: two queries per declared unique key |
| `check_after_upsert(conn, staging)` | after the upsert, raise if it did not write every record | nothing | mysql: any record whose primary key is missing was lost to another row |
| `rowcount_counts_inserts` (class attribute) | whether the upsert's row count is exact; if False, conflicts are always counted first | True | mysql: False (`CLIENT_FOUND_ROWS`) |
| `drop_staging(conn, staging)` | drop the staging table without committing | `Table.drop()` | mysql: `DROP TEMPORARY TABLE` |
| `cleanup_after_rollback(conn, staging)` | after a failure, remove what the rollback did not | nothing | mysql: `DROP TEMPORARY TABLE IF EXISTS` |

Each backend also owns its three public function modules: the same signatures, each building its executor with a `strategy` and calling `run()`.

**The rule:** when backends need different SQL or transaction handling, it is a hook on `UpsertExecutor`, overridden in the backend. Logic is moved to `core/` only once it is identical on every backend.

---

## 3. Public API and dependencies

- Users import a backend namespace: `from sqlalchemy_upsert_kit.api import postgres`, then `postgres.insert_or_merge(...)`. There is no function that dispatches on `engine.dialect`.
- The runtime dependency is SQLAlchemy alone (`sqlalchemy>=2.0.28`: 2.0.28 added the `preserve_rowcount` execution option the workflow needs to read an `INSERT`'s row count). The SQLAlchemy dialect modules import without a driver.
- **Each backend that needs a driver gets its own extra** in `pyproject.toml` (`postgres = ["psycopg[binary]..."]`), and the same driver goes into the `test` extra, so CI, which installs only `--extra test`, can run that backend's tests.

---

## 4. Why one subpackage per backend

The user chose this over one module that branches on the dialect:

- **A change for one database must not ripple into the others.** Each backend changes alone, and its own test folder proves it.
- **Databases differ in ways that turn shared code into `if dialect == ...`.** Found while adding PostgreSQL: SQLite's driver delays `BEGIN`, PostgreSQL's does not; their `ON CONFLICT` syntax differs (SQLite needs `WHERE true`). Found while adding MySQL: it has no `ON CONFLICT` at all (`ON DUPLICATE KEY UPDATE`, which fires on any unique key), its temporary table DDL is not rolled back and a plain `DROP TABLE` commits, its row count cannot tell inserted rows apart, and its dialect overstates the identifier limit. Each became a hook in section 2, not a branch in `core/`.
- **Duplication first, abstraction once it is clearly identical.** The user's words: keep the backends apart, and extract shared logic later, since the user-facing import never changes.

## 5. Why the shared part grew (0.2 refactor)

The first PostgreSQL version kept the SQLite design (regular staging table cloned with `Table.to_metadata`, delete then insert for replace and merge, cleanup through a fresh connection) and added its own strategies. A review then found bugs in the SQLite design (foreign keys and named indexes broke the clone, a missing primary key overwrote rows, staging tables leaked) and contract divergences between the backends. The fix made both backends use a native `INSERT ... SELECT ... ON CONFLICT` and the same SAVEPOINT workflow, at which point the strategies, validation and staging table were identical, so, by the rule in section 2, they moved to `core/`.

---

## 6. Why a staging table plus one native upsert, not JOIN-based writes

Every backend writes with the same two parts, each with its own job:

- **The staging table makes it fast:** all records go to the database in one `executemany`, and every later statement works on the whole batch, instead of one round trip per record.
- **One native upsert makes it correct:** `ON CONFLICT` or `ON DUPLICATE KEY UPDATE` lets the database decide, atomically for each row, whether it conflicts. A write built from JOINs (an anti-join insert, or the delete-then-insert of the first SQLite design) checks first and writes later, so a concurrent call can insert the same key in between and make the write fail or lose data.
- **JOINs only count:** `UpsertExecutor.count_conflicts` joins the staging table to the target to return `(conflicting_rows, inserted_rows)`, and the MySQL unique key check joins them to find taken values. Neither writes.

The user asked about this while choosing MySQL's ignore statement; the answer is recorded here because it holds for every backend.
