---
description: "How the library is tested: the shared behavior contract suite that every backend runs (StrategyContract, MergeContract), the engine and db_url fixtures and why the pool has one connection, how staging leftovers are found on each database, what each test folder holds, the large-data size, how to check that a test catches a bug, and, per backend, how its database is provided locally and in CI. Read before writing or changing tests, or when a backend's tests fail locally or in CI."
---

# Testing the Backends

The template's test rules (path mapping, `all.py`, `run_cov_test`, 100% per module) are in `pywf-oss-base`, in its Test Layout, Writing Tests, and Coverage references. This file covers what is specific to this library.

---

## 1. The contract suite: `sqlalchemy_upsert_kit/tests/contract.py`

- **One suite, run by every backend.** `StrategyContract` holds the tests every strategy must pass; `MergeContract` adds the checks of merge's `columns`. A backend's strategy test file is only a subclass that sets `strategy` and `func = staticmethod(insert_or_<strategy>)`; pytest collects the inherited tests there, so each module still reaches 100% from its own test file.
- **Expected results are literals** (`EXPECTED_ROWS`, the rows in `test_rich_table`), never computed from the input, so a backend that returns different rows or counts fails.
- **Its tables** each target a trap: `t_record` (the main scenario), `t_rich` (key differs from name, reserved word, mixed case, server and Python defaults, a generated column, a unique constraint, a named index), `t_parent` / `t_child` (`ON DELETE CASCADE`), `t_pk_only`, and `t_long` (a 61-character name). `reset()` recreates them and loads `SEEDS`.
- **What it covers:** the main scenario in both modes, empty input, two calls in one transaction, the caller's rollback undoing a call, a failure at every step of `FAIL_STEPS` in auto mode and in user mode (with and without a prior write by the caller, then a retry and commit), every invalid input, the explicit `temp_table_name` and `metadata` arguments, the trap tables, conflicts on a unique key other than the primary key (`test_unique_key_*`, on `t_rich.label`: each must raise `IntegrityError` and change nothing, unless the strategy does not write the key), `LARGE_N` rows, and four concurrent calls.
- **Portable SQL only** in the contract's own queries: MySQL has no `count(*) FILTER (...)`, so `test_large` counts with `count(CASE ...)`.
- **Staging leftovers** are checked with `staging_tables(conn)`, which reads the connection's own temporary tables (`sqlite_temp_master`, `pg_my_temp_schema()`) and any permanent `temp_%` table. A new backend adds its branch there.
  - MySQL cannot list one connection's temporary tables. Its branch reads `information_schema.INNODB_TEMP_TABLE_INFO`, which lists **every** connection's, under internal names (`#sql...`). That works because the tests run one at a time and close their connections; a second process using the same test database at the same time would make the check fail.

A new scenario that every backend must meet goes into the contract, not into one backend's files.

---

## 2. Fixtures

- **`engine`** (`tests/conftest.py`): a fresh contract database (`reset`) for each test, with **a pool of exactly one connection**. After an auto-managed call, `assert_no_staging(engine)` then inspects the very connection the call used: a leaked temporary table is private to its connection, and would be invisible from any other. On SQLite it also turns on `PRAGMA foreign_keys`, so a strategy that deleted a parent row would cascade and fail `test_foreign_keys`.
- **`db_url`**: each test folder's `conftest.py` says which database `engine` uses. `tests/sqlite/` and `tests/core/`: a SQLite file under pytest's `tmp_path`. `tests/postgres/`: `pg_url`, and `tests/mysql/`: `mysql_url`, each checked once per session.
- **`pg_url`** and **`mysql_url`** (`tests/conftest.py`): the environment variable `SQLALCHEMY_UPSERT_KIT_PG_URL` or `SQLALCHEMY_UPSERT_KIT_MYSQL_URL` if set, otherwise `PG_URL` or `MYSQL_URL`, built from the container settings.

---

## 3. Test folders

| Folder | Tests |
| :--- | :--- |
| `tests/core/` | `schema.py` and `validation.py` directly; `UpsertExecutor` through `SqliteUpsertExecutor` (defaults, both modes, every failure step, argument errors) |
| `tests/sqlite/`, `tests/postgres/`, `tests/mysql/` | the backend's executor hooks (`build_upsert` SQL; `begin_transaction` on SQLite; on MySQL every overridden hook on its own, against the database, so `mysql/executor.py` reaches 100% from its own test file), and the contract through its three strategy files |

The PostgreSQL tests talk to a real server, but live in `tests/`, not `tests_int/`: the user chose to run them in every local test run and in CI.

---

## 4. Size and speed

- `LARGE_N = 10_000`: 10,000 existing rows and 10,000 records, half conflicting. Measured locally at about 0.17 s per strategy on PostgreSQL and 0.03 s on SQLite; a review measured 20,000 at about 1 s for the three PostgreSQL strategies together, and named it the ceiling. The whole suite runs in about 5 seconds.
- Keep it that way: no test that generates more data, sleeps, or waits on a timeout.

---

## 5. Check that a test catches the bug

A green suite is not proof. After fixing a bug, put the bug back for a moment and confirm a test fails, then restore the fix. Done for the 0.2 refactor: without `BEGIN IMMEDIATE`, `test_caller_rollback_undoes_the_upsert[False]`, `test_begin_transaction[False]` and `test_concurrent_calls` fail; without the SAVEPOINT, 36 failure tests fail.

Done for the MySQL backend, one hook at a time, on `tests/mysql/`: a plain `DROP TABLE` fails 69 tests; no `cleanup_after_rollback`, 37 (every failure test and the unique key tests); trusting the row count, 68; the dialect's identifier limit, 4 (`test_long_table_name`, `test_max_identifier_length`); no unique key check, 9; a shared value check that skips conflicting records which write the key, 1 (`test_unique_key_shared_with_conflicting_record`, added for it).

---

## 6. Per backend: local and CI

### SQLite

- **Local and CI:** nothing to start; each test gets its own file.

### PostgreSQL

- **Local:** the container from `mise run psql-up` (`06-local-db-tasks.md`), which `mise run test` and `mise run cov` start first. Running `.venv/bin/pytest` or a test file directly needs it running already; otherwise `db_url` fails with a message that says to run `mise run psql-up`.
- **CI** (`.github/workflows/main.yml`, matrix `ubuntu-latest` and `windows-latest`): the PostgreSQL preinstalled, and stopped, on the GitHub-hosted runner image (`actions/runner-images`), so no image is pulled from Docker Hub and no third-party action is needed. Service containers were rejected: they run on Linux runners only, and pull from Docker Hub.
  - Ubuntu: `systemctl start postgresql.service`, set the `postgres` password to `password`, export the URL on port 5432.
  - Windows: start the one `postgresql*` service (user `postgres`, password `root`, from the image's software list), export the URL on port 5432.
- **Status:** these steps pass on both runner OSes (CI of commit `92d38f3`). The run of commit `3516858`, which first added them, never left the queue.

### MySQL

- **Local:** the container from `mise run mysql-up` (`06-local-db-tasks.md`), which `mise run test` and `mise run cov` start first. Without it, `db_url` fails with a message that says to run `mise run mysql-up`.
- **CI:** the MySQL on the runner image, again with no image pulled.
  - Ubuntu: MySQL 8.0, disabled. `systemctl start mysql.service`, create the database `test`, export the URL (user `root`, password `root`, port 3306).
  - Windows: the image installs the MySQL 8.4 server (on `PATH`) with no service and no data directory. The step runs `mysqld --initialize-insecure` (a passwordless `root`) into `$RUNNER_TEMP`, starts `mysqld` in the background, creates `test`, and exports the URL.
- **Status:** these steps pass on both runner OSes, on every Python of the matrix (CI of commit `c9c6dfc`: 426 tests, about 37 s on Ubuntu and 2 minutes on Windows), and PyMySQL authenticates without its `rsa` extra. If a later image breaks authentication (MySQL 8's `caching_sha2_password` over a connection without TLS needs RSA), add `PyMySQL[rsa]` to the `test` extra. Fix any failure with `pywf-oss-fix-ci`.
- The CI log's warnings (about 120, all `SAWarning: Computed column RichItems.total is being created as 'STORED'`) come from the runners' PostgreSQL 16 and 17, which support only stored generated columns (virtual ones arrived in PostgreSQL 18, the local version). They are expected.
