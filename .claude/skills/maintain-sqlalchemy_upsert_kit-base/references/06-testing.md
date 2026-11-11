---
description: "How the strategies are tested: the per-backend test folders and their fixtures, the shared DataFaker helpers, the three scenarios every strategy test file covers, and, per backend, how its database is provided locally and in CI (SQLite: a file; PostgreSQL: psql-up locally, the runner's preinstalled server in CI). Read before writing or changing a backend's tests, or when a backend's tests fail locally or in CI."
---

# Testing the Backends

The template's test rules (path mapping, `all.py`, `run_cov_test`, 100% per module) are in `pywf-oss-base`, in its Test Layout, Writing Tests, and Coverage references. This file covers what is specific to this library.

---

## 1. Layout

| Folder | Tests |
| :--- | :--- |
| `tests/core/` | `UpsertExecutor` itself, driven by a minimal strategy (`CopyAllExecutor`, defined in the test file) on a SQLite file |
| `tests/sqlite/` | the SQLite strategies |
| `tests/postgres/` | the PostgreSQL executor and strategies |

- The PostgreSQL tests talk to a real server, but they live in `tests/`, not `tests_int/`: the user chose to run them in every local test run and in CI, with the server provided as in section 4.
- Each backend folder has a `conftest.py` with the same three fixtures: `data_faker` (4 existing rows, 3 input rows, 2 of them conflicting), `clean_database` (empty tables plus the existing rows, yielding the engine), and `error_scenarios` (the `_raise_on_*` flags that backend supports, each with a staging table name).

---

## 2. Shared helpers: `sqlalchemy_upsert_kit/tests/data.py`

- `Record` / `t_record`: the test table (`id` primary key, `desc`, `create_at`, `update_at`).
- `DataFaker`: builds the existing rows and the shuffled input rows, and checks the outcome: `check_all_data`, `check_conflict_data`, `check_incremental_data` (happy path), `check_rollback` (sad path), and `check_no_temp_tables` (both).
- `check_no_temp_tables` has a branch per dialect, because a PostgreSQL temporary table is invisible to reflection (it queries `pg_class`). A new backend whose staging table hides from reflection adds its own branch.

---

## 3. What every strategy test file covers

Each `test_<engine>_insert_or_<strategy>.py` has the same three tests:

1. `test_success`: the returned counts, the row count, the conflicting rows (merge: `desc` kept, `update_at` updated), the new rows, and no staging table left.
2. `test_rollback_with_auto_managed_transaction`: for each flag in `error_scenarios`, the call raises `UpsertTestError`, no staging table is left, and the data is unchanged.
3. `test_rollback_with_user_managed_transaction`: the same inside a caller's transaction that writes rows before and after the call, plus `_raise_on_post_operation`, an error raised by the caller after a successful upsert.

The PostgreSQL files were copied from the SQLite ones. When a third backend arrives, consider moving these three scenarios into one shared suite that each backend runs, so the contract in `01-scope.md` is tested once, the same way everywhere.

---

## 4. Per backend: local and CI

### SQLite

- **Local and CI:** nothing to start. `tests/sqlite/conftest.py` recreates the file `test.sqlite` under `path_enum.dir_tmp` for each test; `tests/core/` uses pytest's `tmp_path`.

### PostgreSQL

- **Local:** the container from `mise run psql-up` (`05-local-db-tasks.md`), which `mise run test` and `mise run cov` start first. Running `.venv/bin/pytest` or a test file directly needs it running already; without it, the `pg_engine` fixture fails with a message that says to run `mise run psql-up`.
- **Connection URL:** the session fixture `pg_url` in `tests/conftest.py` returns the environment variable `SQLALCHEMY_UPSERT_KIT_PG_URL` if set, and otherwise `PG_URL`, built from the container settings.
- **`pg_engine`** (`tests/postgres/conftest.py`) sets the session time zone to UTC, so the timezone-aware test datetimes round-trip through the naive `timestamp` columns the same way on every machine.
- **CI** (`.github/workflows/main.yml`, matrix `ubuntu-latest` and `windows-latest`): it uses the PostgreSQL preinstalled, and stopped, on the GitHub-hosted runner image (`actions/runner-images`), so no image is pulled from Docker Hub and no third-party action is needed. Service containers were rejected: they run on Linux runners only, and pull from Docker Hub.
  - Ubuntu: `systemctl start postgresql.service`, set the `postgres` password to `password`, export the URL on port 5432.
  - Windows: start the one `postgresql*` service (user `postgres`, password `root`, from the image's software list), export the URL on port 5432.
- **Status:** these CI steps were written from the runner images' published software lists. Their first run, on commit `3516858`, had not finished when this was written; if they fail, fix them with `pywf-oss-fix-ci` and update this section.
