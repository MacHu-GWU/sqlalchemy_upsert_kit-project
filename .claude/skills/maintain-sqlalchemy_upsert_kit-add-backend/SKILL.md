---
name: maintain-sqlalchemy_upsert_kit-add-backend
description: "Add a new database backend (such as MySQL) to sqlalchemy_upsert_kit: a sqlalchemy_upsert_kit/<engine>/ subpackage with the three upsert strategies, its driver extra, its local test container and mise tasks, its tests, its CI database, and its backend reference in the maintenance skill family. Use when the user asks to support another database."
---

Add one database backend end to end, keeping the strategy contract that every existing backend keeps.

Read ../maintain-sqlalchemy_upsert_kit-base/SKILL.md, then:
- ../maintain-sqlalchemy_upsert_kit-base/references/01-scope.md
- ../maintain-sqlalchemy_upsert_kit-base/references/02-architecture.md
- ../maintain-sqlalchemy_upsert_kit-base/references/04-backend-postgres.md (the latest backend, as the model)
- ../maintain-sqlalchemy_upsert_kit-base/references/05-local-db-tasks.md
- ../maintain-sqlalchemy_upsert_kit-base/references/06-testing.md

## Steps

1. **Pin down the backend, and stop for the user's yes.** Propose, one line each:
   - the subpackage name `<engine>` (`sqlalchemy_upsert_kit/<engine>/`, `api.<engine>`, `tests/<engine>/`, the extra) and the mise task short name;
   - the driver and its version constraint (load `pywf-oss-base`, then read its uv and Dependencies reference);
   - the official image for the local container, and its unusual port (never the database's default port);
   - how CI gets a server on each runner OS in the matrix, from the runner images' software list. If an OS has none, propose testing that backend on the other OS only.
2. **Probe the dialect before writing code.** Start the container (step 6 can come first), and check against a throwaway table:
   - the native upsert from a `SELECT` (`ON CONFLICT`, `ON DUPLICATE KEY UPDATE`, `MERGE`), and what its row count counts for inserted, updated, and ignored rows;
   - whether `Connection.begin()` opens a real transaction (the SQLite driver's does not, `03-backend-sqlite.md`), and whether `CREATE TEMPORARY TABLE` commits it;
   - that a SAVEPOINT rollback removes a temporary table, and leaves the transaction usable;
   - anything the shared staging table or validation in `sqlalchemy_upsert_kit/core/` cannot do on this database (identifier length, a temporary table referenced twice in one statement).

   Report the findings to the user before step 4. They become the backend reference in step 9, and decide which hooks step 4 overrides.
3. **Scaffold** the subpackage with `pywf-oss-add-module`: `__init__.py` with a module docstring and **no imports** (`02-architecture.md` section 1), `api.py`, `executor.py`, and the three public function modules, modeled on `sqlalchemy_upsert_kit/postgres/`. Add `from .<engine> import api as <engine>` to `sqlalchemy_upsert_kit/api.py`, and touch the new names in `tests/test_api.py`.
4. **Implement** the backend's `UpsertExecutor` subclass: `build_upsert()` always, and `begin_transaction()` or any other hook only if the probe showed it must differ (`02-architecture.md` section 2). The public function modules keep the signatures of the other backends. If the probe found that something in `core/` is wrong for this database, make it a hook instead of a branch on the dialect.
5. **Dependencies:** add the `<engine>` extra and the same driver in the `test` extra (`02-architecture.md` section 3), then `mise run inst` and `mise run export`.
6. **Local database:** add the `<ENGINE>_*` literal settings to `tests/conftest.py`, a session fixture for the URL that reads `SQLALCHEMY_UPSERT_KIT_<ENGINE>_URL` first, the script `.mise/tasks/<engine>.py` modeled on `.mise/tasks/psql.py`, and the `<engine>-up` / `<engine>-down` tasks in `mise.toml`, with `test` and `cov` depending on `<engine>-up`. Check that each task is idempotent by running it twice (`05-local-db-tasks.md` section 3).
7. **Tests:** create `tests/<engine>/` with `all.py`, a `conftest.py` with the `db_url` fixture, a test file for the executor's hooks, and the three strategy test files, each a subclass of the contract (`06-testing.md` section 1). Add the backend's branch to `staging_tables()` in `sqlalchemy_upsert_kit/tests/contract.py`. A scenario only this database needs goes into its own test file; one every backend needs goes into the contract. Reach 100% with `pywf-oss-write-test`, and check that the tests catch the bugs the probe found (`06-testing.md` section 5).
8. **CI:** add the steps that start the runner's server and export `SQLALCHEMY_UPSERT_KIT_<ENGINE>_URL` to `.github/workflows/main.yml`. Pushing runs CI: push only after the user's yes, then check the run, and fix failures with `pywf-oss-fix-ci`.
9. **Maintenance family,** in the same change (load `pywf-oss-write-maintain-skill`):
   - add `NN-backend-<engine>.md` after the last backend reference, modeled on `04-backend-postgres.md`: its transaction behavior, upsert statement and counts, and what is untested, never the implementation. Renumber the later references and every citation of them;
   - update `02-architecture.md` (layout, and any new reason for the per-backend split), `05-local-db-tasks.md` (the tasks table and settings), `06-testing.md` (a section for the backend), and the base `SKILL.md` paragraph;
   - in this skill's Read list, point to the new backend reference instead of `04-backend-postgres.md` if it becomes the better model.
10. **Finish:** `mise run format`, `mise run lint`, `mise run cov`, and `mise run build-doc` pass. Update the "Backends" line in `CLAUDE.md`. Name the next steps for the user: the changelog (`pywf-oss-changelog`) and a user docs page (`pywf-oss-write-doc`).

## Output

The files added and changed, the dialect findings from step 2, the local and CI test results, and anything left unverified.
