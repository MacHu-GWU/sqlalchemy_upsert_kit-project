.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.2.1 (2026-11-11)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Breaking Changes**

- Drop Python 3.9 and 3.10: ``requires-python`` is now ``>=3.11``.
- Invalid records now raise ``ValueError`` before any SQL runs, instead of being written with silent data loss: a key that is not a writable column of the table (including a generated column), a missing, ``None`` or repeated primary key, and records with different keys. ``insert_or_merge`` also raises ``ValueError`` when ``columns`` is empty, names an unknown column, or names a column the records do not have, and every function raises it when ``temp_table_name`` equals the target table's name.
- SQLite ``insert_or_replace`` and ``insert_or_merge`` now update conflicting rows in place instead of deleting and re-inserting them: update triggers fire, while delete triggers and ``ON DELETE`` foreign key actions (such as ``CASCADE``) no longer do.
- ``from sqlalchemy_upsert_kit.sqlite import insert_or_ignore`` (and ``insert_or_replace``) no longer works: the ``sqlite`` package no longer re-exports the functions. Import them from ``sqlalchemy_upsert_kit.api`` (``api.sqlite``), the public API.

**Features and Improvements**

- Add PostgreSQL support, with the same three functions and signatures as SQLite:
    - ``sqlalchemy_upsert_kit.api.postgres``
    - ``sqlalchemy_upsert_kit.api.postgres.insert_or_ignore``
    - ``sqlalchemy_upsert_kit.api.postgres.insert_or_replace``
    - ``sqlalchemy_upsert_kit.api.postgres.insert_or_merge``
- Add the ``postgres`` extra, which installs the psycopg 3 driver: ``pip install "sqlalchemy_upsert_kit[postgres]"``.

**Minor Improvements**

- All backends write with one native ``INSERT ... SELECT ... ON CONFLICT`` statement, so the same input gives the same rows and counts on SQLite and PostgreSQL.
- In user-managed mode, a failed call now undoes only its own work: the caller's transaction keeps its earlier writes and stays usable, so the caller can retry and commit.
- The staging table is now a ``TEMPORARY`` table with a random name, private to its connection, so concurrent calls and retries never collide on its name. Concurrent SQLite calls wait for each other's write lock instead of failing with "database is locked".
- Raise the SQLAlchemy floor to ``>=2.0.28``, for the ``preserve_rowcount`` execution option that the row counts rely on.

**Bugfixes**

- Fix SQLite overwriting an existing row when a record had no primary key.
- Fix SQLite failing on target tables with a foreign key or a named index.
- Fix a column being dropped from every record when the first record did not have it.
- Fix tables whose column key differs from the column name, and tables with generated (``Computed``) columns.
- Fix SQLite leaving the staging table behind, after waiting about 5 seconds, when a call failed in user-managed mode.
- Fix a user-managed SQLite call being committed by its own ``SAVEPOINT`` release when the caller had not written anything yet, so a later rollback by the caller did not undo it.
- Fix staging table names colliding when two calls ran in the same second, and exceeding PostgreSQL's 63-character limit for target tables with long names.

**Miscellaneous**

- Move the shared workflow (input validation, staging table, transaction handling) to ``sqlalchemy_upsert_kit.core``; each backend now only defines its upsert statement and how a transaction begins.
- Replace the private ``_raise_on_*`` test parameters with one private ``_fail_at`` parameter.
- Rewrite the tests as one behavior contract suite that every backend runs, including a 10,000-row case and concurrent calls.
- Run the PostgreSQL tests locally in a container (``mise run psql-up`` / ``mise run psql-down``, Apple ``container`` installed by mise), and in CI against the PostgreSQL preinstalled on the GitHub-hosted runners.
- Switch the build to setuptools and the project tooling to mise and uv, from the pywf open source template.
- Add the ``maintain-sqlalchemy_upsert_kit`` maintenance skills for agents.
- Add the ``pypi-sqlalchemy_upsert_kit`` Agent Skill, which documents the public API for AI coding agents.
- Rewrite the user docs as runnable notebooks: Quick Start, Transactions and Errors, SQLite, and PostgreSQL pages, plus an updated Understanding UPSERT Operations page.


0.1.1 (2025-06-02)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- First release
- Add the following public APIs:
    - ``sqlalchemy_upsert_kit.api``
    - ``sqlalchemy_upsert_kit.api.sqlite``
    - ``sqlalchemy_upsert_kit.api.sqlite.insert_or_ignore``
    - ``sqlalchemy_upsert_kit.api.sqlite.insert_or_replace``
    - ``sqlalchemy_upsert_kit.api.sqlite.insert_or_merge``
