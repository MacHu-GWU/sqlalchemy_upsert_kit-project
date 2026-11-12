.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- (Planned for 0.3.1) Support tables with a composite primary key: conflicts are detected on all primary key columns together (``ON CONFLICT (a, b)``), so association tables of many-to-many relationships can be upserted too. Each record must have every primary key column, and no two records may share the same key combination.
- (Planned for 0.3.1) Add an optional ``conflict_columns`` parameter to all three functions, to detect conflicts on a unique constraint instead of the primary key, such as ``email`` or ``(source, external_id)`` on a table whose primary key is an auto-increment id. It defaults to the primary key columns, and the named columns must match a unique index or unique constraint of the table exactly.

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.3.1 (2026-11-12)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Add MySQL support (MySQL 8.0+), with the same three functions, signatures and results as SQLite and PostgreSQL:
    - ``sqlalchemy_upsert_kit.api.mysql``
    - ``sqlalchemy_upsert_kit.api.mysql.insert_or_ignore``
    - ``sqlalchemy_upsert_kit.api.mysql.insert_or_replace``
    - ``sqlalchemy_upsert_kit.api.mysql.insert_or_merge``
- Add the ``mysql`` extra, which installs the PyMySQL driver: ``pip install "sqlalchemy-upsert-kit[mysql]"``.
- On MySQL, a record that would break a unique key other than the primary key (the same unique value as an existing row, or as another record) raises ``sqlalchemy.exc.IntegrityError`` and changes nothing, as on SQLite and PostgreSQL. MySQL's own ``ON DUPLICATE KEY UPDATE`` would silently update that other row instead. The error's ``orig`` is the new ``sqlalchemy_upsert_kit.exc.UniqueKeyConflictError``. The unique keys declared in the ``Table`` are checked before the write, with a message that names the key; any other case (a unique index only the database has, or a row committed after your transaction's first read) is caught after the write, which is then undone.
- On MySQL, ``insert_or_ignore`` leaves conflicting rows unchanged with a no-op update, so their ``UPDATE`` triggers fire; it does not use ``INSERT IGNORE``, which would turn errors such as a too long value into warnings and write the row anyway.

**Minor Improvements**

- The rule that a conflict on any unique key other than the primary key is an error, and never an update, is now part of the behavior every backend is tested against, including composite unique keys, unique indexes, and NULL values.
- The staging table now copies the target table's character set and collation options, such as ``mysql_collate``, so staged values compare as in the target.
- When the cleanup after a failed call fails too, such as on a lost connection, the original error is raised, with the cleanup error added as a note.

**Miscellaneous**

- Add backend hooks to ``sqlalchemy_upsert_kit.core.executor.UpsertExecutor`` for how the staging table is dropped and cleaned up after a failure, how rows are counted, the identifier length limit, and checks of other unique keys before and after the upsert. SQLite and PostgreSQL keep their behavior.
- Run the MySQL tests locally in a container (``mise run mysql-up`` / ``mise run mysql-down``), which ``mise run test`` and ``mise run cov`` start along with PostgreSQL, and in CI against the MySQL on the GitHub-hosted runners (8.0 on Ubuntu, 8.4 on Windows).
- Add a MySQL docs page, and mention MySQL and the driver extras in the README and the other docs pages.
- Record MySQL's behavior, and why each part of the backend is built the way it is, in the ``maintain-sqlalchemy_upsert_kit`` maintenance skills.


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
