.. _understanding-upsert-operations:

Understanding UPSERT Operations
==============================================================================
"Upsert" sounds like one operation: insert a record, or update it if it is already there. In practice it is three, because "update it" can mean three different things. This page explains the three strategies this library offers, how a call works, and the behavior that follows from it. For runnable examples, see :ref:`quick-start`.


The Three Strategies
------------------------------------------------------------------------------
Every strategy inserts the records whose primary key is new. They differ only in what they do to an existing row with the same primary key, a *conflicting* row.

Take a ``users`` table with three rows, and three records to upsert: users 2 and 3 conflict, user 4 is new.

.. list-table::
    :header-rows: 1

    * - Existing rows
      - Records
    * - ``(1, alice, alice@old.com)``
      -
    * - ``(2, bob, bob@old.com)``
      - ``(2, robert, bob@new.com)``
    * - ``(3, cathy, cathy@old.com)``
      - ``(3, catherine, cathy@new.com)``
    * -
      - ``(4, dave, dave@new.com)``

.. list-table::
    :header-rows: 1
    :widths: 20 30 50

    * - Strategy
      - Conflicting rows
      - Rows 2 and 3 afterwards
    * - :func:`insert_or_ignore <sqlalchemy_upsert_kit.sqlite.insert_or_ignore.insert_or_ignore>`
      - left unchanged
      - ``(2, bob, bob@old.com)``, ``(3, cathy, cathy@old.com)``
    * - :func:`insert_or_replace <sqlalchemy_upsert_kit.sqlite.insert_or_replace.insert_or_replace>`
      - overwritten to equal the record
      - ``(2, robert, bob@new.com)``, ``(3, catherine, cathy@new.com)``
    * - :func:`insert_or_merge <sqlalchemy_upsert_kit.sqlite.insert_or_merge.insert_or_merge>` with ``columns=["email"]``
      - only ``columns`` updated, the rest kept
      - ``(2, bob, bob@new.com)``, ``(3, cathy, cathy@new.com)``

In all three, user 4 is inserted in full, and the call returns ``(2, 1)``: two conflicting records, one inserted row.

When to use which:

- **Ignore** for append-only loads, and to make a load safe to repeat: rows already loaded are never touched.
- **Replace** to mirror a source of truth: after the call, every row equals its record.
- **Merge** to refresh some fields and keep others, such as a status column your application owns.

.. note::

    ``insert_or_replace`` makes the row equal the record, so a column the records leave out does **not** keep its old value: it is reset to its default, or NULL. To update only some columns, use ``insert_or_merge``.


How a Call Works
------------------------------------------------------------------------------
Upserting one row at a time costs one round trip and one statement per record. This library does the whole batch with a fixed number of statements, whatever its size:

1. **Validate** the records (see below). Invalid input raises ``ValueError`` before any SQL runs.
2. **Open a SAVEPOINT**, inside your transaction or one the call opens.
3. **Create a staging table**: a ``TEMPORARY`` table with the target's writable columns, private to the connection, with a unique random name.
4. **Bulk insert** all records into it, in one ``executemany``.
5. **Upsert** with one native statement:

   .. code-block:: sql

       INSERT INTO users (id, name, email)
       SELECT id, name, email FROM temp_1a2b3c4d_users
       ON CONFLICT (id) <action>

   where ``<action>`` is, per strategy:

   - ignore: ``DO NOTHING``
   - replace: ``DO UPDATE SET name = excluded.name, email = excluded.email`` (every non-key column of the records)
   - merge with ``columns=["email"]``: ``DO UPDATE SET email = excluded.email``

   MySQL has no ``ON CONFLICT``: its statement is ``INSERT ... SELECT ... ON DUPLICATE KEY UPDATE``, with the same result (see :ref:`mysql-backend`).

6. **Drop the staging table** and release the SAVEPOINT.

For a large batch this is typically 5-20x faster than upserting row by row, and the database does the conflict detection with its primary key index.

If any step fails, everything the call did is rolled back to the SAVEPOINT, the staging table is gone, and the exception propagates. Details, and runnable examples, are in :ref:`transactions-and-errors`.


What Follows From It
------------------------------------------------------------------------------
- **Rows are updated in place.** A conflicting row is never deleted and re-inserted. Update triggers fire; delete triggers and ``ON DELETE`` foreign key actions, such as ``CASCADE``, do not. (On MySQL, ``insert_or_ignore`` also fires the update triggers of conflicting rows, without changing them.)
- **Conflicts are on the primary key only.** A record that would break another unique key, such as a unique ``email`` that an existing row already has, raises ``sqlalchemy.exc.IntegrityError``, and the call changes nothing. It never updates that other row.
- **Defaults apply to new rows.** A column the records leave out gets its ``default`` or ``server_default`` on the rows the call inserts.
- **The counts add up.** The call returns ``(conflicting_rows, inserted_rows)``, and their sum is always the number of records.
- **Concurrent calls are safe.** Each call has its own staging table, so calls never collide.


Rules for the Input
------------------------------------------------------------------------------
- The table has exactly one primary key column.
- Records are dicts keyed by column key (the ``key`` of ``sa.Column``, which is its name unless set otherwise).
- Every record has the same keys, including the primary key, which is never ``None`` and never repeated.
- Generated (``sa.Computed``) columns cannot be written.
- For merge, ``columns`` is not empty, and each one is a key of the records.

Input that breaks a rule raises ``ValueError`` and writes nothing. Empty input writes nothing and returns ``(0, 0)``.


Database Support
------------------------------------------------------------------------------
Each database has its own module with the same three functions, signatures and results:

.. list-table::
    :header-rows: 1
    :widths: 20 30 50

    * - Database
      - Module
      - Requirements
    * - SQLite
      - ``sqlalchemy_upsert_kit.api.sqlite``
      - SQLite 3.24+, the standard ``sqlite3`` driver. See :ref:`sqlite-backend`.
    * - PostgreSQL
      - ``sqlalchemy_upsert_kit.api.postgres``
      - psycopg 3, installed by the ``postgres`` extra. See :ref:`postgresql-backend`.
    * - MySQL
      - ``sqlalchemy_upsert_kit.api.mysql``
      - MySQL 8.0+, PyMySQL, installed by the ``mysql`` extra. See :ref:`mysql-backend`.

All need SQLAlchemy 2.0.28 or later, and a synchronous engine. MariaDB is not supported yet.
