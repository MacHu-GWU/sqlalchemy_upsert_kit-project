# -*- coding: utf-8 -*-

"""
High-performance bulk upsert operations for PostgreSQL.

Each operation loads the records into a ``TEMPORARY`` staging table, then
writes them to the target table with one native
``INSERT ... SELECT ... ON CONFLICT`` statement. The shared workflow
(transaction modes, SAVEPOINT, staging lifecycle, input validation) lives in
:mod:`sqlalchemy_upsert_kit.core`; the PostgreSQL-specific parts are in
:mod:`sqlalchemy_upsert_kit.postgres.executor`.

Requires a PostgreSQL driver, such as ``pip install "sqlalchemy_upsert_kit[postgres]"``
for psycopg 3.
"""
