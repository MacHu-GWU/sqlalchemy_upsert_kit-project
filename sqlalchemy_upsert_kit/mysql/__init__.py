# -*- coding: utf-8 -*-

"""
High-performance bulk upsert operations for MySQL.

Each operation loads the records into a ``TEMPORARY`` staging table, then
writes them to the target table with one native
``INSERT ... SELECT ... ON DUPLICATE KEY UPDATE`` statement. The shared workflow
(transaction modes, SAVEPOINT, staging lifecycle, input validation) lives in
:mod:`sqlalchemy_upsert_kit.core`; the MySQL-specific parts are in
:mod:`sqlalchemy_upsert_kit.mysql.executor`.

Requires a MySQL driver, such as ``pip install "sqlalchemy_upsert_kit[mysql]"``
for PyMySQL.
"""
