# -*- coding: utf-8 -*-

"""
High-performance bulk upsert operations for SQLite.

Each operation loads the records into a ``TEMPORARY`` staging table, then
writes them to the target table with one native
``INSERT ... SELECT ... ON CONFLICT`` statement (SQLite 3.24+). The shared
workflow (transaction modes, SAVEPOINT, staging lifecycle, input validation)
lives in :mod:`sqlalchemy_upsert_kit.core`; the SQLite-specific parts are in
:mod:`sqlalchemy_upsert_kit.sqlite.executor`.

Import the functions from :mod:`sqlalchemy_upsert_kit.api` (``api.sqlite``).
This package imports nothing itself: importing SQLAlchemy while coverage starts
breaks SQLAlchemy 2.1 statement compilation in per-module coverage runs.
"""
