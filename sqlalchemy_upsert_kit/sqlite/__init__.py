# -*- coding: utf-8 -*-

"""
High-Performance SQLAlchemy Bulk Operations Module for Sqlite

This module provides optimized bulk operations for SQLAlchemy using temporary tables.
These methods are specifically designed for large datasets and offer
superior performance compared to traditional row-by-row operations.


Implementation Notes
------------------------------------------------------------------------------
**Temporary Table Strategy**:
    All bulk operations use temporary tables as staging areas to achieve optimal
    performance. Temporary tables are created with unique names to avoid conflicts
    in concurrent environments. Comprehensive cleanup ensures no temporary tables
    are left behind, even when errors occur.

**SQLite DDL Behavior**:
    SQLite DDL operations (CREATE/DROP TABLE) are not transactional and commit
    immediately. The cleanup logic accounts for this by using fresh connections
    when necessary to avoid database locks during error scenarios.

**Testing Infrastructure**:
    Functions include boolean parameters prefixed with ``_raise_on_`` that are
    exclusively for testing purposes. These parameters inject controlled failures
    at specific points in the operation flow to verify error handling and cleanup
    behavior. These parameters should never be used in production code.
"""

from .insert_or_ignore import insert_or_ignore
from .insert_or_replace import insert_or_replace

__all__ = ["insert_or_ignore", "insert_or_replace"]
