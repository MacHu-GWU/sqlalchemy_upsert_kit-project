# -*- coding: utf-8 -*-


class UpsertTestError(Exception):
    """
    Custom exception raised during testing to simulate failures.

    This exception is used exclusively for testing error handling and cleanup
    behavior in upsert operations. It allows tests to inject failures at specific
    points in the operation flow to verify proper rollback and cleanup.

    :param message: Descriptive error message indicating where the failure occurred
    """


class UniqueKeyConflictError(Exception):
    """
    The records would break a unique key other than the primary key: a new
    record has the same unique value as an existing row, or two records share
    one.

    A backend whose database would not report this itself (MySQL's
    ``ON DUPLICATE KEY UPDATE`` silently updates the other row instead) raises
    :class:`sqlalchemy.exc.IntegrityError` with this as its ``orig``, the same
    error type the other databases raise, after undoing the whole call.
    """
