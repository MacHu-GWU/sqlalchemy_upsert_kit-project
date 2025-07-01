# -*- coding: utf-8 -*-


class UpsertTestError(Exception):
    """
    Custom exception raised during testing to simulate failures.

    This exception is used exclusively for testing error handling and cleanup
    behavior in upsert operations. It allows tests to inject failures at specific
    points in the operation flow to verify proper rollback and cleanup.

    :param message: Descriptive error message indicating where the failure occurred
    """
