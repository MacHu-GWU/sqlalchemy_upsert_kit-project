# -*- coding: utf-8 -*-

from sqlalchemy_upsert_kit.exc import UpsertTestError

import pytest


def test_upsert_test_error():
    with pytest.raises(UpsertTestError, match="boom"):
        raise UpsertTestError("boom")
    assert issubclass(UpsertTestError, Exception)


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.exc",
        preview=False,
    )
