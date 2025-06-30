# -*- coding: utf-8 -*-

from sqlalchemy_upsert_kit import api


def test():
    _ = api


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.api",
        preview=False,
    )
