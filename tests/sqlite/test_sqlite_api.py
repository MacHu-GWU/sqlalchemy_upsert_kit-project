# -*- coding: utf-8 -*-

from sqlalchemy_upsert_kit.sqlite import api


def test():
    _ = api
    _ = api.insert_or_ignore
    _ = api.insert_or_replace
    _ = api.insert_or_merge


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.sqlite.api",
        preview=False,
    )
