# -*- coding: utf-8 -*-

from sqlalchemy_upsert_kit import api


def test():
    _ = api
    _ = api.sqlite
    _ = api.sqlite.insert_or_ignore
    _ = api.sqlite.insert_or_replace
    _ = api.sqlite.insert_or_merge
    _ = api.postgres
    _ = api.postgres.insert_or_ignore
    _ = api.postgres.insert_or_replace
    _ = api.postgres.insert_or_merge
    _ = api.mysql
    _ = api.mysql.insert_or_ignore
    _ = api.mysql.insert_or_replace
    _ = api.mysql.insert_or_merge


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.api",
        preview=False,
    )
