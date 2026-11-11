# -*- coding: utf-8 -*-

"""
SQLite ``insert_or_ignore`` against the shared behavior contract,
:mod:`sqlalchemy_upsert_kit.tests.contract`.
"""

from sqlalchemy_upsert_kit.sqlite.insert_or_ignore import insert_or_ignore

from sqlalchemy_upsert_kit.tests.contract import StrategyContract


class TestInsertOrIgnore(StrategyContract):
    strategy = "ignore"
    func = staticmethod(insert_or_ignore)


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.sqlite.insert_or_ignore",
        preview=False,
    )
