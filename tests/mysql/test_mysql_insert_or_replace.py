# -*- coding: utf-8 -*-

"""
MySQL ``insert_or_replace`` against the shared behavior contract,
:mod:`sqlalchemy_upsert_kit.tests.contract`.
"""

from sqlalchemy_upsert_kit.mysql.insert_or_replace import insert_or_replace

from sqlalchemy_upsert_kit.tests.contract import StrategyContract


class TestInsertOrReplace(StrategyContract):
    strategy = "replace"
    func = staticmethod(insert_or_replace)


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.mysql.insert_or_replace",
        preview=False,
    )
