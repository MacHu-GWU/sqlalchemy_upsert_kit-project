# -*- coding: utf-8 -*-

"""
SQLite ``insert_or_merge`` against the shared behavior contract,
:mod:`sqlalchemy_upsert_kit.tests.contract`.
"""

from sqlalchemy_upsert_kit.sqlite.insert_or_merge import insert_or_merge

from sqlalchemy_upsert_kit.tests.contract import MergeContract


class TestInsertOrMerge(MergeContract):
    func = staticmethod(insert_or_merge)


if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.sqlite.insert_or_merge",
        preview=False,
    )
