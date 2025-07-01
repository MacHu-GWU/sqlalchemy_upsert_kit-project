# -*- coding: utf-8 -*-

if __name__ == "__main__":
    from sqlalchemy_upsert_kit.tests import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_upsert_kit.sqlite",
        is_folder=True,
        preview=False,
    )
