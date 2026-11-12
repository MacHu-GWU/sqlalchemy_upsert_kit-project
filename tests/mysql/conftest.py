# -*- coding: utf-8 -*-

import pytest
import sqlalchemy as sa


@pytest.fixture(scope="session")
def db_url(mysql_url) -> str:
    """
    The MySQL test database, checked once per session.
    """
    engine = sa.create_engine(mysql_url)
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
    except sa.exc.OperationalError as e:  # pragma: no cover
        pytest.fail(
            f"Cannot connect to the MySQL test database at {engine.url!r}. "
            f"Run 'mise run mysql-up' first.\n{e}"
        )
    finally:
        engine.dispose()
    return mysql_url
