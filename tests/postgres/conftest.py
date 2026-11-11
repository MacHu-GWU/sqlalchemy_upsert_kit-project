# -*- coding: utf-8 -*-

import pytest
import sqlalchemy as sa


@pytest.fixture(scope="session")
def db_url(pg_url) -> str:
    """
    The PostgreSQL test database, checked once per session.
    """
    engine = sa.create_engine(pg_url)
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
    except sa.exc.OperationalError as e:  # pragma: no cover
        pytest.fail(
            f"Cannot connect to the PostgreSQL test database at {engine.url!r}. "
            f"Run 'mise run psql-up' first.\n{e}"
        )
    finally:
        engine.dispose()
    return pg_url
