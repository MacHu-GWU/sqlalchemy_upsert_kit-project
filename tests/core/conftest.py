# -*- coding: utf-8 -*-

import pytest


@pytest.fixture
def db_url(tmp_path) -> str:
    """
    A SQLite file of its own for each test.
    """
    return f"sqlite:///{tmp_path / 'test.sqlite'}"
