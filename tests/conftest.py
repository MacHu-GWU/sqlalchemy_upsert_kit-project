# -*- coding: utf-8 -*-

import sys
import pytest


@pytest.fixture(autouse=True)
def ensure_newline_before_test_output():
    sys.stdout.write("\n")
    sys.stdout.flush()
