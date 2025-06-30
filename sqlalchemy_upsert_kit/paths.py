# -*- coding: utf-8 -*-

from pathlib import Path

dir_here = Path(__file__).absolute().parent
dir_package = dir_here
PACKAGE_NAME = dir_package.name

dir_project_root = dir_package.parent

# ------------------------------------------------------------------------------
# Virtual Environment Related
# ------------------------------------------------------------------------------
dir_venv = dir_project_root / ".venv"
dir_venv_bin = dir_venv / "bin"

# virtualenv executable paths
bin_pytest = dir_venv_bin / "pytest"

# ------------------------------------------------------------------------------
# Test Related
# ------------------------------------------------------------------------------
dir_htmlcov = dir_project_root / "htmlcov"
path_cov_index_html = dir_htmlcov / "index.html"
dir_unit_test = dir_project_root / "tests"
dir_int_test = dir_project_root / "tests_int"
dir_load_test = dir_project_root / "tests_load"

# ------------------------------------------------------------------------------
# Doc Related
# ------------------------------------------------------------------------------
dir_docs_source = dir_project_root / "docs" / "source"
dir_docs_build_html = dir_project_root / "docs" / "build" / "html"
