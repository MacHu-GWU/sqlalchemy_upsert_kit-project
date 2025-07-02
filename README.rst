
.. image:: https://readthedocs.org/projects/sqlalchemy-upsert-kit/badge/?version=latest
    :target: https://sqlalchemy-upsert-kit.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://github.com/MacHu-GWU/sqlalchemy_upsert_kit-project/actions/workflows/main.yml/badge.svg
    :target: https://github.com/MacHu-GWU/sqlalchemy_upsert_kit-project/actions?query=workflow:CI

.. image:: https://codecov.io/gh/MacHu-GWU/sqlalchemy_upsert_kit-project/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/MacHu-GWU/sqlalchemy_upsert_kit-project

.. image:: https://img.shields.io/pypi/v/sqlalchemy-upsert-kit.svg
    :target: https://pypi.python.org/pypi/sqlalchemy-upsert-kit

.. image:: https://img.shields.io/pypi/l/sqlalchemy-upsert-kit.svg
    :target: https://pypi.python.org/pypi/sqlalchemy-upsert-kit

.. image:: https://img.shields.io/pypi/pyversions/sqlalchemy-upsert-kit.svg
    :target: https://pypi.python.org/pypi/sqlalchemy-upsert-kit

.. image:: https://img.shields.io/badge/✍️_Release_History!--None.svg?style=social&logo=github
    :target: https://github.com/MacHu-GWU/sqlalchemy_upsert_kit-project/blob/main/release-history.rst

.. image:: https://img.shields.io/badge/⭐_Star_me_on_GitHub!--None.svg?style=social&logo=github
    :target: https://github.com/MacHu-GWU/sqlalchemy_upsert_kit-project

------

.. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://sqlalchemy-upsert-kit.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/MacHu-GWU/sqlalchemy_upsert_kit-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/MacHu-GWU/sqlalchemy_upsert_kit-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/MacHu-GWU/sqlalchemy_upsert_kit-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/sqlalchemy-upsert-kit#files


Welcome to ``sqlalchemy_upsert_kit`` Documentation
==============================================================================
.. image:: https://sqlalchemy-upsert-kit.readthedocs.io/en/latest/_static/sqlalchemy_upsert_kit-logo.png
    :target: https://sqlalchemy-upsert-kit.readthedocs.io/en/latest/

``sqlalchemy_upsert_kit`` provides high-performance bulk upsert operations for SQLAlchemy applications using temporary table staging. It offers three optimized strategies: ``insert_or_ignore`` for conflict-free insertion, ``insert_or_replace`` for complete record replacement, and ``insert_or_merge`` for selective column updates while preserving existing data. Each strategy leverages efficient SQL JOIN operations and temporary tables to achieve ~20x performance improvements over traditional row-by-row upserts, making it ideal for ETL processes, data synchronization, and bulk data operations where speed and reliability are critical.


.. _install:

Install
------------------------------------------------------------------------------

``sqlalchemy_upsert_kit`` is released on PyPI, so all you need is to:

.. code-block:: console

    $ pip install sqlalchemy-upsert-kit

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade sqlalchemy-upsert-kit
