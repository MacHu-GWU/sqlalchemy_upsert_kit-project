# -*- coding: utf-8 -*-

"""
Start or stop the local PostgreSQL test container with Apple ``container``.

Usage::

    python .mise/tasks/psql.py up    # mise run psql-up
    python .mise/tasks/psql.py down  # mise run psql-down

Both commands are idempotent; see ``local_db.py`` for what each one does.
The settings are the ``PG_*`` literals in ``tests/conftest.py``.
"""

from local_db import load_conftest_literals
from local_db import container_up
from local_db import container_down
from local_db import main


def psql_up():
    conf = load_conftest_literals()
    container_up(
        label="PostgreSQL",
        name=conf["PG_CONTAINER_NAME"],
        image=conf["PG_IMAGE"],
        host=conf["PG_HOST"],
        port=conf["PG_PORT"],
        container_port=5432,
        env={
            "POSTGRES_USER": conf["PG_USER"],
            "POSTGRES_PASSWORD": conf["PG_PASSWORD"],
            "POSTGRES_DB": conf["PG_DATABASE"],
        },
        ready_cmd=[
            "pg_isready",
            "--host",
            "127.0.0.1",
            "--username",
            conf["PG_USER"],
        ],
    )


def psql_down():
    conf = load_conftest_literals()
    container_down(conf["PG_CONTAINER_NAME"])


if __name__ == "__main__":
    main("psql.py", up=psql_up, down=psql_down)
