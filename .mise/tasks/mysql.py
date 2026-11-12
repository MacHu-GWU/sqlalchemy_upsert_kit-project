# -*- coding: utf-8 -*-

"""
Start or stop the local MySQL test container with Apple ``container``.

Usage::

    python .mise/tasks/mysql.py up    # mise run mysql-up
    python .mise/tasks/mysql.py down  # mise run mysql-down

Both commands are idempotent; see ``local_db.py`` for what each one does.
The settings are the ``MYSQL_*`` literals in ``tests/conftest.py``.
"""

from local_db import load_conftest_literals
from local_db import container_up
from local_db import container_down
from local_db import main


def mysql_up():
    conf = load_conftest_literals()
    container_up(
        label="MySQL",
        name=conf["MYSQL_CONTAINER_NAME"],
        image=conf["MYSQL_IMAGE"],
        host=conf["MYSQL_HOST"],
        port=conf["MYSQL_PORT"],
        container_port=3306,
        env={
            "MYSQL_ROOT_PASSWORD": conf["MYSQL_PASSWORD"],
            "MYSQL_DATABASE": conf["MYSQL_DATABASE"],
        },
        # Over TCP on purpose: during first-time initialization the image runs
        # a temporary server with networking off, so only the real one answers.
        ready_cmd=[
            "mysqladmin",
            "ping",
            "--host=127.0.0.1",
            "--protocol=TCP",
            f"--user={conf['MYSQL_USER']}",
            f"--password={conf['MYSQL_PASSWORD']}",
            "--silent",
        ],
    )


def mysql_down():
    conf = load_conftest_literals()
    container_down(conf["MYSQL_CONTAINER_NAME"])


if __name__ == "__main__":
    main("mysql.py", up=mysql_up, down=mysql_down)
