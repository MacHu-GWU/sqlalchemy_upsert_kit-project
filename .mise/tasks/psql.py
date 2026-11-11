# -*- coding: utf-8 -*-

"""
Start or stop the local PostgreSQL test container with Apple ``container``.

Usage::

    python .mise/tasks/psql.py up    # mise run psql-up
    python .mise/tasks/psql.py down  # mise run psql-down

Both commands are idempotent:

- ``up`` starts the ``container`` system services if they are not running,
  reuses the test container if it is already running, otherwise starts a new
  one with ``--rm``, then waits until PostgreSQL accepts TCP connections.
- ``down`` stops the test container if it exists. Because it runs with
  ``--rm``, stopping it also deletes it. The system services keep running.

The image, container name, host, port and credentials are read from the
literal assignments in ``tests/conftest.py``, the single place they are defined.
The script uses only the standard library, so it runs without ``.venv``.
"""

import ast
import json
import sys
import time
import shutil
import subprocess
from pathlib import Path

dir_project_root = Path(__file__).absolute().parent.parent.parent
path_conftest = dir_project_root / "tests" / "conftest.py"

READY_TIMEOUT = 60  # seconds to wait for PostgreSQL to accept connections


def load_conftest_literals() -> dict:
    """
    Read every ``NAME = <literal>`` assignment at the top of ``tests/conftest.py``.
    """
    tree = ast.parse(path_conftest.read_text(encoding="utf-8"))
    values = {}
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            try:
                values[node.targets[0].id] = ast.literal_eval(node.value)
            except ValueError:
                pass  # not a literal, such as an f-string
    return values


def find_container_bin() -> Path:
    """
    Find the ``container`` executable that mise installed from ``[tools]``.
    """
    path = shutil.which("container")
    if path is None:
        print(
            "Error: 'container' not found. It is installed by mise on macOS "
            "(Apple silicon, macOS 26+): run 'mise install' in the project root."
        )
        sys.exit(1)
    return Path(path).resolve()


class Container:
    def __init__(self, bin_path: Path):
        self.bin_path = bin_path
        # mise unpacks the .pkg payload: <install>/Payload/bin/container,
        # so the install root that holds bin/ and libexec/ is two levels up.
        self.install_root = bin_path.parent.parent

    def run(
        self,
        *args: str,
        check: bool = True,
        capture: bool = True,
    ) -> subprocess.CompletedProcess:
        return subprocess.run(
            [str(self.bin_path), *args],
            check=check,
            capture_output=capture,
            text=True,
        )

    def is_system_running(self) -> bool:
        res = self.run("system", "status", check=False)
        return res.returncode == 0

    def ensure_system_running(self):
        if self.is_system_running():
            return
        print("Starting container system services ...")
        self.run(
            "system",
            "start",
            "--install-root",
            str(self.install_root),
            "--enable-kernel-install",
            capture=False,
        )

    def get_status(self, name: str) -> str | None:
        """
        Return the status of the named container (such as ``running``),
        or None if it does not exist.
        """
        res = self.run("list", "--all", "--format", "json")
        for item in json.loads(res.stdout or "[]"):
            config = item.get("configuration", {})
            if name in (config.get("id"), item.get("id")):
                # container 1.7: "status": {"state": "running", "networks": [...], ...}
                status = item.get("status", {})
                if isinstance(status, dict):
                    status = status.get("state", "unknown")
                return str(status).lower()
        return None


def psql_up():
    conf = load_conftest_literals()
    name = conf["PG_CONTAINER_NAME"]
    ctn = Container(find_container_bin())
    ctn.ensure_system_running()

    status = ctn.get_status(name)
    if status == "running":
        print(f"Container {name!r} is already running.")
    else:
        if status is not None:
            # A leftover container that is not running: remove it and start fresh.
            ctn.run("delete", "--force", name, check=False)
        print(f"Starting container {name!r} from {conf['PG_IMAGE']} ...")
        ctn.run(
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--publish",
            f"{conf['PG_HOST']}:{conf['PG_PORT']}:5432",
            "--env",
            f"POSTGRES_USER={conf['PG_USER']}",
            "--env",
            f"POSTGRES_PASSWORD={conf['PG_PASSWORD']}",
            "--env",
            f"POSTGRES_DB={conf['PG_DATABASE']}",
            conf["PG_IMAGE"],
            capture=False,
        )

    # Check over TCP inside the container: during first-time initialization the
    # image runs a temporary server on the Unix socket only, so a TCP check
    # only passes once the real server is up.
    deadline = time.time() + READY_TIMEOUT
    while time.time() < deadline:
        res = ctn.run(
            "exec",
            name,
            "pg_isready",
            "--host",
            "127.0.0.1",
            "--username",
            conf["PG_USER"],
            check=False,
        )
        if res.returncode == 0:
            print(
                f"PostgreSQL is ready at "
                f"{conf['PG_HOST']}:{conf['PG_PORT']} (container {name!r})."
            )
            return
        time.sleep(1)
    print(f"Error: PostgreSQL in {name!r} was not ready after {READY_TIMEOUT}s.")
    print(ctn.run("logs", name, check=False).stdout)
    sys.exit(1)


def psql_down():
    conf = load_conftest_literals()
    name = conf["PG_CONTAINER_NAME"]
    ctn = Container(find_container_bin())
    if not ctn.is_system_running():
        print("Container system services are not running; nothing to stop.")
        return
    if ctn.get_status(name) is None:
        print(f"Container {name!r} does not exist; nothing to stop.")
        return
    print(f"Stopping container {name!r} ...")
    ctn.run("stop", name, check=False)
    # --rm deletes it on stop; delete again in case it was not started with --rm.
    if ctn.get_status(name) is not None:
        ctn.run("delete", "--force", name, check=False)
    print(f"Container {name!r} is stopped and removed.")


def main():
    commands = {"up": psql_up, "down": psql_down}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        print("Usage: python .mise/tasks/psql.py up|down")
        sys.exit(2)
    commands[sys.argv[1]]()


if __name__ == "__main__":
    main()
