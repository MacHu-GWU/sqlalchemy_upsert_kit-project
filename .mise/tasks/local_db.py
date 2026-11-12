# -*- coding: utf-8 -*-

"""
Shared logic of the local test database tasks (``psql.py``, ``mysql.py``):
start or stop one database container with Apple ``container``.

Both operations are idempotent:

- :func:`container_up` starts the ``container`` system services if they are
  not running, reuses the test container if it is already running, otherwise
  starts a new one with ``--rm``, then waits until the database is ready.
- :func:`container_down` stops the test container if it exists. Because it
  runs with ``--rm``, stopping it also deletes it. The system services keep
  running.

The image, container name, host, port and credentials are read from the
literal assignments in ``tests/conftest.py``, the single place they are defined.
Standard library only, so the tasks run without ``.venv``.
"""

import typing as T
import ast
import json
import sys
import time
import shutil
import subprocess
from pathlib import Path

dir_project_root = Path(__file__).absolute().parent.parent.parent
path_conftest = dir_project_root / "tests" / "conftest.py"

READY_TIMEOUT = 60  # seconds to wait for the database to accept connections


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


def container_up(
    label: str,
    name: str,
    image: str,
    host: str,
    port: int,
    container_port: int,
    env: dict[str, str],
    ready_cmd: list[str],
):
    """
    Start the named database container, or reuse it, and wait until
    ``ready_cmd``, run inside the container, succeeds.

    :param label: Database name for messages, such as ``"PostgreSQL"``
    :param ready_cmd: Readiness check run with ``container exec``. It should
        connect over TCP: on first start the official images run a temporary
        server on the Unix socket only, so a socket check passes too early.
    """
    ctn = Container(find_container_bin())
    ctn.ensure_system_running()

    status = ctn.get_status(name)
    if status == "running":
        print(f"Container {name!r} is already running.")
    else:
        if status is not None:
            # A leftover container that is not running: remove it and start fresh.
            ctn.run("delete", "--force", name, check=False)
        print(f"Starting container {name!r} from {image} ...")
        env_args = list()
        for key, value in env.items():
            env_args.extend(["--env", f"{key}={value}"])
        ctn.run(
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--publish",
            f"{host}:{port}:{container_port}",
            *env_args,
            image,
            capture=False,
        )

    deadline = time.time() + READY_TIMEOUT
    while time.time() < deadline:
        res = ctn.run("exec", name, *ready_cmd, check=False)
        if res.returncode == 0:
            print(f"{label} is ready at {host}:{port} (container {name!r}).")
            return
        time.sleep(1)
    print(f"Error: {label} in {name!r} was not ready after {READY_TIMEOUT}s.")
    print(ctn.run("logs", name, check=False).stdout)
    sys.exit(1)


def container_down(name: str):
    """
    Stop and remove the named container, if it exists.
    """
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


def main(script: str, up: T.Callable[[], None], down: T.Callable[[], None]):
    """
    Dispatch ``python .mise/tasks/<script> up|down``.
    """
    commands = {"up": up, "down": down}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        print(f"Usage: python .mise/tasks/{script} up|down")
        sys.exit(2)
    commands[sys.argv[1]]()
