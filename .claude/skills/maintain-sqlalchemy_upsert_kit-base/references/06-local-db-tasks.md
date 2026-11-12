---
description: "The local test database mise tasks (<engine>-up and <engine>-down, today psql and mysql), their shared script local_db.py: Apple container installed by mise, the idempotency rules, --rm, the unusual port, tests/conftest.py as the one place for connection settings, the first-run service start an agent may not run, and why containers were chosen. Read before changing or adding a local database task, or when psql-up or mysql-up fails."
---

# Local Database Tasks

---

## 1. The tasks

| Task | Runs | Does |
| :--- | :--- | :--- |
| `mise run psql-up` | `python .mise/tasks/psql.py up` | start the PostgreSQL test container, or reuse it, and wait until it accepts connections |
| `mise run psql-down` | `python .mise/tasks/psql.py down` | stop the test container, which also deletes it |
| `mise run mysql-up` | `python .mise/tasks/mysql.py up` | the same for the MySQL test container |
| `mise run mysql-down` | `python .mise/tasks/mysql.py down` | the same for the MySQL test container |

- `test` and `cov` depend on `psql-up` and `mysql-up`, so a local test run always has its databases.
- A new backend that needs a server gets the same pair, named `<engine>-up` and `<engine>-down` after its mise task short name, backed by one script `.mise/tasks/<engine>.py up|down`.
- **Each script is a thin configuration of `.mise/tasks/local_db.py`,** which holds everything that is the same for every database: reading the settings, the `Container` wrapper, `container_up`, `container_down`, and the `up|down` dispatch. A backend script passes its image, port, environment variables, and readiness command. Section 3 describes what `local_db.py` does.
- The scripts are standard library only and run with mise's Python, not `.venv`, so they work before `mise run inst`. The scripts import `local_db` as a sibling module, which works because Python puts the script's own folder on `sys.path`.

---

## 2. Container runtime: Apple container, installed by mise

- `mise.toml` `[tools]`: `container = { version = "1.7.1", os = ["macos"] }`. mise's `aqua:apple/container` backend downloads Apple's signed `.pkg` and only unpacks it, under `~/.local/share/mise/installs/container/<version>/Payload/`. No admin password, no Homebrew.
- It needs Apple silicon and macOS 26 or later. On other systems `os = ["macos"]` makes mise skip it, and `psql-up` and `mysql-up` exit with an error that says so.
- Because it is not installed under `/usr/local`, the services start with `container system start --install-root <...>/Payload`. The script derives that path from the resolved `container` executable (two levels up).

**Why containers, and why this runtime** (the user's requirements: long-term stable, maintained by an organization, not a heavy desktop app):

- The official images (`docker.io/library/postgres`, `docker.io/library/mysql`, maintained by the Docker Official Images program) are Linux builds, like the user's production and CI. The native macOS binaries from conda-forge were rejected: MySQL defaults to `lower_case_table_names=2` on macOS, the PostgreSQL locale behavior differs, and a native server leaves data directories and processes to manage.
- `--rm` gives a fresh database on every start, with nothing left on the host.
- Apple's `container` is maintained by Apple; Docker Desktop was rejected as too heavy. The mise `postgres` and `mysql` plugins (vfox and asdf) are archived, and `pgserver` is unmaintained.

---

## 3. Idempotency rules

Running either task twice in a row must be safe and print what it did.

- **`up`:**
  1. If `container system status` fails, start the services.
  2. Find the container by name in `container list --all --format json`. Its state is `status.state` (container 1.7: `status` is an object, not a string).
  3. If it is `running`, reuse it: do not recreate it, because that would wipe the data mid-session.
  4. If it exists but is not running, delete it. Then start a new one with `run --detach --rm`, publishing only on `127.0.0.1:<port>`.
  5. Wait, up to 60 seconds, until the readiness command inside the container succeeds: `pg_isready --host 127.0.0.1` for PostgreSQL, `mysqladmin ping --host=127.0.0.1 --protocol=TCP` for MySQL. The check goes over TCP on purpose: on first start both images run a temporary server without TCP (PostgreSQL on the Unix socket only, MySQL with networking off), so a socket check passes too early.
- **`down`:** if the services are not running, or the container does not exist, say so and exit 0. Otherwise `stop` it (`--rm` deletes it), and `delete --force` it if it is still listed. The system services keep running.

---

## 4. Connection settings live in `tests/conftest.py`

- `PG_IMAGE`, `PG_CONTAINER_NAME`, `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE`, and the same `MYSQL_*` names, are defined once, at the top of `tests/conftest.py`. The task scripts read them with `ast` instead of importing the file (which imports pytest), so **each must stay a plain literal assignment**; an f-string such as `PG_URL` or `MYSQL_URL` is skipped.
- **The ports are deliberately unusual: `PG_PORT = 45432`, `MYSQL_PORT = 43306`,** so the containers never clash with a server already running on 5432 or 3306. A new backend picks its own unusual port in the same way.
- MySQL: image `mysql:8.4` (the LTS line), user `root`, database `test`, created by the image from `MYSQL_DATABASE`.
- The values are test-only credentials for a throwaway local container.

---

## 5. First run and agents

- Whenever the services are not running (always on a machine's first `psql-up` or `mysql-up`), the task runs `container system start`, which registers a launchd service and downloads the VM kernel. An agent's permission mode may block that as persistence. Then the user runs `mise run psql-up` (or `mysql-up`) once themselves; after that, agents can run it, because it only finds the services and the container already running.
- The first start also pulls the image from Docker Hub. The anonymous pull limit does not matter locally: the image is pulled once and cached.
