---
description: "How the upsert behaves on MySQL (sqlalchemy_upsert_kit/mysql/): temporary table DDL that does not commit but is not rolled back, the DROP TABLE that commits, ON DUPLICATE KEY UPDATE and its row counts, the no-op update behind ignore, the checks of other unique keys before and after the upsert, deadlocks, identifier length, collation, the decisions made with the user, and what is untested. Behavior only; the code is the source of truth for how. Read before changing the mysql backend, or when a MySQL upsert commits, leaves a staging table, or updates the wrong row."
---

# Backend: MySQL

This file describes behavior, which does not change when the code is refactored. For how it is implemented, read `sqlalchemy_upsert_kit/mysql/executor.py`; that code is the source of truth, and nothing here repeats it.

The driver is PyMySQL (the `mysql` extra). The backend is tested against MySQL 8.4 locally, and in CI against the MySQL on the runner images (8.0 on Ubuntu, 8.4 on Windows). MariaDB is not supported or tested; the user plans it as a separate backend.

The findings below were probed against MySQL 8.4.13, before the backend was written or in a later review. Each one the code works around has a test that fails without the workaround; the rest are constraints the code must keep (`03-backend-sqlite.md` and `04-backend-postgres.md` are the other two backends, for comparison).

---

## 1. Transactions and the staging table

- `Connection.begin()` opens a real transaction, so `MysqlUpsertExecutor` keeps the default `begin_transaction`. After a rollback to the SAVEPOINT, the caller's transaction is usable again, with its own earlier writes.
- **Except after a deadlock** (error 1213): InnoDB then rolls back the whole transaction and drops its savepoints, so the caller's earlier writes are gone, and the rollback to the SAVEPOINT fails with `1305 SAVEPOINT ... does not exist`, which is the error the caller sees, in place of the deadlock. Concurrent calls of this backend do not deadlock (section 2), but a caller's own concurrent transactions can.
- If `cleanup_after_rollback` fails too (a lost connection), the core keeps the original error and adds the cleanup error to it as a note.
- **`CREATE TEMPORARY TABLE` does not commit, but is not rolled back either.** A rollback to the SAVEPOINT empties the staging table but leaves it in place, on the connection, until the connection closes; pooled connections live long. So `cleanup_after_rollback` drops it with `DROP TEMPORARY TABLE IF EXISTS` (it may not exist yet, when the failure came before `create_staging`). Without it, every failure test of the contract fails.
- **A plain `DROP TABLE` commits the transaction, even on a temporary table**; only `DROP TEMPORARY TABLE` does not. SQLAlchemy's `Table.drop()` emits the plain form, so `drop_staging` sends `DROP TEMPORARY TABLE`. Without it, a user-managed call commits the caller's earlier writes and the upsert, the caller's `rollback()` undoes nothing, and the SAVEPOINT is gone (69 contract tests fail, `test_caller_rollback_undoes_the_upsert` first).
- Other DDL on a temporary table (`ALTER TABLE`, `CREATE INDEX`) also commits. The staging table needs none; keep it that way.
- **A temporary table cannot be opened twice in one statement** (`1137 Can't reopen table`). Every statement here reads the staging table once: the conflict count and the unique key checks join it to the target, never to itself. No test catches a statement that breaks this; it fails at once with that error.

---

## 2. The upsert statement and its counts

- One `INSERT ... SELECT ... FROM <staging> ON DUPLICATE KEY UPDATE`. The update reads the staged values straight from the staging table (`col = <staging>.col`): for `INSERT ... SELECT`, SQLAlchemy's `inserted` renders `VALUES(col)`, deprecated since MySQL 8.0.20 (warning 1287). Conflicting rows are updated in place: update triggers fire, delete triggers and `ON DELETE` foreign key actions never do. Like PostgreSQL, a conflicting row fires `BEFORE INSERT` triggers before its update triggers.
- **Ignore is a no-op update, `<pk> = <table>.<pk>`,** because MySQL has no `DO NOTHING`. The row's values never change, but its `UPDATE` triggers fire, unlike on the other backends; a trigger that sets `updated_at` would change the row. The alternatives were rejected: `INSERT IGNORE` turns every error into a warning and writes the row anyway (a too long string is silently truncated, a NULL in a NOT NULL column becomes the default); an anti-join `INSERT ... WHERE NOT EXISTS` raises a duplicate key error when a concurrent call inserts the same new key in between.
- **The row count cannot tell inserted rows apart.** SQLAlchemy's MySQL drivers always set `CLIENT_FOUND_ROWS`, so a row counts 1 when inserted, 2 when changed, and 1 when unchanged. So `rowcount_counts_inserts` is False: the conflicts are counted first for every strategy, and the inserted rows are `len(values)` minus the conflicts. The split can be off, and the sum and the resulting rows are always right:
  - under concurrent writers, as on PostgreSQL;
  - **in a user-managed call whose transaction read before**: at MySQL's default `REPEATABLE READ`, the count is a plain read of the snapshot that the transaction's first read took, and misses rows committed since. A key committed by another transaction after that is counted as inserted (`test_old_snapshot_counts`).
- **Concurrency and locks.** Four threads upserting overlapping keys raise no deadlock (probed for 40 rounds each of ignore and merge; `test_concurrent_calls`). The reads before the upsert must stay plain reads: locking reads (`LOCK IN SHARE MODE`) were tried, to see the latest rows, and took shared gap locks on the new keys that the inserts of concurrent calls then deadlocked on; half the calls failed.

---

## 3. Unique keys other than the primary key

- `ON DUPLICATE KEY UPDATE` has no conflict target: it fires on **any** unique key. A new record whose unique value an existing row already has would update that other row, and the record would be lost, without an error. PostgreSQL and SQLite raise an IntegrityError instead (their `ON CONFLICT (<pk>)` handles only the primary key).
- Two checks keep the contract (`01-scope.md` section 3). Each raises `sqlalchemy.exc.IntegrityError`, with `UniqueKeyConflictError` as its `orig`, and the rollback to the SAVEPOINT undoes the whole call.
- **`check_unique_keys`, before the upsert,** for every unique key in the `Table` definition (`get_unique_keys` in `core/schema.py`), with a message that names the key. It raises when:
  - a record with a new primary key has the value of an existing row; or
  - several records would write the same value: the new records, plus the conflicting ones when the strategy updates a column of the key. A conflicting record is compared by the value its row will have, the updated columns from the record and the others from the row: merge that writes only `b` of a key `(a, b)` keeps the row's `a` (`test_composite_unique_key_partly_written`).
- It leaves out a conflicting record whose row would end up with another existing row's value: MySQL raises that itself, during the update.
- **It is stricter than the other backends in one case,** on purpose: values are compared as they are before the call, so a value that a conflicting record moves away from is still taken by it. SQLite and PostgreSQL check row by row, and accept or reject the same input depending on the order of the rows. The fixed answer follows the user's rule: when in doubt, fail and change nothing.
- **`check_after_upsert`, after it,** catches what the first check cannot see: a unique index that exists only in the database, not in the `Table`, and, in a user-managed call, a row committed after the transaction's snapshot (section 2). The upsert never changes a primary key, so a record whose key is not in the table afterwards was lost to another row. This check is a locking read, so it sees the latest rows, but it reads only rows that the upsert has just written and locked, so it takes no lock that another transaction holds; only a lost record's key takes a gap lock, and the call fails then anyway. Its message cannot name the key (`test_check_after_upsert`, `test_old_snapshot_unique_key`).
- A unique key on a generated column or an expression is not checked before the upsert: the staging table has no values for it. The check after it still catches a lost record.
- NULL values never conflict, in a single or composite key (`test_unique_key_nulls`).

---

## 4. Names and collation

- Identifiers are limited to 64 characters, but the dialect reports 255, so `max_identifier_length()` returns 64. Without it, the staging name of `t_long` (61 characters) is 75 characters long, and `test_long_table_name` fails.
- **Values compare by the target's collation.** The staging table copies the target's table-level character set and collation options (`mysql_charset`, `mysql_collate`, and the others in `COMPARISON_OPTIONS`, `core/schema.py`), and each column's type keeps its own collation. Without that, the staging table took the database default, and a target declared with `mysql_collate="utf8mb4_bin"` rejected `'e'` and `'E'` as duplicates (`test_table_collation`).
- The default collation, `utf8mb4_0900_ai_ci`, is case-insensitive, so there `'a'` and `'A'` are the same primary key or unique value: records that differ only in case pass the Python validation, then raise `IntegrityError` (a duplicate primary key while the staging table is loaded, or the unique key check), and the call changes nothing. A record `'A'` where `'a'` exists is a conflict. This is the target's own semantics; the library keeps it.

---

## 5. Decisions made with the user, and why

The findings above were explained to the user, who decided each point below. Keep these decisions unless the user changes them.

### 5.1 Ignore is a no-op update (option A)

Three ways to leave a conflicting row alone were compared:

| Option | Statement | Cost | Decision |
| :--- | :--- | :--- | :--- |
| A | `ON DUPLICATE KEY UPDATE <pk> = <table>.<pk>` | the row's `UPDATE` triggers fire, and it is locked until the transaction ends | **chosen** |
| B | `INSERT IGNORE` | every error becomes a warning and the row is written anyway: the probe wrote a silently truncated string; a NULL becomes the default, a foreign key violation is skipped, and a conflict on another unique key is skipped | rejected: silent wrong data |
| C | `INSERT ... SELECT ... WHERE NOT EXISTS (<the key in the target>)` | check, then write: two concurrent calls that both find a new key absent both insert it, and the second fails with `1062 Duplicate entry` | rejected: random failures under concurrency |

Why A: it is the only option where the data is always right and a call never fails because of a concurrent one; its cost is a side effect, documented to users on the MySQL docs page. C fails for the same reason the library left JOIN-based writes in the 0.2 refactor (`02-architecture.md` section 5): only the native upsert decides atomically, row by row, whether a row conflicts.

### 5.2 A conflict on another unique key is an error (option X)

- **The user's rule:** a record that conflicts on a unique key other than the primary key is wrong data for this operation, so the call must raise and do nothing. The precise reason: the three strategies are defined on the primary key only (`01-scope.md` section 3); a record that matches one row by its primary key and another by a unique key has no defined outcome, and any automatic choice would be a guess.
- SQLite and PostgreSQL already behave this way, because their `ON CONFLICT (<pk>)` raises for other keys. MySQL updates the other row instead, so the library checks before the upsert (section 3). The alternative, documenting it as a MySQL limitation (option Y), was rejected: users would get silent data corruption.
- **The check runs after the staging table is loaded,** not with the `ValueError` checks before any SQL: whether a value is taken depends on the rows in the database.
- **It raises `sqlalchemy.exc.IntegrityError`,** the error SQLite and PostgreSQL raise for the same input, so one `except IntegrityError` covers every backend. Its `orig` is `UniqueKeyConflictError`, with the key and the problem in its message.
- **The check before the upsert reads the unique keys declared in the `Table`,** not the ones in the database. This matches how the library already trusts the `Table` (the primary key comes from `get_pk_column`), and costs nothing on a table without other unique keys. Reading the indexes from `information_schema` on every call was left for later; the check after the upsert (section 3) catches the undeclared ones anyway, with a less specific message.
- The rule now holds for every backend: it is in the contract (`01-scope.md` section 3) and its `test_unique_key_*` tests.

### 5.3 The reads before the upsert stay plain reads

A review found that, in a user-managed call, the plain reads before the upsert see the transaction's old snapshot, so the unique key check missed a row committed since, and the record was lost. The planned fix, locking reads, made concurrent calls deadlock (section 2), so it was replaced with the check after the upsert (section 3), which fixes the data loss with no new lock waits. Its cost: in that case the split of the counts can be off (section 2).

### 5.4 Driver, image, port, CI

- **PyMySQL:** pure Python, so it installs on every OS and Python of the CI matrix without a compiler or MySQL client library (mysqlclient needs them). Its `rsa` extra (`cryptography`) is not required: the probe authenticated with MySQL 8.4's default `caching_sha2_password`, after `FLUSH PRIVILEGES` cleared the server's cache, without it. CI also authenticates without it. Add `PyMySQL[rsa]` to the extras only if that changes.
- **`mysql:8.4`,** the LTS line, on port 43306, an unusual port like every local test database.
- **CI uses the MySQL on the runner images,** for the same reasons as PostgreSQL: no image pulled from Docker Hub, and service containers run on Linux only.

### 5.5 MariaDB is a separate backend, later

MariaDB is not supported, and this backend is not tested against it. MariaDB has diverged from MySQL (for example, it keeps `VALUES()` in `ON DUPLICATE KEY UPDATE`, which MySQL deprecated), so every finding here would need its own probe; it would be its own backend, added with the add-backend invoker. The user plans it for a later release.

---

## 6. Not tested

- MariaDB, and drivers other than PyMySQL.
- Callers at the `READ COMMITTED` or `SERIALIZABLE` isolation level.
- Tables in another database (schema) than the connection's default.
- The deadlock case of section 1, which no test provokes.
