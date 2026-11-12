---
description: "How the upsert behaves on MySQL (sqlalchemy_upsert_kit/mysql/): temporary table DDL that does not commit but is not rolled back, the DROP TABLE that commits, ON DUPLICATE KEY UPDATE and its row counts, the no-op update behind ignore, the check of other unique keys, identifier length, collation, and what is untested. Behavior only; the code is the source of truth for how. Read before changing the mysql backend, or when a MySQL upsert commits, leaves a staging table, or updates the wrong row."
---

# Backend: MySQL

This file describes behavior, which does not change when the code is refactored. For how it is implemented, read `sqlalchemy_upsert_kit/mysql/executor.py`; that code is the source of truth, and nothing here repeats it.

The driver is PyMySQL (the `mysql` extra). The backend is tested against MySQL 8.4 locally, and in CI against the MySQL on the runner images (8.0 on Ubuntu, 8.4 on Windows). MariaDB is not supported or tested; the user plans it as a separate backend.

Every finding below was probed against MySQL 8.4.13 before the backend was written, and each has a test that fails without its fix (`03-backend-sqlite.md` and `04-backend-postgres.md` are the other two backends, for comparison).

---

## 1. Transactions and the staging table

- `Connection.begin()` opens a real transaction, so `MysqlUpsertExecutor` keeps the default `begin_transaction`. After a rollback to the SAVEPOINT, the caller's transaction is usable again, with its own earlier writes.
- **`CREATE TEMPORARY TABLE` does not commit, but is not rolled back either.** A rollback to the SAVEPOINT empties the staging table but leaves it in place, on the connection, until the connection closes; pooled connections live long. So `cleanup_after_rollback` drops it with `DROP TEMPORARY TABLE IF EXISTS` (it may not exist yet, when the failure came before `create_staging`). Without it, every failure test of the contract fails.
- **A plain `DROP TABLE` commits the transaction, even on a temporary table**; only `DROP TEMPORARY TABLE` does not. SQLAlchemy's `Table.drop()` emits the plain form, so `drop_staging` sends `DROP TEMPORARY TABLE`. Without it, a user-managed call commits the caller's earlier writes and the upsert, the caller's `rollback()` undoes nothing, and the SAVEPOINT is gone (69 contract tests fail, `test_caller_rollback_undoes_the_upsert` first).
- Other DDL on a temporary table (`ALTER TABLE`, `CREATE INDEX`) also commits. The staging table needs none; keep it that way.
- **A temporary table cannot be opened twice in one statement** (`1137 Can't reopen table`). Every statement here reads the staging table once: the conflict count and the unique key check join it to the target, never to itself.

---

## 2. The upsert statement and its counts

- One `INSERT ... SELECT ... FROM <staging> ON DUPLICATE KEY UPDATE`. The update reads the staged values straight from the staging table (`col = <staging>.col`); SQLAlchemy's `inserted` renders `VALUES(col)`, deprecated since MySQL 8.0.20 (warning 1287). Conflicting rows are updated in place: update triggers fire, delete triggers and `ON DELETE` foreign key actions never do. Like PostgreSQL, a conflicting row fires `BEFORE INSERT` triggers before its update triggers.
- **Ignore is a no-op update, `<pk> = <table>.<pk>`,** because MySQL has no `DO NOTHING`. The row's values never change, but its `UPDATE` triggers fire, unlike on the other backends; a trigger that sets `updated_at` would change the row. The alternatives were rejected: `INSERT IGNORE` turns every error into a warning and writes the row anyway (a too long string is silently truncated, a NULL in a NOT NULL column becomes the default); an anti-join `INSERT ... WHERE NOT EXISTS` raises a duplicate key error when a concurrent call inserts the same new key in between.
- **The row count cannot tell inserted rows apart.** SQLAlchemy's MySQL drivers always set `CLIENT_FOUND_ROWS`, so a row counts 1 when inserted, 2 when changed, and 1 when unchanged. So `rowcount_counts_inserts` is False: the conflicts are counted first for every strategy, and the inserted rows are `len(values)` minus the conflicts. Under concurrent writers the split can be off, as on PostgreSQL; MySQL's default `REPEATABLE READ` reads the count from the transaction's snapshot, so the window is a little wider. The sum and the resulting rows are always right.
- Concurrency: four threads upserting overlapping keys, 80 rounds, raised no deadlock in the probe; `test_concurrent_calls` covers it.

---

## 3. Unique keys other than the primary key

- `ON DUPLICATE KEY UPDATE` has no conflict target: it fires on **any** unique key. A new record whose unique value an existing row already has would update that other row, and the record would be lost, without an error. PostgreSQL and SQLite raise an IntegrityError instead (their `ON CONFLICT (<pk>)` handles only the primary key).
- So `check_unique_keys` runs after the staging table is loaded, for every unique key in the `Table` definition (`get_unique_keys` in `core/schema.py`). It raises `sqlalchemy.exc.IntegrityError`, with `UniqueKeyConflictError` as its `orig`, and the whole call is undone, when:
  - a record with a new primary key has the unique value of an existing row; or
  - two records that write the key share a value: the new records, plus the conflicting ones when the strategy updates a column of the key.
- It does not check a conflicting record that updates the key to an existing row's value: MySQL raises that itself, during the update.
- The user decided that such a conflict is invalid data for an upsert, so the call must fail and change nothing, as on the other backends. The contract tests the four cases (`test_unique_key_*`).
- **Gap:** a unique index that exists only in the database, not in the `Table` (no `unique=True`, `UniqueConstraint`, or unique `Index`), is invisible to the check, and still updates the other row. A table reflected with `autoload_with` has its unique indexes. Reading them from `information_schema` on every call would close the gap; it was left for later.
- A unique key on a generated column or an expression is not checked: the staging table has no values for it.
- Between the check and the upsert, a concurrent insert of the same unique value is not caught.

---

## 4. Names and collation

- Identifiers are limited to 64 characters, but the dialect reports 255, so `max_identifier_length()` returns 64. Without it, the staging name of `t_long` (61 characters) is 75 characters long, and `test_long_table_name` fails.
- The default collation, `utf8mb4_0900_ai_ci`, is case-insensitive, so `'a'` and `'A'` are the same primary key or unique value. Records that differ only in case pass the Python validation, then fail with a duplicate key error while the staging table is loaded, and the call changes nothing. A record `'A'` where `'a'` exists is a conflict. This is MySQL's own semantics; the library does not change it.

---

## 5. Not tested

- MariaDB, and drivers other than PyMySQL.
- Callers at the `READ COMMITTED` or `SERIALIZABLE` isolation level.
- Tables in another database (schema) than the connection's default.
- A composite unique key of which the strategy updates only some columns: the check compares the staged values of every column of the key.
