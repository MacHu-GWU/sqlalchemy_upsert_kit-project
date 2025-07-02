.. _public-api-reference:

Public API Reference
==============================================================================
This section provides comprehensive documentation for all public APIs in ``sqlalchemy_upsert_kit``. The library is designed with a consistent, database-agnostic interface that works seamlessly across different database systems through SQLAlchemy.

**Recommended Import Pattern**:

.. code-block:: python

    import sqlalchemy_upsert_kit.api as sauk

    # Access database-specific APIs
    sauk.sqlite.insert_or_ignore(...)
    sauk.sqlite.insert_or_replace(...)
    sauk.sqlite.insert_or_merge(...)
    
    # Future database support will follow the same pattern
    # sauk.postgres.insert_or_ignore(...)
    # sauk.mysql.insert_or_ignore(...)


Database Support
------------------------------------------------------------------------------


Currently Supported Databases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- **SQLite**: Full support for all three upsert strategies
- **PostgreSQL**: *Coming soon*
- **MySQL**: *Coming soon*

All database implementations follow the same API pattern, ensuring consistent behavior and easy migration between database systems.


Core API Pattern
------------------------------------------------------------------------------
Every database implementation provides exactly three high-performance bulk upsert functions:

1. **insert_or_ignore**: Insert only new records, ignore conflicts
2. **insert_or_replace**: Replace existing records completely, insert new ones
3. **insert_or_merge**: Selectively update specific columns, preserve others

**Common Parameters**:

All three functions share a consistent parameter interface:

- ``engine``: SQLAlchemy engine for database connection
- ``table``: Target table for upsert operation  
- ``values``: List of dictionaries containing record data
- ``metadata``: Optional metadata instance for table isolation
- ``temp_table_name``: Optional custom temporary table name
- ``conn`` & ``trans``: Optional for user-managed transaction mode

**Return Values**:

Each function returns a tuple indicating the operation results:

- ``insert_or_ignore``: ``(ignored_rows, inserted_rows)``
- ``insert_or_replace``: ``(replaced_rows, inserted_rows)``
- ``insert_or_merge``: ``(updated_rows, inserted_rows)``


SQLite Implementation
------------------------------------------------------------------------------
Access SQLite upsert operations through the ``sauk.sqlite`` namespace:

.. code-block:: python

    import sqlalchemy_upsert_kit.api as sauk
    
    # All SQLite upsert operations
    ignored, inserted = sauk.sqlite.insert_or_ignore(engine, table, data)
    replaced, inserted = sauk.sqlite.insert_or_replace(engine, table, data)  
    updated, inserted = sauk.sqlite.insert_or_merge(engine, table, data, columns)


insert_or_ignore
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Performs conditional bulk insertion, inserting only records that don't conflict with existing data.

**Function Signature**:

:func:`~sqlalchemy_upsert_kit.sqlite.insert_or_ignore.insert_or_ignore`

**Core Parameters**:

- **engine** (``sqlalchemy.Engine``): Database engine for connection management
- **table** (``sqlalchemy.Table``): Target table for insertion operations
- **values** (``list[dict]``): Records to insert. Must include primary key values for conflict detection
- **metadata** (``sqlalchemy.MetaData``, *optional*): Metadata instance for temporary table isolation. Creates new instance if None
- **temp_table_name** (``str``, *optional*): Custom temporary table name. Auto-generated with timestamp if None
- **conn** (``sqlalchemy.Connection``, *optional*): Database connection for user-managed transaction mode
- **trans** (``sqlalchemy.Transaction``, *optional*): Transaction for user-managed transaction mode

**Returns**: ``tuple[int, int]``
    - ``ignored_rows``: Number of records that were not inserted (conflicts)
    - ``inserted_rows``: Number of new records successfully inserted

**Example**:

.. code-block:: python

    # Auto-managed transaction (recommended)
    ignored, inserted = sauk.sqlite.insert_or_ignore(
        engine=engine,
        table=users_table,
        values=[
            {'id': 1, 'name': 'Alice'},  # May be ignored if exists
            {'id': 2, 'name': 'Bob'},    # May be ignored if exists
            {'id': 3, 'name': 'Charlie'} # Will be inserted if new
        ]
    )
    print(f"Ignored: {ignored}, Inserted: {inserted}")

**Use Cases**:
- Incremental data loading where duplicates should be ignored
- ETL processes requiring idempotent behavior
- Data synchronization from external sources


insert_or_replace
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Performs bulk replacement operations, completely replacing existing records with new data and inserting new records.

**Function Signature**:

:func:`~sqlalchemy_upsert_kit.sqlite.insert_or_replace.insert_or_replace`

**Core Parameters**:

- **engine** (``sqlalchemy.Engine``): Database engine for connection management
- **table** (``sqlalchemy.Table``): Target table for upsert operations
- **values** (``list[dict]``): Records to insert or replace. Must include primary key values
- **metadata** (``sqlalchemy.MetaData``, *optional*): Metadata instance for temporary table isolation
- **temp_table_name** (``str``, *optional*): Custom temporary table name
- **conn** (``sqlalchemy.Connection``, *optional*): Database connection for user-managed transactions
- **trans** (``sqlalchemy.Transaction``, *optional*): Transaction for user-managed transactions

**Returns**: ``tuple[int, int]``
    - ``replaced_rows``: Number of existing records that were completely replaced
    - ``inserted_rows``: Number of new records that were inserted

**Example**:

.. code-block:: python

    # Replace existing records entirely, insert new ones
    replaced, inserted = sauk.sqlite.insert_or_replace(
        engine=engine,
        table=users_table,
        values=[
            {'id': 1, 'name': 'Alice Updated', 'email': 'alice.new@example.com'},
            {'id': 2, 'name': 'Bob Updated', 'email': 'bob.new@example.com'},
            {'id': 4, 'name': 'David', 'email': 'david@example.com'}  # New record
        ]
    )
    print(f"Replaced: {replaced}, Inserted: {inserted}")

**Use Cases**:
- Full data synchronization from authoritative sources
- Complete record refresh scenarios  
- When new data should entirely replace existing records

.. warning::
    This operation completely overwrites existing records. All fields of conflicting records will be replaced with new data, including historical fields like timestamps.


insert_or_merge
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Performs selective bulk merge operations, updating only specified columns of existing records while preserving others.

**Function Signature**:

:func:`~sqlalchemy_upsert_kit.sqlite.insert_or_merge.insert_or_merge`

**Core Parameters**:

- **engine** (``sqlalchemy.Engine``): Database engine for connection management
- **table** (``sqlalchemy.Table``): Target table for merge operations
- **values** (``list[dict]``): Records to merge or insert. Must include primary key values
- **columns** (``list[str]``): **Required**. List of column names to update with new values. Other columns are preserved
- **metadata** (``sqlalchemy.MetaData``, *optional*): Metadata instance for temporary table isolation
- **temp_table_name** (``str``, *optional*): Custom temporary table name
- **conn** (``sqlalchemy.Connection``, *optional*): Database connection for user-managed transactions
- **trans** (``sqlalchemy.Transaction``, *optional*): Transaction for user-managed transactions

**Returns**: ``tuple[int, int]``
    - ``updated_rows``: Number of existing records that were selectively updated
    - ``inserted_rows``: Number of new records that were inserted

**Example**:

.. code-block:: python

    # Update only 'email' and 'updated_at' columns, preserve other fields
    updated, inserted = sauk.sqlite.insert_or_merge(
        engine=engine,
        table=users_table,
        values=[
            {'id': 1, 'name': 'Alice', 'email': 'alice.updated@example.com', 'updated_at': datetime.now()},
            {'id': 2, 'name': 'Bob', 'email': 'bob.updated@example.com', 'updated_at': datetime.now()},
            {'id': 5, 'name': 'Eve', 'email': 'eve@example.com', 'updated_at': datetime.now()}
        ],
        columns=['email', 'updated_at']  # Only these columns will be updated
    )
    print(f"Updated: {updated}, Inserted: {inserted}")
    # Records 1-2: email and updated_at changed, name preserved
    # Record 5: completely new record inserted

**Use Cases**:
- Selective column updates (e.g., timestamps while preserving descriptions)
- Incremental data synchronization
- Preserving historical or audit data in non-updated columns

.. note::
    The ``columns`` parameter is **required** and cannot be empty. If you need to update all columns, use ``insert_or_replace`` instead. If you want to ignore conflicts entirely, use ``insert_or_ignore``.


Future Database Support
------------------------------------------------------------------------------
The API is designed for seamless extension to additional database systems:

**Planned Support**:

.. code-block:: python

    # PostgreSQL (coming soon)
    import sqlalchemy_upsert_kit.api as sauk
    sauk.postgres.insert_or_ignore(...)
    sauk.postgres.insert_or_replace(...)
    sauk.postgres.insert_or_merge(...)
    
    # MySQL (coming soon)  
    sauk.mysql.insert_or_ignore(...)
    sauk.mysql.insert_or_replace(...)
    sauk.mysql.insert_or_merge(...)

**Consistent Interface**: All database implementations will maintain the same function signatures and behavior patterns, ensuring easy migration and multi-database support.
