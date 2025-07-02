.. _transaction-mode:

Transaction Mode
==============================================================================
Any upsert operation is wrapped in a transaction to ensure atomicity and consistency.

This library supports two modes of transaction management:

- **Auto-managed transactions**: The function creates and manages its own database connection and transaction.
- **User-managed transactions**: The function operates within an existing connection and transaction provided by the caller.


Auto-Managed Transactions
------------------------------------------------------------------------------
The function creates and manages its own database connection and transaction.

This is the default mode when no connection/transaction parameters are provided. The entire operation is wrapped in a single transaction that automatically commits on success or rolls back on error.

Example:

.. code-block:: python

    # Auto-managed: function handles all transaction logic
    upsert_operation(
        engine,
        table,
        values,
    )


User-Managed Transactions
------------------------------------------------------------------------------
The function operates within an existing connection and transaction provided by the caller.

This allows the operation to be part of a larger transactional context. The caller is responsible for committing or rolling back the transaction.

Example:

.. code-block:: python

    # User-managed: operation is part of larger transaction
    with engine.connect() as conn:
        with conn.begin() as trans:
            # Other database operations...

            # upsert operation
            upsert_operation(
                engine,
                table,
                values,
                conn=conn,
                trans=trans,
            )

            # More database operations...

            # Transaction committed/rolled back by user
