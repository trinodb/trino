=====
MERGE
=====

Synopsis
--------

.. code-block:: text

    MERGE INTO target_table [ [ AS ]  target_alias ]
    USING { source_table | query } [ [ AS ] source_alias ]
    ON search_condition
    when_clause [...]

where ``when_clause`` is one of

.. code-block:: text

    WHEN MATCHED [ AND condition ]
        THEN DELETE

.. code-block:: text

    WHEN MATCHED [ AND condition ]
        THEN UPDATE SET ( column = expression [, ...] )

.. code-block:: text

    WHEN NOT MATCHED [ AND condition ]
        THEN INSERT [ column_list ] VALUES (expression, ...)

Description
-----------

Conditionally update and/or delete rows of a table and/or insert new
rows into a table.

``MERGE`` supports an arbitrary number of ``WHEN`` clauses with different
``MATCHED`` conditions, executing the ``DELETE``, ``UPDATE`` or ``INSERT``
operation in the first ``WHEN`` clause selected by the ``MATCHED``
state and the match condition.

In ``WHEN`` clauses with ``UPDATE`` operations, the column value expressions
can depend on any field of the target or the source.  In the ``NOT MATCHED``
case, the ``INSERT`` expressions can depend only on the source.

Each row in the target table must matched by at most one row in the source.
If more than one source row is matched by a target table row, a
``MERGE_TARGET_ROW_MULTIPLE_MATCHES`` exception is raised.


Examples
--------

Delete all customers mentioned in the source table::

    MERGE INTO target_table t USING source_table s
        ON (t.customer = s.customer)
        WHEN MATCHED
            THEN DELETE

For matching customer rows, increment the purchases, and if there is no
match, insert the row from the source table::

    MERGE INTO target_table t USING source_table s
        ON (t.customer = s.customer)
        WHEN MATCHED
            THEN UPDATE SET purchases = s.purchases + t.purchases
        WHEN NOT MATCHED
            THEN INSERT (customer, purchases, address)
                  VALUES(s.customer, s.purchases, s.address)

``MERGE`` into the target table from the source table, deleting any matching
target row for which the source address is Centreville.  For all other
matching rows, add the source purchases and set the address to the source
address, if there is no match in the target table, insert the source
table row::

    MERGE INTO target_table t USING source_table s
        ON (t.customer = s.customer)
        WHEN MATCHED AND s.address = 'Centreville'
            THEN DELETE
        WHEN MATCHED
            THEN UPDATE
                SET purchases = s.purchases + t.purchases, address = s.address
        WHEN NOT MATCHED
            THEN INSERT (customer, purchases, address)
                  VALUES(s.customer, s.purchases, s.address)

Limitations
-----------

Some connectors have limited or no support for ``MERGE``.
See connector documentation for more details.
