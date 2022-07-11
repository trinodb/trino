===========
CREATE VIEW
===========

Synopsis
--------

.. code-block:: text

    CREATE [ OR REPLACE ] VIEW view_name
    [ COMMENT view_comment ]
    [ SECURITY { DEFINER | INVOKER } ]
    AS query

Description
-----------

Create a new view of a :doc:`select` query. The view is a logical table
that can be referenced by future queries. Views do not contain any data.
Instead, the query stored by the view is executed every time the view is
referenced by another query.

The optional ``OR REPLACE`` clause causes the view to be replaced if it
already exists rather than raising an error.

Security
--------

In the default ``DEFINER`` security mode, tables referenced in the view
are accessed using the permissions of the view owner (the *creator* or
*definer* of the view) rather than the user executing the query. This
allows providing restricted access to the underlying tables, for which
the user may not be allowed to access directly.

In the ``INVOKER`` security mode, tables referenced in the view are accessed
using the permissions of the user executing the query (the *invoker* of the view).
A view created in this mode is simply a stored query.

Regardless of the security mode, the ``current_user`` function will
always return the user executing the query and thus may be used
within views to filter out rows or otherwise restrict access.

Examples
--------

Create a simple view ``test`` over the ``orders`` table::

    CREATE VIEW test AS
    SELECT orderkey, orderstatus, totalprice / 2 AS half
    FROM orders

Create a view ``test_with_comment`` with a view comment::

    CREATE VIEW test_with_comment
    COMMENT 'A view to keep track of orders.'
    AS
    SELECT orderkey, orderstatus, totalprice
    FROM orders

Create a view ``orders_by_date`` that summarizes ``orders``::

    CREATE VIEW orders_by_date AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

Create a view that replaces an existing view::

    CREATE OR REPLACE VIEW test AS
    SELECT orderkey, orderstatus, totalprice / 4 AS quarter
    FROM orders

See also
--------

:doc:`drop-view`, :doc:`show-create-view`
