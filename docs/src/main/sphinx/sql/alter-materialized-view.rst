=======================
ALTER MATERIALIZED VIEW
=======================

Synopsis
--------

.. code-block:: text

    ALTER MATERIALIZED VIEW [ IF EXISTS ] name RENAME TO new_name

Description
-----------

Change the name of an existing materialized view.

The optional ``IF EXISTS`` clause causes the error to be suppressed if the
materialized view does not exist. The error is not suppressed if the
materialized view does not exist, but a table or view with the given name
exists.

Examples
--------

Rename materialized view ``people`` to ``users`` in the current schema::

    ALTER MATERIALIZED VIEW people RENAME TO users;

Rename materialized view ``people`` to ``users``, if materialized view
``people`` exists in the current catalog and schema::

    ALTER MATERIALIZED VIEW IF EXISTS people RENAME TO users;

See also
--------

* :doc:`create-materialized-view`
* :doc:`refresh-materialized-view`
* :doc:`drop-materialized-view`
