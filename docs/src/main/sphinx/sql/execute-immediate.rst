=================
EXECUTE IMMEDIATE
=================

Synopsis
--------

.. code-block:: text

    EXECUTE IMMEDIATE `statement` [ USING parameter1 [ , parameter2, ... ] ]

Description
-----------

Executes a statement without the need to prepare or deallocate the statement.
Parameter values are defined in the ``USING`` clause.

Examples
--------

Execute a query with no parameters::

    EXECUTE IMMEDIATE
    'SELECT name FROM nation';

Execute a query with two parameters::

    EXECUTE IMMEDIATE
    'SELECT name FROM nation WHERE regionkey = ? and nationkey < ?'
    USING 1, 3;

This is equivalent to::

   PREPARE statement_name FROM SELECT name FROM nation WHERE regionkey = ? and nationkey < ?
   EXECUTE statement_name USING 1, 3
   DEALLOCATE PREPARE statement_name

See also
--------

:doc:`execute`, :doc:`prepare`, :doc:`deallocate-prepare`
