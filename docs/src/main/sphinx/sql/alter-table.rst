===========
ALTER TABLE
===========

Synopsis
--------

.. code-block:: text

    ALTER TABLE [ IF EXISTS ] name RENAME TO new_name
    ALTER TABLE [ IF EXISTS ] name ADD COLUMN [ IF NOT EXISTS ] column_name data_type
      [ NOT NULL ] [ COMMENT comment ]
      [ WITH ( property_name = expression [, ...] ) ]
    ALTER TABLE [ IF EXISTS ] name DROP COLUMN [ IF EXISTS ] column_name
    ALTER TABLE [ IF EXISTS ] name RENAME COLUMN [ IF EXISTS ] old_name TO new_name
    ALTER TABLE name SET AUTHORIZATION ( user | USER user | ROLE role )
    ALTER TABLE name SET PROPERTIES property_name = expression [, ...]
    ALTER TABLE name EXECUTE command [ ( parameter => expression [, ... ] ) ]
        [ WHERE expression ]

Description
-----------

Change the definition of an existing table.

The optional ``IF EXISTS`` (when used before the table name) clause causes the error to be suppressed if the table does not exists.

The optional ``IF EXISTS`` (when used before the column name) clause causes the error to be suppressed if the column does not exists.

The optional ``IF NOT EXISTS`` clause causes the error to be suppressed if the column already exists.

.. _alter-table-execute:

EXECUTE
^^^^^^^

The ``ALTER TABLE EXECUTE`` statement followed by a ``command`` and
``parameters`` modifies the table according to the specified command and
parameters. ``ALTER TABLE EXECUTE`` supports different commands on a
per-connector basis.

You can use the ``=>`` operator for passing named parameter values.
The left side is the name of the parameter, the right side is the value being passed::

    ALTER TABLE hive.schema.test_table EXECUTE optimize(file_size_threshold => '10MB')

Examples
--------

Rename table ``users`` to ``people``::

    ALTER TABLE users RENAME TO people;

Rename table ``users`` to ``people`` if table ``users`` exists::

    ALTER TABLE IF EXISTS users RENAME TO people;

Add column ``zip`` to the ``users`` table::

    ALTER TABLE users ADD COLUMN zip varchar;

Add column ``zip`` to the ``users`` table if table ``users`` exists and column ``zip`` not already exists::

    ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS zip varchar;

Drop column ``zip`` from the ``users`` table::

    ALTER TABLE users DROP COLUMN zip;

Drop column ``zip`` from the ``users`` table if table ``users`` and column ``zip`` exists::

    ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS zip;

Rename column ``id`` to ``user_id`` in the ``users`` table::

    ALTER TABLE users RENAME COLUMN id TO user_id;

Rename column ``id`` to ``user_id`` in the ``users`` table if table ``users`` and column ``id`` exists::

    ALTER TABLE IF EXISTS users RENAME column IF EXISTS id to user_id;

Change owner of table ``people`` to user ``alice``::

    ALTER TABLE people SET AUTHORIZATION alice

Allow everyone with role public to drop and alter table ``people``::

    ALTER TABLE people SET AUTHORIZATION ROLE PUBLIC

Set table properties (``x=y``) to table ``users``::

    ALTER TABLE people SET PROPERTIES x = 'y'

Collapse files in a table that are over 10 megabytes in size, as supported by
the Hive connector::

    ALTER TABLE hive.schema.test_table EXECUTE optimize(file_size_threshold => '10MB')

See also
--------

:doc:`create-table`
