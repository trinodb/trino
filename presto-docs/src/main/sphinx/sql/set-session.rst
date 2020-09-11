===========
SET SESSION
===========

Synopsis
--------

.. code-block:: none

    SET SESSION name = expression
    SET SESSION catalog.name = expression

Description
-----------

Set a session property value or a catalog session property.


Connectors can provide session properties. They are set separately for each
catalog by prefixing them with the catalog name. There is no sharing of
properties across catalogs, even if the catalogs use the same connector.

Examples
--------

The following example sets a system session property to enable optimized hash
generation::

    SET SESSION optimize_hash_generation = true;

An example of the usage of catalog session properties is the :doc:`Accumulo
connector </connector/accumulo>`, which supports a property named
``optimize_locality_enabled``. If your Accumulo catalog is named ``data``, you
would use the following to set the catalog session property::

    SET SESSION data.optimize_locality_enabled = false;

This catalog session property does not apply to any other catalog, even if it
also uses the Accumulo connector.

See Also
--------

:doc:`reset-session`, :doc:`show-session`
