=========================
REFRESH MATERIALIZED VIEW
=========================

Synopsis
--------

.. code-block:: text

    REFRESH MATERIALIZED VIEW view_name

Description
-----------

Initially populate or refresh the data stored in the materialized view
``view_name``. The materialized view must be defined with
:doc:`create-materialized-view`. Data is retrieved from the underlying tables
accessed by the defined query.

The initial population of the materialized view is typically processing
intensive since it reads the data from the source tables and performs physical
write operations.

The refresh operation can be less intensive, if the underlying data has not
changed and the connector has implemented a mechanism to be aware of that. The
specific implementation and performance varies by connector used to create the
materialized view.

See also
--------

* :doc:`create-materialized-view`
* :doc:`drop-materialized-view`
* :doc:`show-create-materialized-view`
