==========
SHOW STATS
==========

Synopsis
--------

.. code-block:: text

    SHOW STATS FOR table
    SHOW STATS FOR ( query )

Description
-----------

Returns approximated statistics for the named table or for the results of a
query. Returns ``NULL`` for any statistics that are not populated or
unavailable on the data source.

Statistics are returned as a row for each column, plus a summary row for
the table (identifiable by a ``NULL`` value for ``column_name``). The following
table lists the returned columns and what statistics they represent. Any
additional statistics collected on the data source, other than those listed
here, are not included.

.. list-table:: Statistics
  :widths: 20, 40, 40
  :header-rows: 1

  * - Column
    - Description
    - Notes
  * - ``column_name``
    - The name of the column
    - ``NULL`` in the table summary row
  * - ``data_size``
    - The total size in bytes of all of the values in the column
    - ``NULL`` in the table summary row. Available for columns of :ref:`string
      <string-data-types>` data types with variable widths.
  * - ``distinct_values_count``
    - The estimated number of distinct values in the column
    - ``NULL`` in the table summary row
  * - ``nulls_fractions``
    - The portion of the values in the column that are ``NULL``
    - ``NULL`` in the table summary row.
  * - ``row_count``
    - The estimated number of rows in the table
    - ``NULL`` in column statistic rows
  * - ``low_value``
    - The lowest value found in this column
    - ``NULL`` in the table summary row. Available for columns of :ref:`DATE
      <date-data-type>`, :ref:`integer <integer-data-types>`,
      :ref:`floating-point <floating-point-data-types>`, and
      :ref:`fixed-precision <fixed-precision-data-types>` data types.
  * - ``high_value``
    - The highest value found in this column
    - ``NULL`` in the table summary row. Available for columns of :ref:`DATE
      <date-data-type>`, :ref:`integer <integer-data-types>`,
      :ref:`floating-point <floating-point-data-types>`, and
      :ref:`fixed-precision <fixed-precision-data-types>` data types.
