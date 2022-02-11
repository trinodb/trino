============
SHOW COLUMNS
============

Synopsis
--------

.. code-block:: text

    SHOW COLUMNS FROM table [ LIKE pattern ]

Description
-----------

List the columns in a ``table`` along with their data type and other attributes.

:ref:`Specify a pattern <like_operator>` in the optional ``LIKE`` clause to
filter the results to the desired subset. For example, the following query
allows you to find columns ending in ``key``::

    SHOW COLUMNS FROM nation LIKE '%key'
