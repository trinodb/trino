============
SHOW COLUMNS
============

Synopsis
--------

.. code-block:: text

    SHOW COLUMNS FROM table [ LIKE pattern ]

Description
-----------

List the columns in a ``table`` along with their data type and other attributes::

    SHOW COLUMNS FROM nation;

.. code-block:: text

      Column   |     Type     | Extra | Comment
    -----------+--------------+-------+---------
     nationkey | bigint       |       |
     name      | varchar(25)  |       |
     regionkey | bigint       |       |
     comment   | varchar(152) |       |


:ref:`Specify a pattern <like-operator>` in the optional ``LIKE`` clause to
filter the results to the desired subset. For example, the following query
allows you to find columns ending in ``key``::

    SHOW COLUMNS FROM nation LIKE '%key';

.. code-block:: text

      Column   |     Type     | Extra | Comment
    -----------+--------------+-------+---------
     nationkey | bigint       |       |
     regionkey | bigint       |       |
