============
Release 0.81
============

Hive changes
------------

* Fix ORC predicate pushdown.
* Fix column selection in RCFile.

General changes
---------------

* Fix handling of null and out-of-range offsets for
  :func:`lead`, :func:`lag` and :func:`nth_value` functions.
