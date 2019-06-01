===========
Release 313
===========

General Changes
---------------

* Fix leak in operator peak memory computations. (:issue:`843`)
* Fix incorrect results for queries involving ``GROUPING SETS`` and ``LIMIT`` (:issue:`864`)
* Add spill to disk compression and encryption support. (:issue:`778`)
