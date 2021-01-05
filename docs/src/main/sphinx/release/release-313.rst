=========================
Release 313 (31 May 2019)
=========================

General changes
---------------

* Fix leak in operator peak memory computations. (:issue:`843`)
* Fix incorrect results for queries involving ``GROUPING SETS`` and ``LIMIT``. (:issue:`864`)
* Add compression and encryption support for :doc:`/admin/spill`. (:issue:`778`)

CLI changes
-----------

* Fix failure when selecting a value of type :ref:`uuid_type`. (:issue:`854`)

JDBC driver changes
-------------------

* Fix failure when selecting a value of type :ref:`uuid_type`. (:issue:`854`)

Phoenix connector changes
---------------------------

* Allow matching schema and table names case insensitively. This can be enabled by setting
  the ``case-insensitive-name-matching`` configuration property to true. (:issue:`872`)
