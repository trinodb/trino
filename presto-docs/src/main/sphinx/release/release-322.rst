===========
Release 322
===========

General Changes
---------------

* Improve performance of certain join queries by reducing the amount of data
  that needs to be scanned. (:issue:`1673`)

Server RPM Changes
------------------

* Fix a regression that caused zero-length files in the RPM. (:issue:`1767`)

Connector Changes
-----------------

These changes apply to MySQL, PostgreSQL, Redshift, and SQL Server.

* Add support for providing credentials using a keystore file. This can be enabled
  by setting the ``credential-provider.type`` configuration property to ``KEYSTORE``
  and by setting the ``keystore-file-path``, ``keystore-type``, ``keystore-password``,
  ``keystore-user-credential-password``, ``keystore-password-credential-password``,
  ``keystore-user-credential-name``, and ``keystore-password-credential-name``
  configuration properties. (:issue:`1521`)
