===============================
Migrate from PrestoSQL to Trino
===============================

Upgrade and migrate to first version of Trino, formally know as
Presto SQL. 

Rebranding from Presto SQL to Trino has some unvoidable compatibility issues
Trino admins need to know. The information in this guide is designed to make
this transition as smooth as possible.

Migration guide
---------------

Follow these four steps to migrate from Presto SQL to Trino.

1. Prepare to deploy the new version
2. Upgrade your servers to Trino 351+
3. Upgrade clients
4. Cleanup

Prepare to deploy the new version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before you deploy a new version:

* Notify your users of the name and logo changes  
* Confirm they use client version 350

Check the coordinator HTTP request logs to review your users client versions.

To allow continued support of your exisiting Presto clients, update your
server configuration with ``protocol.v1.alternate-header-name=Presto``.

If you use RPM, you need a plan to deal with the new RPM name and the Trino
directory names.

If you work with Docker:

* Use the new image name 
* Confirm your configuration mounts use the Trino path name
* Remember that Trino is the new name of the CLI.

Update any custom plugins to use the new SPI.

If you're using JMX to monitor your clusters, decide if you want to:

* Update them to the new names
* Set a Trino config to revert to the old names

Upgrade your servers to Trino 351+
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Upgrade your development, staging, and production servers. 

You can upgrade clusters one at a time and verify everything works before
upgrading the next cluster.

Upgrade clients
^^^^^^^^^^^^^^^

Upgrade all clients to the Trino versions, including the CLI, JDBC driver,
Python, etc.,.


Update any applications using JDBC to use the new ``jdbc:trino:`` connection URL
prefix.

Cleanup
^^^^^^^

Remove the ``protocol.v1.alternate-header-name`` configuration property. 

If you configured Trino to use the old JMX names, convert your monitoring system
to use the new JMX names and remove the fallback configs.