=======================
Generic JDBC Connector
=======================

The Generic JDBC connector allows querying tables in an
external database (using the database's native JDBC driver). This can be used to join data between
different systems like Sybase and Hive, or between two different
Oracle instances.

Configuration
-------------

To configure the Generic JDBC connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``sybase.properties``, to
mount the Generic JDBC connector as the ``sybase`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=genericjdbc
    driver-class=
    connection-url=jdbc:
    connection-user=root
    connection-password=secret
    case-insensitive-name-matching=true

Some examples:

.. code-block:: none

    connector.name=genericjdbc
    driver-class=com.sybase.jdbc4.jdbc.SybDataSource
    connection-url=jdbc:sybase:Tds:localhost:8000/MYSYBASE
    connection-user=sa
    connection-password=myPassword
    case-insensitive-name-matching=true

.. code-block:: none

    connector.name=genericjdbc
    driver-class=oracle.jdbc.driver.OracleDriver
    connection-url=jdbc:oracle:thin:@localhost:49161:xe
    connection-user=system
    connection-password=rootpw
    case-insensitive-name-matching=true

Multiple Generic JDBC Databases or Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Generic JDBC connector can only access a single database within
a SGeneric JDBC server. Thus, if you have multiple Generic JDBC databases,
or want to connect to multiple instances of the Generic JDBC, you must configure
multiple catalogs, one for each instance.

To add another catalog, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``. For example,
if you name the property file ``sales.properties``, Presto creates a
catalog named ``sales`` using the configured connector.

Specific driver jars
--------------------

You must place the specific JAR for your database (ie ojdbc6.jar, jconn4.jar etc) containing the driver-class into the plugin/genericjdbc folder.

Generic JDBC Connector Limitations
----------------------------------

Read-only access
Not all data types can be mapped
Not all databases will work
Performance
Experimental
