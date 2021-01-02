===================
Snowflake Connector
===================

The Snowflake connector allows querying and creating tables in an
external Snowflake account. This can be used to join data between
different systems like Snowflake and Hive, or between two different
Snowflake accounts.

Configuration
-------------

To configure the Snowflake connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``snowflake.properties``, to
mount the Snowflake connector as the ``snowflake`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=snowflake
    connection-url=jdbc:snowflake://<account>.snowflakecomputing.com
    connection-user=root
    connection-password=secret
    snowflake.account=account
    snowflake.database=database
    snowflake.role=role
    snowflake.warehouse=warehouse


Multiple Snowflake Databases or Accounts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Snowflake connector can only access a single database within
a Snowflake account. Thus, if you have multiple Snowflake databases,
or want to connect to multiple Snowflake accounts, you must configure
multiple instances of the Snowflake connector.

To add another catalog, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``. For example,
if you name the property file ``sales.properties``, Presto creates a
catalog named ``sales`` using the configured connector.
