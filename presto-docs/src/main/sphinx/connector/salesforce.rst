====================
Salesforce Connector
====================

The Salesforce connector allows querying and creating tables in an
external Salesforce instance. This can be used to join data between
different systems like Salesforce and Hive, or between two different
Salesforce instances.

Configuration
-------------

To configure the Salesforce connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``salesforce.properties``, to
mount the Salesforce connector as the ``salesforce`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=salesforce
    connection-url=jdbc:salesforce://
    connection-user=admin
    connection-password=secret
    salesforce.security-token=abc

Querying Salesforce
-------------------

The Salesforce connector provides single a schema named ``salesforce``.

    SHOW TABLES FROM salesforce.salesforce;

You can see a list of the columns in the ``account`` table in the ``salesforce`` database
using either of the following::

    DESCRIBE salesforce.salesforce.account;
    SHOW COLUMNS FROM salesforce.salesforce.account;

Finally, you can access the ``account`` table::

    SELECT * FROM salesforce.salesforce.account;

If you used a different name for your catalog properties file, use
that catalog name instead of ``salesforce`` in the above examples.

Redshift Connector Limitations
------------------------------

At this time this connector is read-only. Furthermore, it fetches data
using the Salesforce synchronous API, which offers limited performance.

Queries on the information schema can be especially expensive.
