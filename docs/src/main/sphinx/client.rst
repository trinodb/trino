*******
Clients
*******

A client is used to send queries to Trino and receive results, or otherwise
interact with Trino and the connected data sources.

Some clients, such as the :doc:`command line interface </client/cli>`, can
provide a user interface directly. Clients like the :doc:`JDBC driver
</client/jdbc>`, provide a mechanism for other tools to connect to Trino.

The following clients are available:

.. toctree::
    :maxdepth: 1

    client/cli
    client/jdbc

In addition, the community provides `numerous other clients
<https://trino.io/resources.html>`_ for platforms such as Python, and these
can in turn be used to connect applications using these platforms.
