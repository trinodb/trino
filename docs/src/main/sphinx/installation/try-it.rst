Try Trino with Docker
======================

This guide focuses on your first ten minutes
using Trino, and relies on Docker to simplify the installation and
ensure that the CLI runs on your platform of choice.

This Docker container includes both the Trino server and the 
Trino command line interface (CLI), which provides a terminal-based,
interactive shell for running queries and inspecting catalog structures
in large data clusters. In production, it's unlikely that your server 
and client will reside in the same location. However, to keep this guide 
short and sweet, we are going to run both from your machine.

In this guide, you'll learn how to:

-  Install and run the Trino Interactive CLI
-  Call Trino from the terminal

Prerequisites
-------------

Trino runs best on Linux. Our Docker container is built on 
Linux, which means that you can run this guide from Linux, 
macOS, or Windows.

-  `Install Docker <https://docs.docker.com/get-docker/>`__

Pull Trino from Docker
------------------------------

Once Docker is installed, the next step is to pull the Trino container
from Docker. This container contains both the server and the CLI.

From your terminal, run:

.. code-block:: shell

    docker pull trinodb/trino

Start the Trino server
----------------------

After you've pulled the container, you'll need to start your Trino server. 
You're going to make calls against this server (and it's data) throughout 
this guide. 

.. code-block:: shell

   docker run -p 8080:8080 --name trino trinodb/trino

When the server is ready, you'll see this message:

.. code-block:: text

   INFO    main    io.trino.server.Server    ======== SERVER STARTED ========

Start the Trino CLI
--------------------

In a new terminal window or tab, run this command to start the Trino
Interactive CLI:

.. code-block:: shell

    docker exec -it trino trino

When the Trino Interactive CLI is ready to use, you should see:

.. code-block:: shell

    trino>

You can exit the Trino Interactive CLI at any time, run:

.. code-block:: shell

    trino> EXIT;

How to get help
~~~~~~~~~~~~~~~

One of the most important things you need to know when learning a new
tool is where to find help. With Trino, it's as easy as passing
``help;`` to the interactive CLI.

.. code-block:: shell

    trino> help;

Show configured resources
~~~~~~~~~~~~~~~~~~~~~~~~~

Now that your server is running and you've started the CLI, let's make a
few calls. We're going to start by listing configured resources
(``CATALOGS``).

.. code-block:: shell

    trino> SHOW CATALOGS;

The server included with the Docker image will return this response:

.. code-block:: text

      Catalog
    -----------
     jmx
     memory
     system
     tpcds
     tpch
    (5 rows)

Explore a catalog
~~~~~~~~~~~~~~~~~

Start with the ``tpch`` catalog, which lets you to test the capabilities
and query syntax of Trino without configuring access to an external data
source.

.. code-block:: shell

    trino> SHOW SCHEMAS FROM tpch;

Set catalog and schema
~~~~~~~~~~~~~~~~~~~~~~

To avoid typing the catalog and schema each time, try the ``USE``
command:

.. code-block:: shell

    trino> USE tpch.sf100;

Now you should see:

.. code-block:: shell

    trino:sf100>

Show table data
~~~~~~~~~~~~~~~

To view the tables in sf100, run:

.. code-block:: shell

    trino:sf100> SHOW TABLES;

Which returns:

.. code-block:: text

      Table
    ----------
     customer
     lineitem
     nation
     orders
     part
     partsupp
     region
     supplier
    (8 rows)

With this list of tables, you can continue to drill-down. Let's look at
customer data:

.. code-block:: shell

    trino:sf100> SHOW COLUMNS FROM customer;

Which returns:

.. code-block:: text

       Column   |     Type     | Extra | Comment
    ------------+--------------+-------+---------
     custkey    | bigint       |       |
     name       | varchar(25)  |       |
     address    | varchar(40)  |       |
     nationkey  | bigint       |       |
     phone      | varchar(15)  |       |
     acctbal    | double       |       |
     mktsegment | varchar(10)  |       |
     comment    | varchar(117) |       |
    (8 rows)

Run a SQL script
~~~~~~~~~~~~~~~~

From the interactive terminal you can run SQL queries. Run:

.. code-block:: shell

    trino> SELECT custkey, name, phone, acctbal FROM tpch.sf100.customer LIMIT 7;

Which returns:

.. code-block:: text

     custkey |        name        |      phone      | acctbal 
    ---------+--------------------+-----------------+---------
     3750001 | Customer#003750001 | 17-219-461-2765 | 3711.02 
     3750002 | Customer#003750002 | 18-659-357-4460 | -966.64 
     3750003 | Customer#003750003 | 21-489-373-2061 | 9557.01 
     3750004 | Customer#003750004 | 29-489-412-3729 |  742.49 
     3750005 | Customer#003750005 | 28-522-477-1174 | 2915.28 
     3750006 | Customer#003750006 | 25-234-691-1349 | 1011.81 
     3750007 | Customer#003750007 | 27-555-235-7461 | 8396.42 
    (7 rows)

Call Trino from terminal
-------------------------

Using the Trino Interactive CLI isn't required. You can call Trino
directly from your terminal session. Let's look at a few
examples.

Pass SQL queries to Trino
~~~~~~~~~~~~~~~~~~~~~~~~~

In the previous section, you learned how to run a SQL query from the
Trino Interactive CLI. You can also pass a query directly to Trino. From
the terminal, run:

.. code-block:: shell

    docker exec -it trino trino --execute 'SELECT custkey, name, phone, acctbal FROM tpch.sf100.customer LIMIT 7'

Which returns:

.. code-block:: text

     custkey |        name        |      phone      | acctbal 
    ---------+--------------------+-----------------+---------
     3750001 | Customer#003750001 | 17-219-461-2765 | 3711.02 
     3750002 | Customer#003750002 | 18-659-357-4460 | -966.64 
     3750003 | Customer#003750003 | 21-489-373-2061 | 9557.01 
     3750004 | Customer#003750004 | 29-489-412-3729 |  742.49 
     3750005 | Customer#003750005 | 28-522-477-1174 | 2915.28 
     3750006 | Customer#003750006 | 25-234-691-1349 | 1011.81 
     3750007 | Customer#003750007 | 27-555-235-7461 | 8396.42 
    (7 rows)

Run SQL scripts with Trino
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two TPCH scripts are included with the sample files for the `O’Reilly
book Presto: The Definitive
Guide <https://www.starburst.io/oreilly-presto-guide-download/>`__.

To use these scripts, download the book’s samples from their `GitHub
location <https://github.com/trinodb/presto-the-definitive-guide>`__
either as a zip file or a git clone.

.. code-block:: shell

    docker exec -it trino trino -f filename.sql

Next steps
----------

Learn more about deployment options, see :doc:`Deploying Trino </installation/deployment>`.