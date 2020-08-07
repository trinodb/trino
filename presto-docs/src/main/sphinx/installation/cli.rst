======================
Command Line Interface
======================

The Presto CLI provides a terminal-based, interactive shell for running
queries. The CLI is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file, which means it acts like a normal UNIX executable.

Requirements
------------

The CLI requires a Java virtual machine available on the path.
It can be used with Java version 8 and higher.

Installation
------------

Download :maven_download:`cli`, rename it to ``presto``,
make it executable with ``chmod +x``, then run it:

.. code-block:: none

    ./presto --server localhost:8080 --catalog hive --schema default

Run the CLI with the ``--help`` option to see the available options.

Authentication
--------------

You can override your username with the ``--user`` option. It defaults to your
operating system username. If your Presto server requires password
authentication, use the ``--password`` option to have the CLI prompt for a
password. You can set the ``PRESTO_PASSWORD`` environment variable with the
password value to avoid the prompt.

Use ``--help`` to see information about specifying the keystore, truststore, and
other authentication details as required. If using Kerberos, see :doc:`/security/cli`.

Pagination
----------

By default, the results of queries are paginated using the ``less`` program
which is configured with a carefully selected set of options. This behavior
can be overridden by setting the environment variable ``PRESTO_PAGER`` to the
name of a different program such as ``more`` or `pspg <https://github.com/okbob/pspg>`_,
or it can be set to an empty value to completely disable pagination.

Output Formats
--------------

The Presto CLI provides the option ``--output-format`` to control how the output
is displayed when running in noninteractive mode. The available options shown in
the following table must be entered in uppercase. The default value is ``CSV``.

.. list-table::
  :widths: 20, 80
  :header-rows: 1

  * - Option
    - Description
  * - ``CSV``
    - Comma-separated values, each value quoted. No header row.
  * - ``CSV_HEADER``
    - Comma-separated values, quoted with header row.
  * - ``CSV_UNQUOTED``
    - Comma-separated values without quotes.
  * - ``CSV_HEADER_UNQUOTED``
    - Comma-separated values with header row but no quotes.
  * - ``TSV``
    - Tab-separated values.
  * - ``TSV_HEADER``
    - Tab-separated values with header row.
  * - ``JSON``
    - Output rows emitted as JSON objects with name-value pairs.
  * - ``ALIGNED``
    - Output emitted as an ASCII character table with values.
  * - ``VERTICAL``
    - Output emitted as record-oriented top-down lines, one per value.
  * - ``NULL``
    - Suppresses normal query results. This can be useful during development
      to test a query's shell return code or to see whether it results in
      error messages.

Examples
^^^^^^^^

Consider the following command run as shown, or with ``--output-format CSV``:

.. code-block:: none

   presto --execute 'SELECT nationkey, name, regionkey FROM tpch.sf1.nation LIMIT 3'

The output is:

.. code-block:: none

  "0","ALGERIA","0"
  "1","ARGENTINA","1"
  "2","BRAZIL","1"

With ``--output-format JSON``:

.. code-block:: json

  {"nationkey":0,"name":"ALGERIA","regionkey":0}
  {"nationkey":1,"name":"ARGENTINA","regionkey":1}
  {"nationkey":2,"name":"BRAZIL","regionkey":1}

With ``--output-format ALIGNED``:

.. code-block:: none

   nationkey |   name    | regionkey
   ----------+-----------+----------
           0 | ALGERIA   |         0
           1 | ARGENTINA |         1
           2 | BRAZIL    |         1

With ``--output-format VERTICAL``:

.. code-block:: none

  -[ RECORD 1 ]--------
  nationkey | 0
  name      | ALGERIA
  regionkey | 0
  -[ RECORD 2 ]--------
  nationkey | 1
  name      | ARGENTINA
  regionkey | 1
  -[ RECORD 3 ]--------
  nationkey | 2
  name      | BRAZIL
  regionkey | 1

The preceding command with ``--output-format NULL`` returns 0, and no output
is emitted. If you incorrectly specify ``region`` instead of ``regionkey``, the
return value is 1 and the output is:

.. code-block:: none

  Query 20200707_170726_00030_2iup9 failed: line 1:25: Column 'region' cannot be resolved
  SELECT nationkey, name, region FROM tpch.sf1.nation LIMIT 3

