======================
Command line interface
======================

The Trino CLI provides a terminal-based, interactive shell for running
queries. The CLI is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file, which means it acts like a normal UNIX executable.

Requirements
------------

The CLI requires a Java virtual machine available on the path.
It can be used with Java version 8 and higher.

Installation
------------

Download :maven_download:`cli`, rename it to ``trino``,
make it executable with ``chmod +x``, then run it:

.. code-block:: text

    ./trino --server localhost:8080 --catalog hive --schema default

Run the CLI with the ``--help`` option to see the available options.

The CLI uses the HTTP protocol and the
:doc:`Trino client REST API </develop/client-protocol>` to communicate
with Trino.

TLS/HTTPS
---------

Trino is typically available with an HTTPS URL. This means that all network
traffic between the CLI and Trino uses TLS. :doc:`TLS configuration
</security/tls>` is common, since it is a requirement for any authentication.

Use the HTTPS URL to connect to the server:

.. code-block:: text

    ./trino --server https://trino.example.com

The recommended TLS implementation is to use a globally trusted certificate. In
this case, no other options are necessary, since the JVM running the CLI
recognizes these certificates.

When Trino is configured using custom certificate, invoking CLI requires additional options
such as ``--keystore-*`` or ``--truststore-*`` to secure TLS connection.
The simplest way to invoke the CLI is with a wrapper script.

Authentication
--------------
Password/LDAP Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can override your username with the ``--user`` option. It defaults to your
operating system username. If your Trino server requires password
authentication, use the ``--password`` option to have the CLI prompt for a
password. You can set the ``TRINO_PASSWORD`` environment variable with the
password value to avoid the prompt.

.. code-block:: text

    #!/bin/bash
    ./trino \
    --server https://trino-coordinator.example.com:8443 \
    --catalog <catalog> \
    --schema <schema> \
    --user <LDAP user> \
    --password

Trino CLI options
-----------------

=============================== =========================================================================
Option                          Description
=============================== =========================================================================
``--server``                    The address and port of the Trino coordinator.  The port must
                                be set to the port the Trino coordinator is listening for HTTPS
                                connections on. Trino CLI does not support using ``http`` scheme for
                                the URL when using LDAP authentication.
``--keystore-path``             The location of the Java Keystore file that will be used
                                to secure TLS.
``--keystore-password``         The password for the keystore. This must match the
                                password you specified when creating the keystore.
``--truststore-path``           The location of the Java truststore file that will be used
                                to secure TLS.
``--truststore-password``       The password for the truststore. This must match the
                                password you specified when creating the truststore.
``--user``                      The LDAP username. For Active Directory this should be your
                                ``sAMAccountName`` and for OpenLDAP this should be the ``uid`` of
                                the user. This is the username which is
                                used to replace the ``${USER}`` placeholder pattern in the properties
                                specified in ``config.properties``.
``--password``                  Prompts for a password for the ``user``.
=============================== =========================================================================

Use ``--help`` to see information about specifying the keystore, truststore, and
other authentication details as required. 

Pagination
----------

By default, the results of queries are paginated using the ``less`` program
which is configured with a carefully selected set of options. This behavior
can be overridden by setting the environment variable ``TRINO_PAGER`` to the
name of a different program such as ``more`` or `pspg <https://github.com/okbob/pspg>`_,
or it can be set to an empty value to completely disable pagination.

History
-------

The CLI keeps a history of your previously used commands. You can access your
history by scrolling or searching. Use the up and down arrows to scroll and
:kbd:`Control+S` and :kbd:`Control+R` to search. To execute a query again,
press :kbd:`Enter`.

By default, you can locate the Trino history file in ``~/.trino_history``.
Use the ``TRINO_HISTORY_FILE`` environment variable to change the default.

Output Formats
--------------

The Trino CLI provides the option ``--output-format`` to control how the output
is displayed when running in noninteractive mode. The available options shown in
the following table must be entered in uppercase. The default value is ``CSV``.

.. list-table:: Output format options
  :widths: 25, 75
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

.. code-block:: text

    trino --execute 'SELECT nationkey, name, regionkey FROM tpch.sf1.nation LIMIT 3'

The output is as follows:

.. code-block:: text

    "0","ALGERIA","0"
    "1","ARGENTINA","1"
    "2","BRAZIL","1"

The output with ``--output-format JSON`` is:

.. code-block:: json

    {"nationkey":0,"name":"ALGERIA","regionkey":0}
    {"nationkey":1,"name":"ARGENTINA","regionkey":1}
    {"nationkey":2,"name":"BRAZIL","regionkey":1}

The output with ``--output-format ALIGNED`` is:

.. code-block:: text

    nationkey |   name    | regionkey
    ----------+-----------+----------
            0 | ALGERIA   |         0
            1 | ARGENTINA |         1
            2 | BRAZIL    |         1

The output with ``--output-format VERTICAL`` is:

.. code-block:: text

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

The preceding command with ``--output-format NULL`` produces no output.
However, if you have an error in the query, such as incorrectly using
``region`` instead of ``regionkey``, the command has an exit status of 1
and displays an error message (which is unaffected by the output format):

.. code-block:: text

    Query 20200707_170726_00030_2iup9 failed: line 1:25: Column 'region' cannot be resolved
    SELECT nationkey, name, region FROM tpch.sf1.nation LIMIT 3

Troubleshooting
---------------

If something goes wrong, you see an error message:

.. code-block:: text

    $ trino
    trino> select count(*) from tpch.tiny.nations;
    Query 20200804_201646_00003_f5f6c failed: line 1:22: Table 'tpch.tiny.nations' does not exist
    select count(*) from tpch.tiny.nations

To view debug information, including the stack trace for failures, use the
``--debug`` option:

.. code-block:: text

    $ trino --debug
    trino> select count(*) from tpch.tiny.nations;
    Query 20200804_201629_00002_f5f6c failed: line 1:22: Table 'tpch.tiny.nations' does not exist
    io.trino.spi.TrinoException: line 1:22: Table 'tpch.tiny.nations' does not exist
    at io.trino.sql.analyzer.SemanticExceptions.semanticException(SemanticExceptions.java:48)
    at io.trino.sql.analyzer.SemanticExceptions.semanticException(SemanticExceptions.java:43)
    ...
    at java.base/java.lang.Thread.run(Thread.java:834)
    select count(*) from tpch.tiny.nations
