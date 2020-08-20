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

History
-------

The CLI keeps a history of your previously used commands. You can access your
history by scrolling or searching. Use the up and down arrows to scroll and
:kbd:`Control+S` and :kbd:`Control+R` to search. To execute a query again,
press :kbd:`Enter`.

By default, you can locate the Presto history file in ``~/.presto_history``.
Use the ``PRESTO_HISTORY_FILE`` environment variable to change the default.

Troubleshooting
---------------

If something goes wrong, you see an error message:

.. code-block:: none

    $ presto
    presto> select count(*) from tpch.tiny.nations;
    Query 20200804_201646_00003_f5f6c failed: line 1:22: Table 'tpch.tiny.nations' does not exist
    select count(*) from tpch.tiny.nations

To view debug information, including the stack trace for failures, use the
``--debug`` option:

.. code-block:: none

    $ presto --debug 
    presto> select count(*) from tpch.tiny.nations;
    Query 20200804_201629_00002_f5f6c failed: line 1:22: Table 'tpch.tiny.nations' does not exist
    io.prestosql.spi.PrestoException: line 1:22: Table 'tpch.tiny.nations' does not exist
    at io.prestosql.sql.analyzer.SemanticExceptions.semanticException(SemanticExceptions.java:48)
    at io.prestosql.sql.analyzer.SemanticExceptions.semanticException(SemanticExceptions.java:43)      
    ... 
    at java.base/java.lang.Thread.run(Thread.java:834)
    select count(*) from tpch.tiny.nations