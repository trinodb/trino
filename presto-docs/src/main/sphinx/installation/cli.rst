======================
Command Line Interface
======================

The Presto CLI provides a terminal-based, interactive shell for running
queries. The CLI is a
`self-executing <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
JAR file, which means it acts like a normal UNIX executable.

.. contents::
    :local:

Requirements
------------

The CLI requires a Java virtual machine available on the path. It can be used
with Java version 8 and higher.

Installation
------------

Download :maven_download:`cli`, rename it to ``presto``,
make it executable with ``chmod +x``, then run it:

.. code-block:: none

    ./presto --server localhost:8080 --catalog hive --schema default

Run the CLI with the ``--help`` option to see the available options.

Authentication
--------------

You can use your authentication details with the ``--user`` and ``-password``
options to connect to a secured Presto.

The built-in documentation includes further help for passing keystore,
truststore, and other authentication details as required. Using the CLI with
Kerberos is :doc:`documented separately </security/cli>`.

Pagination
----------

By default, the results of queries are paginated using the ``less`` program
which is configured with a carefully selected set of options. This behavior can
be overridden by setting the environment variable ``PRESTO_PAGER`` to the name
of a different program such as ``more`` or `pspg
<https://github.com/okbob/pspg>`_, or set it to an empty value to completely
disable pagination.
