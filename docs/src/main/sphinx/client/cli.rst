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

The CLI uses the :doc:`Trino client REST API </develop/client-protocol>` over
HTTP/HTTPS to communicate with the coordinator on the cluster.

The CLI version should be identical to the version of the Trino cluster, or
newer. Older versions typically work, but only a subset is regularly tested.
Versions before 350 are not supported.

.. _cli-installation:

Installation
------------

Download :maven_download:`cli`, rename it to ``trino``, make it executable with
``chmod +x``, and run it to show the version of the CLI:

.. code-block:: text

    ./trino --version

Run the CLI with ``--help`` or ``-h`` to see all available options.

Windows users, and users unable to execute the preceeding steps, can use the
equivalent ``java`` command with the ``-jar`` option to run the CLI, and show
the version:

.. code-block:: text

    java -jar trino-cli-*-executable.jar --version

The syntax can be used for the examples in the following sections. In addition,
using the ``java`` command allows you to add configuration options for the Java
runtime with the ``-D`` syntax. You can use this for debugging and
troubleshooting, such as when :ref:`specifying additional Kerberos debug options
<cli-kerberos-debug>`.

Running the CLI
---------------

The minimal command to start the CLI in interactive mode specifies the URL of
the coordinator in the Trino cluster:

.. code-block:: text

    ./trino --server http://trino.example.com:8080

If successful, you will get a prompt to execute commands. Use the ``help``
command to see a list of supported commands. Use the ``clear`` command to clear
the terminal. To stop and exit the CLI, run ``exit`` or ``quit``.:

.. code-block:: text

    trino> help

    Supported commands:
    QUIT
    EXIT
    CLEAR
    EXPLAIN [ ( option [, ...] ) ] <query>
        options: FORMAT { TEXT | GRAPHVIZ | JSON }
                TYPE { LOGICAL | DISTRIBUTED | VALIDATE | IO }
    DESCRIBE <table>
    SHOW COLUMNS FROM <table>
    SHOW FUNCTIONS
    SHOW CATALOGS [LIKE <pattern>]
    SHOW SCHEMAS [FROM <catalog>] [LIKE <pattern>]
    SHOW TABLES [FROM <schema>] [LIKE <pattern>]
    USE [<catalog>.]<schema>

You can now run SQL statements. After processing, the CLI will show results and
statistics.

.. code-block:: text

  trino> SELECT count(*) FROM tpch.tiny.nation;

  _col0
  -------
      25
  (1 row)

  Query 20220324_213359_00007_w6hbk, FINISHED, 1 node
  Splits: 13 total, 13 done (100.00%)
  2.92 [25 rows, 0B] [8 rows/s, 0B/s]

As part of starting the CLI, you can set the default catalog and schema. This
allows you to query tables directly without specifying catalog and schema.

.. code-block:: text

  ./trino --server http://trino.example.com:8080 --catalog tpch --schema tiny

  trino:tiny> SHOW TABLES;

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

You can also set the default catalog and schema with the :doc:`/sql/use`
statement.

.. code-block:: text

  trino> USE tpch.tiny;
  USE
  trino:tiny>

Many other options are available to further configure the CLI in interactive
mode:

.. list-table::
  :widths: 40, 60
  :header-rows: 1

  * - Option
    - Description
  * - ``--catalog``
    - Sets the default catalog. You can change the default catalog and schema
      with :doc:`/sql/use`.
  * - ``--client-info``
    - Adds arbitrary text as extra information about the client.
  * - ``--client-request-timeout``
    - Sets the duration for query processing, after which, the client request is
      terminated. Defaults to ``2m``.
  * - ``--client-tags``
    - Adds extra tags information about the client and the CLI user. Separate
      multiple tags with commas. The tags can be used as input for
      :doc:`/admin/resource-groups`.
  * - ``--debug``
    - Enables display of debug information during CLI usage for
      :ref:`cli-troubleshooting`. Displays more information about query
      processing statistics.
  * - ``--disable-auto-suggestion``
    - Disables autocomplete suggestions.
  * - ``--disable-compression``
    - Disables compression of query results.
  * - ``--editing-mode``
    - Sets key bindings in the CLI to be compatible with VI or
      EMACS editors. Defaults to ``EMACS``.
  * - ``--http-proxy``
    - Configures the URL of the HTTP proxy to connect to Trino.
  * - ``--history-file``
    - Path to the :ref:`history file <cli-history>`. Defaults to ``~/.trino_history``.
  * - ``--network-logging``
    - Configures the level of detail provided for network logging of the CLI.
      Defaults to ``NONE``, other options are ``BASIC``, ``HEADERS``, or
      ``BODY``.
  * - ``--output-format-interactive=<format>``
    - Specify the :ref:`format <cli-output-format>` to use
      for printing query results. Defaults to ``ALIGNED``.
  * - ``--pager=<pager>``
    - Path to the pager program used to display the query results. Set to
      an empty value to completely disable pagination. Defaults to ``less``
      with a carefully selected set of options.
  * - ``--no-progress``
    - Do not show query processing progress.
  * - ``--password``
    - Prompts for a password. Use if your Trino server requires password
      authentication. You can set the ``TRINO_PASSWORD`` environment variable
      with the password value to avoid the prompt. For more information, see :ref:`cli-username-password-auth`.
  * - ``--schema``
    - Sets the default schema. You can change the default catalog and schema
      with :doc:`/sql/use`.
  * - ``--server``
    - The HTTP/HTTPS address and port of the Trino coordinator. The port must be
      set to the port the Trino coordinator is listening for connections on.
      Trino server location defaults to ``http://localhost:8080``.
  * - ``--session``
    - Sets one or more :ref:`session properties
      <session-properties-definition>`. Property can be used multiple times with
      the format ``session_property_name=value``.
  * - ``--socks-proxy``
    - Configures the URL of the SOCKS proxy to connect to Trino.
  * - ``--source``
    - Specifies the name of the application or source connecting to Trino.
      Defaults to ``trino-cli``. The value can be used as input for
      :doc:`/admin/resource-groups`.
  * - ``--timezone``
    - Sets the time zone for the session using the `time zone name
      <https://wikipedia.org/wiki/List_of_tz_database_time_zones>`_. Defaults
      to the timezone set on your workstation.
  * - ``--user``
    - Sets the username for :ref:`cli-username-password-auth`. Defaults to your
      operating system username. You can override the default username,
      if your cluster uses a different username or authentication mechanism.

.. _cli-tls:

TLS/HTTPS
---------

Trino is typically available with an HTTPS URL. This means that all network
traffic between the CLI and Trino uses TLS. :doc:`TLS configuration
</security/tls>` is common, since it is a requirement for :ref:`any
authentication <cli-authentication>`.

Use the HTTPS URL to connect to the server:

.. code-block:: text

    ./trino --server https://trino.example.com

The recommended TLS implementation is to use a globally trusted certificate. In
this case, no other options are necessary, since the JVM running the CLI
recognizes these certificates.

Use the options from the following table to further configure TLS and
certificate usage:

.. list-table::
  :widths: 40, 60
  :header-rows: 1

  * - Option
    - Description
  * - ``--insecure``
    - Skip certificate validation when connecting with TLS/HTTPS (should only be
      used for debugging).
  * - ``--keystore-path``
    - The location of the Java Keystore file that contains the certificate of
      the server to connect with TLS.
  * - ``--keystore-password``
    - The password for the keystore. This must match the password you specified
      when creating the keystore.
  * - ``--keystore-type``
    - Determined by the keystore file format. The default keystore type is JKS.
      This advanced option is only necessary if you use a custom Java
      Cryptography Architecture (JCA) provider implementation.
  * - ``--truststore-password``
    - The password for the truststore. This must match the password you
      specified when creating the truststore.
  * - ``--truststore-path``
    - The location of the Java truststore file that will be used to secure TLS.
  * - ``--truststore-type``
    - Determined by the truststore file format. The default keystore type is
      JKS. This advanced option is only necessary if you use a custom Java
      Cryptography Architecture (JCA) provider implementation.
  * - ``--use-system-truststore``
    - Verify the server certificate using the system truststore of the
      operating system. Windows and macOS are supported. For other operating
      systems, the default Java truststore is used. The truststore type can
      be overridden using ``--truststore-type``.

.. _cli-authentication:

Authentication
--------------

The Trino CLI supports many :doc:`/security/authentication-types` detailed in
the following sections:

.. _cli-username-password-auth:

Username and password authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Username and password authentication is typically configured in a cluster using
the ``PASSWORD`` :doc:`authentication type </security/authentication-types>`,
for example with :doc:`/security/ldap` or :doc:`/security/password-file`.

The following code example connects to the server, establishes your user name,
and prompts the CLI for your password:

.. code-block:: text

  ./trino --server https://trino.example.com --user=exampleusername --password

Alternatively, set the password as the value of the ``TRINO_PASSWORD``
environment variables. Typically use single quotes to avoid problems with
special characters such as ``$``:

.. code-block:: text

  export TRINO_PASSWORD='LongSecurePassword123!@#'

The password is automatically used in any following start of the CLI:

.. code-block:: text

  ./trino --server https://trino.example.com --user=exampleusername

.. _cli-external-sso-auth:

External authentication - SSO
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the ``--external-authentication`` option for browser-based SSO
authentication, as detailed in :doc:`/security/oauth2`. With this configuration,
the CLI displays a URL that you must open in a web browser for authentication.

The detailed behavior is as follows:

* Start the CLI with the ``--external-authentication`` option and execute a
  query.
* The CLI starts and connects to Trino.
* A message appears in the CLI directing you to open a browser with a specified
  URL when the first query is submitted.
* Open the URL in a browser and follow through the authentication process.
* The CLI automatically receives a token.
* When successfully authenticated in the browser, the CLI proceeds to execute
  the query.
* Further queries in the CLI session do not require additional logins while the
  authentication token remains valid. Token expiration depends on the external
  authentication type configuration.
* Expired tokens force you to log in again.

.. _cli-certificate-auth:

Certificate authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the following CLI arguments to connect to a cluster that uses
:doc:`certificate authentication </security/certificate>`.

.. list-table:: CLI options for certificate authentication
   :widths: 35 65
   :header-rows: 1

   * - Option
     - Description
   * - ``--keystore-path=<path>``
     - Absolute or relative path to a :doc:`PEM </security/inspect-pem>` or
       :doc:`JKS </security/inspect-jks>` file, which must contain a certificate
       that is trusted by the Trino cluster you are connecting to.
   * - ``--keystore-password=<password>``
     - Only required if the keystore has a password.

The truststore related options are independent of client certificate
authentication with the CLI; instead, they control the client's trust of the
server's certificate.

.. _cli-jwt-auth:

JWT authentication
^^^^^^^^^^^^^^^^^^

To access a Trino cluster configured to use :doc:`/security/jwt`, use the
``--access-token=<token>`` option to pass a JWT to the server.

.. _cli-kerberos-auth:

Kerberos authentication
^^^^^^^^^^^^^^^^^^^^^^^

The Trino CLI can connect to a Trino cluster that has :doc:`/security/kerberos`
enabled.

Invoking the CLI with Kerberos support enabled requires a number of additional
command line options. You also need the :ref:`Kerberos configuration files
<server_kerberos_principals>` for your user on the machine running the CLI. The
simplest way to invoke the CLI is with a wrapper script:

.. code-block:: text

    #!/bin/bash

    ./trino \
      --server https://trino.example.com \
      --krb5-config-path /etc/krb5.conf \
      --krb5-principal someuser@EXAMPLE.COM \
      --krb5-keytab-path /home/someuser/someuser.keytab \
      --krb5-remote-service-name trino

When using Kerberos authentication, access to the Trino coordinator must be
through :doc:`TLS and HTTPS </security/tls>`.

The following table lists the available options for Kerberos authentication:

.. list-table:: CLI options for Kerberos authentication
  :widths: 40, 60
  :header-rows: 1

  * - Option
    - Description
  * - ``--krb5-config-path``
    - Path to Kerberos configuration files.
  * - ``--krb5-credential-cache-path``
    - Kerberos credential cache path.
  * - ``--krb5-disable-remote-service-hostname-canonicalization``
    - Disable service hostname canonicalization using the DNS reverse lookup.
  * - ``--krb5-keytab-path``
    - The location of the keytab that can be used to authenticate the principal
      specified by ``--krb5-principal``.
  * - ``--krb5-principal``
    - The principal to use when authenticating to the coordinator.
  * - ``--krb5-remote-service-name``
    - Trino coordinator Kerberos service name.
  * - ``--krb5-service-principal-pattern``
    - Remote kerberos service principal pattern. Defaults to
      ``${SERVICE}@${HOST}``.

.. _cli-kerberos-debug:

Additional Kerberos debugging information
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can enable additional Kerberos debugging information for the Trino CLI
process by passing ``-Dsun.security.krb5.debug=true``,
``-Dtrino.client.debugKerberos=true``, and
``-Djava.security.debug=gssloginconfig,configfile,configparser,logincontext``
as a JVM argument when :ref:`starting the CLI process <cli-installation>`:

.. code-block:: text

    java \
      -Dsun.security.krb5.debug=true \
      -Djava.security.debug=gssloginconfig,configfile,configparser,logincontext \
      -Dtrino.client.debugKerberos=true \
      -jar trino-cli-*-executable.jar \
      --server https://trino.example.com \
      --krb5-config-path /etc/krb5.conf \
      --krb5-principal someuser@EXAMPLE.COM \
      --krb5-keytab-path /home/someuser/someuser.keytab \
      --krb5-remote-service-name trino

For help with interpreting Kerberos debugging messages, see :ref:`additional
resources <kerberos-debug>`.

Pagination
----------

By default, the results of queries are paginated using the ``less`` program
which is configured with a carefully selected set of options. This behavior
can be overridden by setting the ``--pager`` option or
the ``TRINO_PAGER`` environment variable to the name of a different program
such as ``more`` or `pspg <https://github.com/okbob/pspg>`_,
or it can be set to an empty value to completely disable pagination.

.. _cli-history:

History
-------

The CLI keeps a history of your previously used commands. You can access your
history by scrolling or searching. Use the up and down arrows to scroll and
:kbd:`Control+S` and :kbd:`Control+R` to search. To execute a query again,
press :kbd:`Enter`.

By default, you can locate the Trino history file in ``~/.trino_history``.
Use the ``--history-file`` option or the ```TRINO_HISTORY_FILE`` environment variable
to change the default.

Auto suggestion
^^^^^^^^^^^^^^^

The CLI generates autocomplete suggestions based on command history.

Press :kbd:`→` to accept the suggestion and replace the current command line
buffer. Press :kbd:`Ctrl+→` (:kbd:`Option+→` on Mac) to accept only the next
keyword. Continue typing to reject the suggestion.

Configuration file
------------------

The CLI can read default values for all options from a file. It uses the first
file found from the ordered list of locations:

* File path set as value of the ``TRINO_CONFIG`` environment variable.
* ``.trino_config`` in the current users home directory.
* ``$XDG_CONFIG_HOME/trino/config``.

For example, you could create separate configuration files with different
authentication options, like ``kerberos-cli.properties`` and ``ldap-cli.properties``.
Assuming they're located in the current directory, you can set the
``TRINO_CONFIG`` environment variable for a single invocation of the CLI by
adding it before the ``trino`` command:

.. code-block:: text

    TRINO_CONFIG=kerberos-cli.properties trino --server https://first-cluster.example.com:8443
    TRINO_CONFIG=ldap-cli.properties trino --server https://second-cluster.example.com:8443

In the preceding example, the default configuration files are not used.

You can use all supported options without the ``--`` prefix in the configuration
properties file. Options that normally don't take an argument are boolean, so
set them to either ``true`` or ``false``. For example:

.. code-block:: properties

    output-format-interactive=AUTO
    timezone=Europe/Warsaw
    user=trino-client
    network-logging=BASIC
    krb5-disable-remote-service-hostname-canonicalization=true

Batch mode
----------

Running the Trino CLI with the ``--execute``, ``--file``, or passing queries to
the standard input uses the batch (non-interactive) mode. In this mode
the CLI does not report progress, and exits after processing the supplied
queries. Results are printed in ``CSV`` format by default. You can configure
other formats and redirect the output to a file.

The following options are available to further configure the CLI in batch
mode:

.. list-table::
  :widths: 40, 60
  :header-rows: 1

  * - Option
    - Description
  * - ``--execute=<execute>``
    - Execute specified statements and exit.
  * - ``-f``, ``--file=<file>``
    - Execute statements from file and exit.
  * - ``--ignore-errors``
    - Continue processing in batch mode when an error occurs. Default is to
      exit immediately.
  * - ``--output-format=<format>``
    - Specify the :ref:`format <cli-output-format>` to use
      for printing query results. Defaults to ``CSV``.
  * - ``--progress``
    - Show query progress in batch mode. It does not affect the output,
      which, for example can be safely redirected to a file.

Examples
^^^^^^^^

Consider the following command run as shown, or with the
``--output-format=CSV`` option, which is the default for non-interactive usage:

.. code-block:: text

    trino --execute 'SELECT nationkey, name, regionkey FROM tpch.sf1.nation LIMIT 3'

The output is as follows:

.. code-block:: text

    "0","ALGERIA","0"
    "1","ARGENTINA","1"
    "2","BRAZIL","1"

The output with the ``--output-format=JSON`` option:

.. code-block:: json

    {"nationkey":0,"name":"ALGERIA","regionkey":0}
    {"nationkey":1,"name":"ARGENTINA","regionkey":1}
    {"nationkey":2,"name":"BRAZIL","regionkey":1}

The output with the ``--output-format=ALIGNED`` option, which is the default
for interactive usage:

.. code-block:: text

    nationkey |   name    | regionkey
    ----------+-----------+----------
            0 | ALGERIA   |         0
            1 | ARGENTINA |         1
            2 | BRAZIL    |         1

The output with the ``--output-format=VERTICAL`` option:

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

The preceding command with ``--output-format=NULL`` produces no output.
However, if you have an error in the query, such as incorrectly using
``region`` instead of ``regionkey``, the command has an exit status of 1
and displays an error message (which is unaffected by the output format):

.. code-block:: text

    Query 20200707_170726_00030_2iup9 failed: line 1:25: Column 'region' cannot be resolved
    SELECT nationkey, name, region FROM tpch.sf1.nation LIMIT 3

.. _cli-output-format:

Output formats
--------------

The Trino CLI provides the options ``--output-format``
and ``--output-format-interactive`` to control how the output is displayed.
The available options shown in the following table must be entered
in uppercase. The default value is ``ALIGNED`` in interactive mode,
and ``CSV`` in non-interactive mode.

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
  * - ``AUTO``
    - Same as ``ALIGNED`` if output would fit the current terminal width,
      and ``VERTICAL`` otherwise.
  * - ``NULL``
    - Suppresses normal query results. This can be useful during development
      to test a query's shell return code or to see whether it results in
      error messages.

.. _cli-troubleshooting:

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
