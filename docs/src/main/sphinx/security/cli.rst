===========================
CLI Kerberos authentication
===========================

The Trino :doc:`/installation/cli` can connect to a Trino cluster that has
:doc:`kerberos` enabled.

Environment configuration
-------------------------

.. |subject_node| replace:: client

.. include:: kerberos-services.fragment
.. include:: kerberos-configuration.fragment

Kerberos principals and keytab files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each user, who connects to the Trino coordinator, needs a Kerberos principal.
You need to create these users in Kerberos using `kadmin
<http://web.mit.edu/kerberos/krb5-latest/doc/admin/admin_commands/kadmin_local.html>`_.

Additionally, each user needs a `keytab file
<http://web.mit.edu/kerberos/krb5-devel/doc/basic/keytab_def.html>`_. The
keytab file can be created using :command:`kadmin` after you create the
principal.

.. code-block:: text

    kadmin
    > addprinc -randkey someuser@EXAMPLE.COM
    > ktadd -k /home/someuser/someuser.keytab someuser@EXAMPLE.COM

.. include:: ktadd-note.fragment

Configuration for TLS
^^^^^^^^^^^^^^^^^^^^^

When using Kerberos authentication, access to the Trino coordinator must be
through HTTPS. If you have not yet configured TLS/HTTPS for your coordinator,
refer to :doc:`HTTPS and TLS </security/tls>`.

Trino CLI execution
--------------------

In addition to the options that are required when connecting to a Trino
coordinator, that does not require Kerberos authentication, invoking the CLI
with Kerberos support enabled requires a number of additional command line
options. The simplest way to invoke the CLI is with a wrapper script.

.. code-block:: text

    #!/bin/bash

    ./trino \
      --server https://trino-coordinator.example.com:7778 \
      --krb5-config-path /etc/krb5.conf \
      --krb5-principal someuser@EXAMPLE.COM \
      --krb5-keytab-path /home/someuser/someuser.keytab \
      --krb5-remote-service-name trino \
      --keystore-path /tmp/trino.jks \
      --keystore-password password \
      --catalog <catalog> \
      --schema <schema>

=============================== =========================================================================
Option                          Description
=============================== =========================================================================
``--server``                    The address and port of the Trino coordinator.  The port must
                                be set to the port the Trino coordinator is listening for HTTPS
                                connections on.
``--krb5-config-path``          Kerberos configuration file.
``--krb5-principal``            The principal to use when authenticating to the coordinator.
``--krb5-keytab-path``          The location of the keytab that can be used to
                                authenticate the principal specified by ``--krb5-principal``
``--krb5-remote-service-name``  Trino coordinator Kerberos service name.
``--keystore-path``             The location of the Java Keystore file that is used
                                to secure TLS.
``--keystore-password``         The password for the keystore. This must match the
                                password you specified when creating the keystore.
=============================== =========================================================================

Troubleshooting
---------------

Many of the same steps, that can be used when troubleshooting the :ref:`Trino
coordinator <coordinator-troubleshooting>`, apply to troubleshooting the CLI.

Additional Kerberos debugging information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can enable additional Kerberos debugging information for the Trino CLI
process by passing ``-Dsun.security.krb5.debug=true``,
``-Dtrino.client.debugKerberos=true``, and
``-Djava.security.debug=gssloginconfig,configfile,configparser,logincontext``
as a JVM argument, when starting the CLI process. Doing so requires invoking
the CLI JAR via ``java`` instead of running the self-executable JAR directly.

.. code-block:: text

    #!/bin/bash

    java \
      -Dsun.security.krb5.debug=true \
      -Djava.security.debug=gssloginconfig,configfile,configparser,logincontext \
      -Dtrino.client.debugKerberos=true \
      -jar trino-cli-*-executable.jar \
      --server https://trino-coordinator.example.com:7778 \
      --krb5-config-path /etc/krb5.conf \
      --krb5-principal someuser@EXAMPLE.COM \
      --krb5-keytab-path /home/someuser/someuser.keytab \
      --krb5-remote-service-name trino \
      --keystore-path /tmp/trino.jks \
      --keystore-password password \
      --catalog <catalog> \
      --schema <schema>

The :ref:`additional resources <server_additional_resources>` listed in the
documentation for setting up Kerberos authentication for the Trino coordinator
may be of help when interpreting the Kerberos debugging messages.

See `Troubleshooting Security
<https://docs.oracle.com/en/java/javase/11/security/troubleshooting-security.html>`_
in the Java documentation for more details about
the ``-Djava.security.debug`` flag, and `Troubleshooting
<https://docs.oracle.com/en/java/javase/11/security/troubleshooting.html>`_
for more details about the Java GSS-API and Kerberos issues.
