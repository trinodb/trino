====================
Authentication types
====================

Trino supports multiple authentication types to ensure all users of the system
are authenticated. Different authenticators allow user management in one or more
systems. Using :doc:`TLS <tls>` is required for all authentications types.

You can configure one or more authentication types with the
``http-server.authentication.type`` property. The following authentication types
and authenticators are available:

* ``PASSSWORD`` for  :doc:`password-file`, :doc:`ldap`, and :doc:`salesforce`
* ``OAUTH2`` for :doc:`oauth2`
* ``CERTIFICATE`` for certificate authentication
* ``JWT`` for Java Web Token (JWT) authentication
* ``KERBEROS`` for :doc:`kerberos`

Get started with a basic password authentication configuration backed by a
:doc:`password file <password-file>`:

.. code-block:: properties

    http-server.authentication.type=PASSWORD


Multiple authentication types
-----------------------------

You can use multiple authentication types, separated with commas in the
configuration:

.. code-block:: properties

    http-server.authentication.type=PASSWORD,CERTIFICATE


Authentication is performed in order of the entries, and first successful
authentication results in access, using the :doc:`mapped user <user-mapping>`
from that authentication method.

Multiple password authenticators
--------------------------------

You can use multiple password authenticator types by referencing multiple
configuration files:

.. code-block:: properties

    http-server.authentication.type=PASSWORD
    password-authenticator.config-files=etc/ldap1.properties,etc/ldap2.properties,etc/password.properties

In the preceding example, the configuration files ``ldap1.properties`` and
``ldap2.properties`` are regular :doc:`LDAP authenticator configuration files
<ldap>`. The ``password.properties`` is a :doc:`password file authenticator
configuration file <password-file>`.

Relative paths to the installation directory or absolute paths can be used.

User authentication credentials are first validated against the LDAP server from
``ldap1``, then the separate server from ``ldap2``, and finally the password
file. First successful authentication results in access, and no further
authenticators are called.

