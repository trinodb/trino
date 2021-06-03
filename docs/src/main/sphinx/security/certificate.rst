==========================
Certificate authentication
==========================

You can configure Trino to support client-provided certificates validated by the
Trino server on initial connection.

.. important::

    This authentication method is only provided to support sites that have an
    absolute requirement for client authentication *and already have* client
    certificates for each client. Sites in this category have an existing PKI
    infrastructure, possibly including an onsite Certificate Authority.

    This feature is not appropriate for sites that would need to generate a set
    of client certificates in order to use this authentication method. Consider
    instead using another :doc:`authentication method
    </security/authentication-types>`.

Using certificate authentication
--------------------------------

All clients connecting with HTTPS/TLS go through the following initial steps:

* The client attempts to contact the coordinator.
* The coordinator returns its certificate to the client.
* The client validates the server's certificate using the client's trust store.

A cluster with certificate authentication enabled goes through the following
additional steps:

* The coordinator asks the client for its certificate.
* The client responds with its certificate.
* The coordinator verifies the client's certificate, using the coordinator's
  trust store.

Several rules emerge from these steps:

1. Trust stores used by clients must include the certificate of the signer of
   the coordinator's certificate.
2. Trust stores used by coordinators must include the certificate of the signer
   of client certificates.
3. The trust stores used by the coordinator and clients do not need to be the
   same.
4. The certificate that verifies the coordinator does not need to be the same as
   the certificate verifying clients.

Trino validates based on the distinguished name (DN) from the X.509 ``Subject``
field. The principal created for the connection is the common name (CN) from the
``Subject`` field. You can use :doc:`User mapping </security/user-mapping>` to
map the CN to a Trino user name.

There are three levels of client certificate support possible. From the point of
view of the server:

* The server does not require a certificate from clients.
* The server asks for a certificate from clients, but allows connection without one.
* The server must have a certificate from clients to allow connection.

Trino's client certificate support is the middle type, asks for a certificate
but allows connection if another authentication method passes.

Certificate authentication configuration
----------------------------------------

Enable certificate authentication by setting the :doc:`Certificate
authentication type <authentication-types>` in :ref:`etc/config.properties
<config_properties>`:

.. code-block:: properties

    http-server.authentication.type=CERTIFICATE

You can specify certificate authentication along with another authenticaton
method, such as ``PASSWORD``. In this case, authentication is performed in the
order of entries, and the first successful authentication results in access.

.. code-block:: properties

    http-server.authentication.type=CERTIFICATE,PASSWORD

The following configuration properties are also available:

.. list-table:: Configuration properties
   :widths: 50 50
   :header-rows: 1

   * - Property
     - Description
   * - ``http-server.authentication.certificate.user-mapping.pattern``
     -  A regular expression pattern to :doc:`map all user names
        </security/user-mapping>` for this authentication system to the format
        expected by the Trino server.
   * - ``http-server.authentication.certificate.user-mapping.file``
     - The path to a JSON file that contains a set of :doc:`user mapping
       rules </security/user-mapping>` for this authentication system.

Use certificate authencation with clients
-----------------------------------------

When using the Trino :doc:`CLI </installation/cli>`, specify the
``--keystore-path`` and ``--keystore-password`` options as described
in :ref:`cli-certificate-auth`.

When using the Trino :doc:`JDBC driver </installation/jdbc>` to connect to a
cluster with certificate authentication enabled, use the ``SSLKeyStoreType`` and
``SSLKeyStorePassword`` :ref:`parameters <jdbc-parameter-reference>` to specify
the path to the client's certificate and its password, if any.
