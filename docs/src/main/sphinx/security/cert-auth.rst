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

The validation sequence with certificate authentication includes the following
steps:

* A client verfies the coordinator's certificate, and makes an attempt to
  connect to the coordinator.
* The coordinator asks the client for its certificate.
* The client responds with *its* certificate, validated by *its* trust store.
* The coordinator verifies the certificate against that trust store.

The server’s trust store must include the certificate for the signer of the
client certificates, and the client’s trust store must include the certificate
for the signer of the server’s certificate. They do not need to be the same
signer certificate.

Trino validates based on the distinguished name (DN) from the X.509 Subject
field. The principal created for the connection is the common name (CN) from the
Subject field. You can use :doc:`User mapping </security/user-mapping>` to map
the CN to a Trino user name.

There are three levels of client certificate support possible. From the point of
view of the server:

* I do not need a certificate from clients
* I would like to have a certificate
* I must have a certificate

Trino's client certificate support is the middle one, "would like to have."

Certificate authentication configuration
----------------------------------------

Enable certificate authentication by setting the :doc:`Certificate
authentication type <authentication-types>` in ``etc/config.properties``:

.. code-block:: properties

    http-server.authentication.type=CERTIFICATE

You can specify certificate authentication along with another authenticaton
method, such as PASSWORD. In this case, authentication is performed in the order
of entries, and the first successful authentication results in access.

.. code-block:: properties

    http-server.authentication.type=CERTIFICATE,PASSWORD

The following configuration properties for ``etc/config.properties`` are also
available:

.. list-table:: Configuration properties for JWT authentication
   :widths: 50 50
   :header-rows: 1

   * - Property
     - Description
   * - ``http-server.authentication.certificate.user-mapping.pattern``
     - Optional. A regular expression pattern to map all user names for this
       authentication system to the format expected by the Trino server. See
       :doc:`User mapping </security/user-mapping>`.
   * - ``http-server.authentication.certificate.user-mapping.file``
     - Optional. The path to a JSON file that contains a set of user mapping
       rules for this authentication system. See :doc:`User
       mapping </security/user-mapping>`.

Trino CLI arguments
-------------------

The Trino :doc:`CLI </installation/cli>` provides the following command line
arguments to allow the CLI to be used with client certificate authentication.

.. list-table:: CLI options for client certificates
   :widths: 35 65
   :header-rows: 1

   * - Option
     - Description
   * - ``--keystore-path=<path>``
     - Keystore path to a PEM or JKS file, which must include a certificate and
       private key for the client.
   * - ``--keystore-password=<password>``
     - Only required if the key has a password.
   * - ``--keystore-type=<type>``
     - Rarely used. Specifies the service name for the keystore type in the JVM.
       You can query the JVM for its list of supported key types.

The three ``--truststore`` related options are used to specify the trust chain
for the *server's* certificate, and are not part of client certificate
authentication for the CLI.
