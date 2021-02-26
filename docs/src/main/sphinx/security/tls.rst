=============
HTTPS and TLS
=============

Trino runs with no security by default. This allows you to connect to the server
using URLs that specify the HTTP protocol when using the Trino :doc:`CLI
</installation/cli>`, the :doc:`Web UI </admin/web-interface>`, or other
clients.

This topic describes how to configure your Trino server to use :ref:`TLS
<glossTLS>` to require clients to use the HTTPS connection protocol. All
authentication technologies supported by Trino require configuring TLS as the
foundational layer.

When configured to use TLS, a Trino server responds to client connections using
TLS 1.2 and TLS 1.3 certificates. The server rejects TLS 1.1, TLS 1.0, and all
SSL format certificates.

.. important::

    This page discusses only how to prepare the Trino server for secure client
    connections from outside of the Trino cluster to its coordinator.

See the :ref:`TLS Glossary <tlsglossary>` to clarify unfamiliar terms.

Approaches
----------

To configure Trino with TLS support, consider two alternative paths:

* Use the :ref:`load balancer or proxy <https-load-balancer>` at your site
  or cloud environment to terminate HTTPS/TLS. This approach is the simplest and
  strongly preferred solution.

* Secure the Trino :ref:`server directly <https-secure-directly>`. This
  requires you to obtain a valid certificate, and add it to the Trino
  coordinator's configuration.

.. _https-load-balancer:

Use a load balancer to terminate HTTPS/TLS
------------------------------------------

Your site or cloud environment may already have a :ref:`load balancer <glossLB>`
or proxy server configured and running with a valid, globally trusted TLS
certificate. In this case, you can work with your network administrators to set
up your Trino server behind the load balancer. The load balancer or proxy server
accepts TLS connections and forwards them to the Trino coordinator, which
typically runs with default HTTP configuration on the default port, 8080.

When a load balancer accepts a TLS encrypted connection, it adds a
`forwarded
<https://developer.mozilla.org/en-US/docs/Web/HTTP/Proxy_servers_and_tunneling#forwarding_client_information_through_proxies>`_
HTTP header to the request, such as:

.. code-block:: text

    X-Forwarded-Proto: https

This tells the Trino coordinator to process the connection as if a TLS
connection has already been successfully negotiated for it. This is why you do
not need to configure ``http-server.https.enabled=true`` for a coordinator
behind a load balancer.

However, to enable processing of such forwarded headers, the server's
:ref:`config properties file <config_properties>` *must* include the following:

.. code-block:: text

  http-server.process-forwarded=true

This completes any necessary configuration for using HTTPS with a load balancer.
Client tools can access Trino with the URL exposed by the load balancer.

.. _https-secure-directly:

Secure Trino directly
----------------------

Instead of the preferred mechanism of using an :ref:`external load balancer
<https-load-balancer>`, you can secure the Trino coordinator itself. This
requires you to obtain and install a TLS :ref:`certificate <glossCert>`, and
configure Trino to use it for client connections.

Add a TLS certificate
^^^^^^^^^^^^^^^^^^^^^

Obtain a TLS certificate file for use with your Trino server. Consider the
following types of certificates:

* **Globally trusted certificates** — A certificate that is automatically
  trusted by all browsers and clients. This is the easiest type to use because
  you do not need to configure clients. Obtain a certificate of this type from:

  *  A commercial certificate vendor
  *  Your cloud infrastructure provider
  *  A domain name registrar, such as Verisign or GoDaddy
  *  A free certificate generator, such as
     `letsencrypt.org <https://letsencrypt.org/>`_ or
     `sslforfree.com <https://www.sslforfree.com/>`_

* **Corporate trusted certificates** — A certificate trusted by browsers and
  clients in your organization. Typically, a site's IT department runs a local
  :ref:`certificate authority <glossCA>` and preconfigures clients and servers
  to trust this CA.

* **Generated self-signed certificates** — A certificate generated just for
  Trino that is not automatically trusted by any client. Before using, make sure
  you understand the :ref:`limitations of self-signed certificates
  <self_signed_limits>`.

The most convenient option and strongly recommended option is a globally trusted
certificate. It may require a little more work up front, but it is worth it to
not have to configure every single client.

Keys and certificates
^^^^^^^^^^^^^^^^^^^^^

Trino can read certificates and private keys encoded in PEM encoded PKCS #1, PEM
encoded PKCS #8, PKCS #12, and the legacy Java KeyStore (JKS) format.

Make sure you obtain a certificate that is validated by a recognized
:ref:`certificate authority <glossCA>`.

Inspect received certificates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before installing your certificate, inspect and validate the received key and
certificate files to make sure they reference the correct information to access
your Trino server. Much unnecessary debugging time is saved by taking the time
to validate your certificates before proceeding to configure the server.

Inspect PEM-encoded files as described in :doc:`Inspect PEM files
</security/inspect-pem>`.

Inspect PKCS # 12 and JKS keystores as described in :doc:`Inspect JKS files
</security/inspect-jks>`.

Invalid certificates
^^^^^^^^^^^^^^^^^^^^^

If your certificate does not pass validation, or does not show the expected
information on inspection, contact the group or vendor who provided it for a
replacement.

.. _cert-placement:

Place the certificate file
^^^^^^^^^^^^^^^^^^^^^^^^^^

There are no location requirements for a certificate file as long as:

* The file can be read by the Trino coordinator server process.
* The location is secure from copying or tampering by malicious actors.

You can place your file in the Trino coordinator's ``etc`` directory, which
allows you to use a relative path reference in configuration files. However,
this location can require you to keep track of the certificate file, and move it
to a new ``etc`` directory when you upgrade your Trino version.

.. _configure-https:

Configure the coordinator
^^^^^^^^^^^^^^^^^^^^^^^^^

On the coordinator, add the following lines to the :ref:`config properties file
<config_properties>` to enable TLS/HTTPS support for the server.

.. note::

  Legacy ``keystore`` and ``truststore`` wording is used in property names, even
  when directly using PEM-encoded certificates.

.. code-block:: text

  http-server.https.enabled=true
  http-server.https.port=8443
  http-server.https.keystore.path=etc/clustercoord.pem

Possible alternatives for the third line include:

.. code-block:: text

  http-server.https.keystore.path=etc/clustercoord.jks
  http-server.https.keystore.path=/usr/local/certs/clustercoord.p12

Relative paths are relative to the Trino server's root directory. In a
``tar.gz`` installation, the root directory is one level above ``etc``.

JKS keystores always require a password, while PEM format certificates can
optionally require a password. For cases where you need a password, add the
following line to the configuration.

.. code-block:: text

  http-server.https.keystore.key=<keystore-password>

It is possible for a key inside a keystore to have its own password,
independent of the keystore's password. In this case, specify the key's password
with the following property:

.. code-block:: text

  http-server.https.keymanager.password=<key-password>

When your Trino coordinator has an authenticator enabled along with HTTPS
enabled, HTTP access is automatically disabled for all clients, including the
:doc:`Web UI </admin/web-interface>`. Although not recommended, you can
re-enable it by setting:

.. code-block:: text

  http-server.authentication.allow-insecure-over-http=true

Test configuration
^^^^^^^^^^^^^^^^^^

To test your configuration settings, restart the server and try to connect to it
with the Trino :doc:`CLI </installation/cli>` or :doc:`Web UI
</admin/web-interface>`, using a URL that begins with ``https://``.

Now that TLS is configured, go back and :doc:`configure the authentication
</security>` method for your server.

.. _self_signed_limits:

Limitations of self-signed certificates
---------------------------------------

It is possible to generate a self-signed certificate with the ``openssl``,
``keytool``, or on Linux, ``certtool`` commands. Self-signed certificates can be
useful during development of a cluster for internal use only. We recommend never
using a self-signed certificate for a production Trino server.

Self-signed certificates are not trusted by anyone. They are typically created
by an administrator for expediency, because they do not require getting trust
signoff from anyone.

To use a self-signed certificate while developing your cluster requires:

* distributing to every client a local truststore that validates the certificate
* configuring every client to use this certificate

However, even with this client configuration, modern browsers reject these
certificates, which makes self-signed servers difficult to work with.

There is a difference between self-signed and unsigned certificates. Both types
are created with the same tools, but unsigned certificates are meant to be
forwarded to a CA with a Certificate Signing Request (CSR). The CA returns the
certificate signed by the CA and now globally trusted.

.. _tlsglossary:

TLS Glossary
------------

.. _glossCA:

CA
    Certificate Authority, a trusted organization that examines and validates
    organizations and their proposed server URIs, and issues digital
    certificates verified as valid for the requesting organization.

.. _glossCert:

Certificate
    A public key `certificate
    <https://en.wikipedia.org/wiki/Public_key_certificate>`_ issued by a CA,
    sometimes abbreviated as *cert*, that verifies the ownership of a server's
    keys. Certificate format is specified in the `X.509
    <https://en.wikipedia.org/wiki/X.509>`_ standard.

.. _glossJKS:

JKS
    Java KeyStore, the system of public key cryptography supported as one part
    of the Java security APIs. The legacy JKS system recognizes keys and
    certificates stored in *keystore* files, typically with the ``.jks``
    extension, and relies on a system-level list of CAs in *truststore* files
    installed as part of the current Java installation.

.. _glossKey:

Key
    A cryptographic key specified as a pair of public and private keys.

.. _glossLB:

Load Balancer (LB)
    Software or a hardware device that sits on a network's outer edge or
    firewall and accepts network connections on behalf of servers behind that
    wall. Load balancers carefully manage network traffic, and can accept TLS
    connections from incoming clients and pass those connections transparently
    to servers behind the wall.

.. _glossPEM:

PEM
    Privacy-Enhanced Mail, a syntax for private key information, and a content
    type used to store and send cryptographic keys and certificates. PEM format
    can contain both a key and its certificate, plus the chain of certificates
    from authorities back to the root :ref:`CA <glossCA>`, or back to a CA
    vendor's intermediate CA.

.. _glossPKCS12:

PKCS #12
    A binary archive used to store keys and certificates or certificate chains
    that validate a key. `PKCS #12 <https://en.wikipedia.org/wiki/PKCS_12>`_
    files have ``.p12`` or ``.pfx`` extensions.

SSL
    Secure Sockets Layer, now superceded by TLS, but still recognized as the
    term for what TLS does now.

.. _glossTLS:

TLS
    `Transport Layer Security
    <https://en.wikipedia.org/wiki/Transport_Layer_Security>`_ is the successor
    to SSL. These security topics use the term TLS to refer to both TLS and SSL.

