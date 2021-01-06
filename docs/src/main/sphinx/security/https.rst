=============
HTTPS and TLS
=============

Trino runs with no security configuration in its default installation. This
allows you to connect to the server using URLs that specify the HTTP protocol
with the :doc:`Trino CLI </installation/cli>`, the :doc:`Web UI
</admin/web-interface>`, or other clients.

This guide describes how to configure Trino to use `Transport Layer Security
(TLS) <https://en.wikipedia.org/wiki/Transport_Layer_Security>`_, and thereby
require the HTTPS protocol from client connections. All authentication and
authorization technologies supported by Trino require configuring TLS as the
foundation.

..  note::

    TLS is the successor to its now-deprecated predecessor, Secure Sockets Layer
    (SSL). This page uses the term TLS to refer to TLS and SSL.

Approaches
----------

To configure Trino with TLS support, there are two alternative paths to
consider:

* Use the :ref:`load balancer or proxy <https-load-balancer>` at your site
  or cloud environment to terminate HTTPS/TLS. This approach is the simplest and
  strongly preferred solution.

* Secure the :ref:`Trino server directly <https-trino>`. This requires you
  to obtain a valid certificate, and add it to the Trino coordinator's
  configuration.

.. _https-load-balancer:

Use a load balancer to terminate HTTPS/TLS
------------------------------------------

Your site or cloud environment may already have a load balancer or proxy server
configured and running with a valid TLS certificate. In this case, you can work
with your network administration group to set up your Trino server behind the
load balancer. The load balancer or proxy server accepts TLS connections and
forwards them to the Trino coordinator, which runs with default HTTP
configuration on the default port, 8080.

In this case, the Trino cluster is contained in a virtual private network. The
load balancer adds request headers with the original client information:

.. code-block:: text

    X-Forwarded-Proto: https
    X-Forwarded-Host: trino.example.com
    Forwarded:  for=10.0.16.42;proto=https;by=73.159.60.147

To enable processing of forwarded headers, :ref:`the config properties file
<config_properties>` must include the following:

.. code-block:: text

    http-server.process-forwarded=true

This completes any necessary configuration for using HTTPS with a load balancer.
Client tools can access Trino with the URL exposed by the load balancer.

.. _https-trino:

Secure Trino directly
----------------------

Instead of the preferred mechanism of :ref:`using an external load balancer
<https-load-balancer>`, you can secure the Trino coordinator itself. This
requires you to obtain and install a TLS certificate and configure Trino to use
it for any client connections.

Add a TLS certificate
^^^^^^^^^^^^^^^^^^^^^

Obtain a TLS certificate file (a cert) from one of the following sources:

* Your site's network administration group
* Your cloud environment provider
* A domain name registrar, such as Verisign or GoDaddy
* A certificate vendor
* A free certificate generator, such as `letsencrypt.org
  <https://letsencrypt.org/>`_ or `sslforfree.com
  <https://www.sslforfree.com/>`_
* A certificate generating command such as ``openssl`` or ``keytool``

Trino supports certificate files in :ref:`PEM (PKCS #8 and PKCS #12)
<pem_certs>` or :ref:`Java KeyStore (JKS) <server_java_keystore>` format. The
PEM format is preferred, and is easier to use.

Make sure you obtain a certificate that is certified by a recognized
certification authority (CA), or that you submit a self-generated certificate
to a CA for validation.

.. _inspect-pems:

Inspect PEM certificates
""""""""""""""""""""""""

When you receive your PEM format certificate, inspect it and verify that it
shows the correct information for your Trino server. First, use the ``cat``
command to view this plain text file. It should contain sections for a PRIVATE
KEY and at least one CERTIFICATE:

.. code-block:: text

    -----BEGIN PRIVATE KEY-----
    MIIEowIBAAKCAQEAwJL8CLeDFAHhZe3QOOF1vWt4Vuk9vyO38Y1y9SgBfB02b2jW
    ....
    -----END PRIVATE KEY-----
    -----BEGIN CERTIFICATE-----
    MIIDujCCAqICAQEwDQYJKoZIhvcNAQEFBQAwgaIxCzAJBgNVBAYTAlVTMRYwFAYD
    ....
    -----END CERTIFICATE-----
    -----BEGIN CERTIFICATE-----
    MIIDwjCCAqoCCQCxyqwZ9GK50jANBgkqhkiG9w0BAQsFADCBojELMAkGA1UEBhMC
    ....
    -----END CERTIFICATE-----

If your PEM or key file reports ``BEGIN ENCRYPTED PRIVATE KEY``, you must use a
password when invoking that key.

If you received separate PEM files, one for the server's key and another for its
certificate, concatenate the files into one, in order from key to cert.

.. code-block:: text

    cat key.pem cert.pem > server.pem

.. _validate-pems:

Validate key
""""""""""""

This page presumes your system uses OpenSSL 1.1 or later.

Test the key's validity with the following command:

.. code-block:: text

    openssl rsa -in server.pem -check -noout

Look for the following confirmation message:

.. code-block:: text

    RSA key ok

Validate certificate
""""""""""""""""""""

For the cert file, analyze it with a different ``openssl`` command:

.. code-block:: text

    openssl x509 -in server.pem -text -noout

If your cert was generated with a password, ``openssl`` prompts for it.

In the output of the ``openssl`` command, look for the following
characteristics:

* Modern browsers now enforce 398 days as the maximum validity period for a
  cert. Look for ``Not Before`` and ``Not After`` dates in the ``Validity``
  section of the output, and make sure the time span does not exceed 398
  days.
* If you received an x509 version 3 certificate, it may have a **Subject
  Alternative Name** field. If present, make sure this shows the DNS name of
  your server, such as ``DNS:trino.example.com``
* The legacy common name (CN) field is ignored by modern browsers, but is a good
  visual aid to distinguish certs. Example: ``CN=trino.example.com``

Invalid certs
"""""""""""""

If your cert does not pass validation, or does not show the expected information
upon inspection, contact the group or vendor who provided it.

.. _troubleshooting_keystore:

Inspect JKS keystores
"""""""""""""""""""""

The JKS system is provided by your Java installation. JKS keys are stored in a
keystore file.

Inspect the keystore file to make sure it contains the correct information
for your Trino server. Use the ``keytool`` command, which is installed as part
of Java, to retrieve information from your keystore container file:

.. code-block:: text

    keytool -list -v -keystore yourKeystore.jks

Keystores always require a password. If not provided on the ``keytool`` command
line, ``keytool`` prompts for the password.

Independent of the keystore's password, it is possible that the JKS key has its
own password. It is easiest to make sure these passwords are the same. If the
JKS key inside the keystore has a different password, you must specify it with
the ``keymanager.password`` configuration property described
below.

In the output of the ``keytool -list`` command, look for:

* The keystore must contain a private key, such as ``Entry type:
  PrivateKeyEntry``
* Depending on the origin of your keystore file, it *may* also contain a
  certificate. Example: ``Entry type: trustedCertEntry``
* Confirm that the ``Valid from ... until`` entry shows no more than 398 days.
* Verify that the subject alternative name, if present, matches your server's
  DNS name. Example:

  .. code-block:: text

      SubjectAlternativeName [
          DNSName:  trino.example.com
      ]

Import CA certificate into keystore
"""""""""""""""""""""""""""""""""""

If you generated a self-signed keystore, you must send that with a Certificate
Signing Request to a Certificate Authority. In return, you receive a certificate
keystore, possibly with ``.cer`` extension. Depending on the CA, you might
receive a certificate self-signed only by the issuing CA, or one signed by a
chain of validating CAs.

To create a certified keystore, you must import the certificate received from
the CA into the keystore, using the ``keytool ‑import`` command. The ``keytool``
command is complex and covers many cases. Please study the **Certificate
Chains** and **Importing Certificates** sections of the ``keytool`` man page for
instructions.

The JKS system works in conjunction with JKS truststore files, described in
:ref:`config_truststore`.

.. _cert-placement:

Place the certificate file
^^^^^^^^^^^^^^^^^^^^^^^^^^

There are no location requirements for the PEM or JKS certificate file as long
as the following applies:

* The location is visible from the server's configuration file location with a
  relative path reference from the server's root directory, or with an absolute
  path reference.
* The location is secure from copying or tampering by malicious actors.

You can place your PEM file or keystore file in the Trino server's ``etc``
directory, which allows you to use a relative path reference in configuration
files. However, this location might require you to keep track of the cert file
and move it to a new ``etc`` directory if you upgrade your Trino version.

.. _configure-https:

Configure the coordinator
^^^^^^^^^^^^^^^^^^^^^^^^^

On the coordinator, add the following lines to :ref:`the config properties file
<config_properties>` to enable HTTPS support for the server:

.. code-block:: text

    http-server.https.enabled=true
    http-server.https.port=8443
    http-server.https.keystore.path=etc/trinoExampleCom.pem

Possible alternatives for the third line include:

.. code-block:: text

    http-server.https.keystore.path=etc/trinoExampleCom.jks
    http-server.https.keystore.path=/usr/local/etc/trinoExampleCom.p12

Relative paths are relative to the Trino server's root directory. In a
``tar.gz`` installation, the root directory is one level above ``etc``.

Keystores always require a password, and PEM certs can optionally require one.
For these cases, add the following line to specify the keystore or PEM password.

.. code-block:: text

    http-server.https.keystore.key=<keystore-password>

If the JKS key itself inside the keystore has an independent password, specify
that with the following property:

.. code-block:: text

    http-server.https.keymanager.password=<key-password>

Restart the server and test connecting to it with a URL that begins with
``https://`` using the :doc:`Trino CLI </installation/cli>` or :doc:`Web UI
</admin/web-interface>`.

Trino disables HTTP access to the :doc:`Web UI </admin/web-interface>` when
HTTPS is enabled for the coordinator. Although not recommended, you can enable
it by setting:

.. code-block:: text

    http-server.authentication.allow-insecure-over-http=true

When configured to provide HTTPS connections as shown above, the server
continues to allow HTTP connections to all clients except the Web UI. When
you are certain HTTPS connections are stable and reliable from the clients of
interest, you can disable HTTP access:

.. code-block:: text

    http-server.http.enabled=false

However, there are configuration scenarios that require the server to respond to
HTTP requests for inter-node communication with worker nodes even with HTTPS
enabled for client access. In these cases, configure both server types as
``enabled=true``:

.. code-block:: text

    http-server.http.enabled=true
    http-server.https.enabled=true

.. _config_truststore:

Additional configuration for truststores
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The JKS truststore file is a list of Certificate Authorities trusted by Java to
validate private keys. The truststore file, ``cacerts``, is provided as part of
your Java installation.

A PEM format certificate usually contains a private key for your server plus a
validating certificate that is recognized by at least one Certificate Authority,
or a chain of CAs going back to a globally recognized CA. Thus, there is no need
to identify a local truststore when using a signed PEM certificate.

Keystores normally rely on the default location of the system truststore as
installed with your Java installation.

However, you may need to *temporarily* use a local CA to validate a self-signed
cert. Do not use a local CA in a production enviroment, but you might need one
during development of your cluster configuration while waiting for your
self-signed cert to return signed by a recognized CA. In this case, identify the
location of the local CA file with configuration properties like the following:

.. code-block:: text

    http-server.https.truststore.path=etc/tempca
    http-server.https.truststore.key=<truststore-password>

