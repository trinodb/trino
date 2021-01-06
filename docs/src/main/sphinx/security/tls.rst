===================================
PEM, keystore, and truststore files
===================================

This guide describes the :ref:`PEM <pem_certs>` and :ref:`JKS
<server_java_keystore>` certificate formats accepted by Trino for client access
and for communication within the cluster.

.. _pem_certs:

PEM certificate
---------------

PEM is a container format used to hold `X.509 certificate information
<https://en.wikipedia.org/wiki/X.509>`_. Trino has direct support for `PKCS #8
<https://en.wikipedia.org/wiki/PKCS_8>`_ and `PKCS #12
<https://en.wikipedia.org/wiki/PKCS_12>`_ formats in PEM containers.

A single PEM file can contain both certificate and private key information.
Because the certificate portion derives from a recognized Certificate Authority
(CA), it does not need a separate trust store.

If you receive a PEM certificate from your network group or a vendor
:ref:`inspect <inspect-pems>` and :ref:`validate the PEM file<validate-pems>` to
ensure its readiness to work with Trino. Then :ref:`place the file
<cert-placement>` and :ref:`configure Trino <configure-https>` to accept client
connections using HTTPS.

.. _server_java_keystore:

Java keystore
-------------

The Trino server recognizes :ref:`PEM <pem_certs>` and JKS certificates for
configuring access to the Trino server using HTTPS.

.. note::
    See :ref:`pem_certs` above for information on the preferred certificate
    format.

The Java KeyStore (JKS) system is provided as part of your Java installation.
Private keys for your server are stored in a keystore file. The keystore file is
always password-protected.

To be recognized as valid by the :doc:`Trino CLI </installation/cli>` and
modern browsers, the JKS key must be signed by a trusted Certificate Authority
(CA). For JKS keys, this means either that the signing authority for a key must
be listed in the current machine's :ref:`cli_java_truststore`, or that the
certificate received from a CA is chained all the way back to a recognized CA.

If you receive a keystore file from your network group or from a vendor,
:ref:`inspect the keystore's readiness <troubleshooting_keystore>` to work with
Trino. Then :ref:`place the file <cert-placement>` and :ref:`configure Trino
<configure-https>` to accept client connections using HTTPS.


.. _cli_java_truststore:

Java truststore files
---------------------

.. note::
    JKS truststore files are not used if your server certificate is in
    :ref:`pem_certs` format.

The truststore file is a list of Certificate Authorities trusted by Java to
validate private keys. The truststore file is provided as part of your Java
installation.

Truststore files contain certificates of trusted TLS/SSL servers, or of
Certificate Authorities trusted to identify servers. For securing access to the
Trino coordinator through HTTPS, the clients can configure truststores. For the
Trino CLI to trust the Trino coordinator, the coordinator's certificate must
be imported into the CLI's truststore.

You can either import the certificate to a custom truststore, or to the system
default truststore. However, the default truststore is likely to be overwritten
by the next Java version update, so a custom truststore is recommended.

Use :command:`keytool` to import the certificate to the truststore. For one
example below, we import ``trino_certificate.cer`` to a custom truststore
``trino_trust.jks``; you are prompted for whether or not the certificate can be
trusted. Study the ``keytool`` man page for alternative commands.

.. code-block:: text

    keytool -import -v -trustcacerts -alias trino_trust -file trino_certificate.cer -keystore trino_trust.jks -keypass <truststore_pass>

Limitations of self-signed certificates
---------------------------------------

You can generate a self-signed certificate with either the ``openssl`` or
``keytool`` commands. These are meant to be forwarded to a CA, which returns a
validating certificate to add to the local cert.

For a global CA to validate your Certificate Signing Request, you must generate
the cert for use on a specific server that has an external DNS address that can
be verified by the CA. This requirement prevents signed certs that are valid on
one site from being copied and re-used on another site. Modern browsers detect
cases of valid certs used on the wrong server, and block attempts to connect to
them.

You cannot use a self-signed cert without validation from a Certificate
Authority. Modern browsers detect servers running with such keys and either put
up connection roadblocks or refuse outright to open such sites. Likewise, the
Trino CLI detects a Trino server running with an unvalidated self-signed cert
and either warns against connecting or blocks the connection entirely.

However, for development purposes, it is possible to create a local Certificate
Authority root cert that vouches for your self-signed server certificate. This
combination must never be used on a production server. Consult your network
administrator group for assistance generating a self-signed certificate file.

