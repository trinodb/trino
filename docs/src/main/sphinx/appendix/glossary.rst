========
Glossary
========

The glossary contains a list of key Trino terms and definitions.

Terms A-E
---------

.. _glossCA:

CA
    Certificate Authority, a trusted organization that examines and validates
    organizations and their proposed server URIs, and issues digital
    certificates verified as valid for the requesting organization.

.. _glossCert:

Certificate
    A public key `certificate
    <https://en.wikipedia.org/wiki/Public_key_certificate>`_ issued by a CA,
    sometimes abbreviated as *cert*, that verifies the ownership of a
    server's keys. Certificate format is specified in the `X.509
    <https://en.wikipedia.org/wiki/X.509>`_ standard.

Terms F-J
---------

.. _glossJKS:

JKS
    Java KeyStore, the system of public key cryptography supported as one part
    of the Java security APIs. The legacy JKS system recognizes keys and
    certificates stored in *keystore* files, typically with the ``.jks``
    extension, and relies on a system-level list of CAs in *truststore* files
    installed as part of the current Java installation.

Terms K-O
---------

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

Terms P-T
---------

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

Terms U-Z
---------