======================
HTTP client properties
======================

HTTP client properties allow you to configure the connection from Trino to
external services using HTTP.

The following properties can be used after adding the specific prefix to the
property. For example, for :doc:`/security/oauth2`, you can enable HTTP for
interactions with the external OAuth 2.0 provider by adding the prefix
``oauth2-jwk`` to the ``http-client.connect-timeout`` property, and increasing
the connection timeout to ten seconds by setting the value to ``10``:

.. code-block::

    oauth2-jwk.http-client.connect-timeout=10s

The following prefixes are supported:

* ``oauth2-jwk`` for :doc:`/security/oauth2`
* ``jwk`` for :doc:`/security/jwt`

General properties
------------------

``http-client.connect-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-duration`
* **Default value:** ``5s``
* **Minimum value:** ``0ms``

Timeout value for establishing the connection to the external service.

``http-client.max-connections``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-integer`
* **Default value:** ``200``

Maximum connections allowed to the service.

``http-client.request-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-duration`
* **Default value:** ``5m``
* **Minimum value:** ``0ms``

Timeout value for the overall request.

TLS and security properties
---------------------------

``http-client.https.excluded-cipher``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

A comma-separated list of regexes for the names of cipher algorithms to exclude.

``http-client.https.included-cipher``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

A comma-separated list of regexes for the names of the cipher algorithms to use.

``http-client.https.hostname-verification``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

Verify that the server hostname matches the server DNS name in the
SubjectAlternativeName (SAN) field of the certificate.

``http-client.key-store-password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

Password for the keystore.

``http-client.key-store-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

File path on the server to the keystore file.

``http-client.secure-random-algorithm``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

Set the secure random algorithm for the connection. The default varies by
operating system. Algorithms are specified according to standard algorithm name
documentation.

Possible types include ``NativePRNG``, ``NativePRNGBlocking``,
``NativePRNGNonBlocking``, ``PKCS11``, and ``SHA1PRNG``.

``http-client.trust-store-password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

Password for the truststore.

``http-client.trust-store-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

File path on the server to the truststore file.

Proxy properties
----------------

``http-client.http-proxy``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

Host and port for an HTTP proxy with the format ``example.net:8080``.

``http-client.http-proxy.secure``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``false``

Enable HTTPS for the proxy.

``http-client.socks-proxy``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

Host and port for a SOCKS proxy.

Request logging
---------------

``http-client.log.compression-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

Enable log file compression. The client uses the ``.gz`` format for log files.

``http-client.log.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``false``

Enable logging of HTTP requests.

``http-client.log.flush-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-duration`
* **Default value:** ``10s``

Frequency of flushing the log data to disk.

``http-client.log.max-history``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-integer`
* **Default value:** ``15``

Retention limit of log files in days. Files older than the ``max-history`` are
deleted when the HTTP client creates files for new logging periods.

``http-client.log.max-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``1GB``

Maximum total size of all log files on disk.

``http-client.log.path``
^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **Default value:** ``var/log/``

Sets the path of the log files. All log files are named ``http-client.log``, and
have the prefix of the specific HTTP client added. For example,
``jwk-http-client.log``.

``http-client.log.queue-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-integer`
* **Default value:** ``10000``
* **Minimum value:** ``1``

Size of the HTTP client logging queue.
