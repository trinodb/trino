==================
JWT authentication
==================

Trino can be configured to authenticate client access using `JSON web tokens
<https://en.wikipedia.org/wiki/JSON_Web_Token>`_. A JWT is a small, web-safe
JSON file that contains cryptographic information similar to a certificate,
including:

*  Subject
*  Valid time period
*  Signature

Sites such as `jwt.io <https://jwt.io>`_ are available to help you decode and
verify a JWT.

A JWT is designed to be passed between servers as proof of prior authentication
in a workflow like the following:

1. An end user logs into a client application and requests access to a server.
2. The server sends the user's credentials to a separate authentication service
   that:

   * validates the user
   * generates a JWT as proof of validation
   * returns the JWT to the requesting server

3. The same JWT can then be forwarded to other services to maintain the user's
   validation without further credentials.

.. important::

    If you are trying to configure OAuth2 or OIDC, there is a dedicated system
    for that in Trino, as described in :doc:`/security/oauth2`. When using
    OAuth2 authentication, you do not need to configure JWT authentication,
    because JWTs are handled automatically by the OAuth2 code.

    A typical use for JWT authentication is to support administrators at large
    sites who are writing their own single sign-on or proxy system to stand
    between users and the Trino coordinator, where their new system submits
    queries on behalf of users.

Using JWT authentication
------------------------

Trino supports Base64 encoded JWTs, but not encrypted JWTs.

There are two ways to get the encryption key necessary to validate the JWT
signature:

- Load the key from a JSON web key set (JWKS) endpoint service (the
  typical case)
- Load the key from the local file system on the Trino coordinator

A JWKS endpoint is a read-only service that contains public key information in
`JWK <https://datatracker.ietf.org/doc/html/rfc7517>`_ format. These public
keys are the counterpart of the private keys that sign JSON web tokens.

JWT authentication configuration
--------------------------------

Enable JWT authentication by setting the :doc:`JWT authentication type
<authentication-types>` in :ref:`etc/config.properties <config_properties>`, and
specifying a URL or path to a key file:

.. code-block:: properties

    http-server.authentication.type=JWT
    http-server.authentication.jwt.key-file=https://cluster.example.net/.well-known/jwks.json

JWT authentication is typically used in addition to other authentication
methods:

.. code-block:: properties

    http-server.authentication.type=PASSWORD,JWT
    http-server.authentication.jwt.key-file=https://cluster.example.net/.well-known/jwks.json

The following configuration properties are available:

.. list-table:: Configuration properties for JWT authentication
   :widths: 50 50
   :header-rows: 1

   * - Property
     - Description
   * - ``http-server.authentication.jwt.key-file``
     - Required. Specifies either the URL to a JWKS service or the path to a
       PEM or HMAC file, as described below this table.
   * - ``http-server.authentication.jwt.required-issuer``
     - Specifies a string that must match the value of the JWT's
       issuer (``iss``) field in order to consider this JWT valid.
       The ``iss`` field in the JWT identifies the principal that issued the
       JWT.
   * - ``http-server.authentication.jwt.required-audience``
     - Specifies a string that must match the value of the JWT's
       Audience (``aud``) field in order to consider this JWT valid.
       The ``aud`` field in the JWT identifies the recipients that the
       JWT is intended for.
   * - ``http-server.authentication.jwt.principal-field``
     - String to identify the field in the JWT that identifies the
       subject of the JWT. The default value is ``sub``. This field is used to
       create the Trino principal.
   * - ``http-server.authentication.jwt.user-mapping.pattern``
     - A regular expression pattern to :doc:`map all user names
       </security/user-mapping>` for this authentication system to the format
       expected by the Trino server.
   * - ``http-server.authentication.jwt.user-mapping.file``
     - The path to a JSON file that contains a set of
       :doc:`user mapping rules </security/user-mapping>` for this
       authentication system.

Use the ``http-server.authentication.jwt.key-file`` property to specify
either:

-  The URL to a JWKS endpoint service, where the URL begins with ``https://``.
   The JWKS service must be reachable from the coordinator. If the coordinator
   is running in a secured or firewalled network, the administrator *may* have
   to open access to the JWKS server host.

   .. caution::

        The Trino server also accepts JWKS URLs that begin with ``http://``, but
        using this protocol results in a severe security risk. Only use this
        protocol for short-term testing during development of your cluster.

-  The path to a local file in :doc:`PEM </security/inspect-pem>` or `HMAC
   <https://en.wikipedia.org/wiki/HMAC>`_ format that contains a single key.
   If the file path contains ``$KEYID``, then Trino interpolates the ``keyid``
   from the JWT into the file path before loading this key. This enables support
   for setups with multiple keys.

Using JWTs with clients
-----------------------

When using the Trino :doc:`CLI </installation/cli>`, specify a JWT as described
in :ref:`cli-jwt-auth`.

When using the Trino JDBC driver, specify a JWT with the ``accessToken``
:ref:`parameter <jdbc-parameter-reference>`.

