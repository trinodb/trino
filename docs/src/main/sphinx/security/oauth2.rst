========================
OAuth 2.0 authentication
========================

Trino can be configured to enable OAuth 2.0 authentication over HTTPS for the
Web UI and the JDBC driver. Trino uses the `Authorization Code
<https://tools.ietf.org/html/rfc6749#section-1.3.1>`_ flow which exchanges an
Authorization Code for a token. At a high level, the flow includes the following
steps:

#. the Trino coordinator redirects a user's browser to the Authorization Server
#. the user authenticates with the Authorization Server, and it approves the Trino's permissions request
#. the user's browser is redirected back to the Trino coordinator with an authorization code
#. the Trino coordinator exchanges the authorization code for a token

.. note::

    OAuth 2.0 authentication currently supports JWT access tokens only, and
    therefore does not support opaque access tokens.

To enable OAuth 2.0 authentication for Trino, configuration changes are made on
the Trino coordinator. No changes are required to the worker configuration;
only the communication from the clients to the coordinator is authenticated.

Set the callback/redirect URL to ``https://<trino-coordinator-domain-name>/oauth2/callback``,
when configuring an OAuth 2.0 authorization server like an OpenID-connect
provider.

Trino server configuration
--------------------------

Using the OAuth2 authentication requires the Trino coordinator to be secured
with TLS.

The following is an example of the required properties that need to be added
to the coordinator's ``config.properties`` file:

.. code-block:: properties

    http-server.authentication.type=oauth2

    http-server.https.port=8443
    http-server.https.enabled=true

    http-server.authentication.oauth2.auth-url=https://authorization-server.com/authorize
    http-server.authentication.oauth2.token-url=https://authorization-server.com/token
    http-server.authentication.oauth2.jwks-url=https://authorization-server.com/.well-known/jwks.json
    http-server.authentication.oauth2.client-id=CLIENT_ID
    http-server.authentication.oauth2.client-secret=CLIENT_SECRET

In order to enable OAuth 2.0 authentication for the Web UI, the following
properties need to be added:

.. code-block:: properties

    web-ui.authentication.type=oauth2

The following configuration properties are available:

.. list-table:: OAuth2 configuration properties
   :widths: 40 60
   :header-rows: 1

   * - Property
     - Description
   * - ``http-server.authentication.type``
     - The type of authentication to use. Must  be set to ``oauth2`` to enable
       OAuth2 authentication for the Trino coordinator.
   * - ``http-server.authentication.oauth2.auth-url``
     - The authorization URL. The URL a user's browser will be redirected to in
       order to begin the OAuth 2.0 authorization process.
   * - ``http-server.authentication.oauth2.token-url``
     - The URL of the endpoint on the authorization server which Trino uses to
       obtain an access token.
   * - ``http-server.authentication.oauth2.jwks-url``
     - The URL of the JSON Web Key Set (JWKS) endpoint on the authorization
       server. It provides Trino the set of keys containing the public key
       to verify any JSON Web Token (JWT) from the authorization server.
   * - ``http-server.authentication.oauth2.client-id``
     - The public identifier of the Trino client.
   * - ``http-server.authentication.oauth2.client-secret``
     - The secret used to authorize Trino client with the authorization server.
   * - ``http-server.authentication.oauth2.audience``
     - The audience of a JSON Web Token is used as the target audience of an
       access token requested by the Trino coordinator as well as the required
       audience of an access token included in user requests to the coordinator.
   * - ``http-server.authentication.oauth2.scopes``
     - Scopes requested by the server during the authorization challenge. See:
       https://tools.ietf.org/html/rfc6749#section-3.3
   * - ``http-server.authentication.oauth2.challenge-timeout``
     - Maximum duration of the authorization challenge. Default is ``15m``.
   * - ``http-server.authentication.oauth2.state-key``
     - A secret key used by the SHA-256
       `HMAC <https://tools.ietf.org/html/rfc2104>`_
       algorithm to sign the state parameter in order to ensure that the
       authorization request was not forged. Default is a random string
       generated during the coordinator start.
   * - ``http-server.authentication.oauth2.user-mapping.pattern``
     - Regex to match against user. If matched, the user name is replaced with
       first regex group. If not matched, authentication is denied.  Default is
       ``(.*)`` which allows any user name.
   * - ``http-server.authentication.oauth2.user-mapping.file``
     - File containing rules for mapping user. See :doc:`/security/user-mapping`
       for more information.
   * - ``http-server.authentication.oauth2.principal-field``
     - The field of the access token used for the Trino user principal. Defaults to ``sub``. Other commonly used fields include ``sAMAccountName``, ``name``, ``upn``, and ``email``.


Troubleshooting
---------------

If you need to debug issues with Trino OAuth 2.0 configuration you can change
the :ref:`log level <log-levels>` for the OAuth 2.0 authenticator:

.. code-block:: none

    io.trino.server.security.oauth2=DEBUG
