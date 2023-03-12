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

To enable OAuth 2.0 authentication for Trino, configuration changes are made on
the Trino coordinator. No changes are required to the worker configuration;
only the communication from the clients to the coordinator is authenticated.

Set the callback/redirect URL to ``https://<trino-coordinator-domain-name>/oauth2/callback``,
when configuring an OAuth 2.0 authorization server like an OpenID Connect (OIDC)
provider.

Using :doc:`TLS <tls>` and :doc:`a configured shared secret
</security/internal-communication>` is required for OAuth 2.0 authentication.

OpenID Connect Discovery
------------------------

Trino supports reading Authorization Server configuration from `OIDC provider
configuration metadata document
<https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata>`_.
During startup of the coordinator Trino retrieves the document and uses provided
values to set corresponding OAuth2 authentication configuration properties:

* ``authorization_endpoint`` -> ``http-server.authentication.oauth2.auth-url``
* ``token_endpoint`` -> ``http-server.authentication.oauth2.token-url``
* ``jwks_uri`` -> ``http-server.authentication.oauth2.jwks-url``
* ``userinfo_endpoint`` ->  ``http-server.authentication.oauth2.userinfo-url``
* ``access_token_issuer`` -> ``http-server.authentication.oauth2.access-token-issuer``

.. warning::

  If the authorization server is issuing JSON Web Tokens (JWTs) and the
  metadata document contains ``userinfo_endpoint``, Trino uses this endpoint to
  check the validity of OAuth2 access tokens. Since JWTs can be inspected
  locally, using them against ``userinfo_endpoint`` may result in authentication
  failure. In this case, set the
  ``http-server.authentication.oauth2.oidc.use-userinfo-endpoint`` configuration
  property to ``false``
  (``http-server.authentication.oauth2.oidc.use-userinfo-endpoint=false``). This
  instructs Trino to ignore ``userinfo_endpoint`` and inspect tokens locally.

This functionality is enabled by default but can be turned off with:
``http-server.authentication.oauth2.oidc.discovery=false``.

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

    http-server.authentication.oauth2.issuer=https://authorization-server.com
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
   * - ``http-server.authentication.oauth2.issuer``
     - The issuer URL of the IdP. All issued tokens must have this in the ``iss`` field.
   * - ``http-server.authentication.oauth2.access-token-issuer``
     - The issuer URL of the IdP for access tokens, if different.
       All issued access tokens must have this in the ``iss`` field.
       Providing this value while OIDC discovery is enabled overrides the value
       from the OpenID provider metadata document.
       Defaults to the value of ``http-server.authentication.oauth2.issuer``.
   * - ``http-server.authentication.oauth2.auth-url``
     - The authorization URL. The URL a user's browser will be redirected to in
       order to begin the OAuth 2.0 authorization process. Providing this value
       while OIDC discovery is enabled overrides the value from the OpenID
       provider metadata document.
   * - ``http-server.authentication.oauth2.token-url``
     - The URL of the endpoint on the authorization server which Trino uses to
       obtain an access token. Providing this value while OIDC discovery is
       enabled overrides the value from the OpenID provider metadata document.
   * - ``http-server.authentication.oauth2.jwks-url``
     - The URL of the JSON Web Key Set (JWKS) endpoint on the authorization
       server. It provides Trino the set of keys containing the public key
       to verify any JSON Web Token (JWT) from the authorization server.
       Providing this value while OIDC discovery is enabled overrides the value
       from the OpenID provider metadata document.
   * - ``http-server.authentication.oauth2.userinfo-url``
     - The URL of the IdPs ``/userinfo`` endpoint. If supplied then this URL is
       used to validate the OAuth access token and retrieve any associated
       claims. This is required if the IdP issues opaque tokens. Providing this
       value while OIDC discovery is enabled overrides the value from the OpenID
       provider metadata document.
   * - ``http-server.authentication.oauth2.client-id``
     - The public identifier of the Trino client.
   * - ``http-server.authentication.oauth2.client-secret``
     - The secret used to authorize Trino client with the authorization server.
   * - ``http-server.authentication.oauth2.additional-audiences``
     - Additional audiences to trust in addition to the client ID which is
       always a trusted audience.
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
   * - ``http-server.authentication.oauth2.oidc.discovery``
     - Enable reading the `OIDC provider metadata <https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata>`_.
       Default is ``true``.
   * - ``http-server.authentication.oauth2.oidc.discovery.timeout``
     - The timeout when reading OpenID provider metadata. Default is ``30s``.
   * - ``http-server.authentication.oauth2.oidc.use-userinfo-endpoint``
     - Use the value of ``userinfo_endpoint`` `in the provider metadata <https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata>`_.
       When a ``userinfo_endpoint`` value is supplied this URL is used to
       validate the OAuth 2.0 access token, and retrieve any associated claims.
       This flag allows ignoring the value provided in the metadata document.
       Default is ``true``.

Refresh tokens
^^^^^^^^^^^^^^

In order to start using refresh tokens flows, the following property must be
enabled in the coordinator configuration.

.. code-block:: properties

    http-server.authentication.oauth2.refresh-tokens=true

Additional scopes for offline access might be required, depending on
IdP configuration.

.. code-block:: properties

    http-server.authentication.oauth2.scopes=openid,offline_access [or offline]

The following configuration properties are available:

.. list-table:: OAuth2 configuration properties for refresh flow
   :widths: 40 60
   :header-rows: 1

   * - Property
     - Description
   * - ``http-server.authentication.oauth2.refresh-tokens.issued-token.timeout``
     - Expiration time for issued token. Value must be less than or equal to
       the duration of the refresh token expiration issued by the IdP.
       Defaults to ``1h``.
   * - ``http-server.authentication.oauth2.refresh-tokens.issued-token.issuer``
     - Issuer representing the coordinator instance, that is referenced in the
       issued token, defaults to ``Trino_coordinator``. The current
       Trino version is appended to the value. This is mainly used for
       debugging purposes.
   * - ``http-server.authentication.oauth2.refresh-tokens.issued-token.audience``
     - Audience representing this coordinator instance, that is used in the
       issued token. Defaults to ``Trino_coordinator``.
   * - ``http-server.authentication.oauth2.refresh-tokens.secret-key``
     - Base64-encoded secret key used to encrypt the generated token.
       By default it's generated during startup.


Troubleshooting
---------------

If you need to debug issues with Trino OAuth 2.0 configuration you can change
the :ref:`log level <log-levels>` for the OAuth 2.0 authenticator:

.. code-block:: none

    io.trino.server.security.oauth2=DEBUG
