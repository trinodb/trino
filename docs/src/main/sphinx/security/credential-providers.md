# Credential providers
A credential provider is a separate module to be configured and to be linked to
a connector. This way, a connector can obtain a per-user credential to be used
to authenticate when connecting to a data source.

:::{list-table} Server configuration
:widths: 50 50
:header-rows: 1

* - Property name
  - Description
* - `credential-provider.config-dir`
  - The directory for the named properties files. Defaults to 
    `etc/credential-provider`
:::

## Configuration
To configure credential providers, create a named properties file
in `etc/credential-provider`, for example `test.properties`. This 
name can be used in the connector properties to connect to a data source, for
example `credential-provider.database.name=test`. The name `database` is
chosen by the developer of the connector and describes where the credential 
is used for. A connector may need multiple credential providers for 
different purposes (`database`, `storage`, etc.).

(security-supported-credential-providers)=
## Supported credential providers

(security-credential-api-key)=
### API key
```none
credential-provider.name=api_key
api-key=YOUR-KEY
```

(security-credential-provider-basic)=
### Basic auth
```none
credential-provider.name=basic
username=admin
password=welcome123!
```

(security-credential-provider-oidc)=
### OpenID connect token-exchange RFC-8693
```none
credential-provider.name=oidc
audience=
client-id=
client-secret=
scopes=
token-url=
```

* - Property name
  - Description
* - `audience`
  - The audience for the access token to obtain
* - `client-id`
  - The oauth2 client id
* - `client-secret`
  - The oauth2 client secret
* - `scopes`
  - The scopes for the access token to obtain, e.g. `openid email profile`
* - `token-url`
  - The token url, e.g.
    `https://keycloak.company.com/realms/master/protocol/openid-connect/token`
:::
