# Authentication types

Trino supports multiple authentication types to ensure all users of the system
are authenticated. Different authenticators allow user management in one or more
systems.

All authentication requires secure connections using [](/security/tls) or
[process forwarding enabled](http-server-process-forwarded), and [a configured
shared secret](/security/internal-communication).

You can configure one or more authentication types with the
`http-server.authentication.type` property. The following authentication types
and authenticators are available:

- `PASSWORD` for

  - {doc}`password-file`
  - {doc}`ldap`
  - {doc}`salesforce`

- `OAUTH2` for {doc}`oauth2`

- `KERBEROS` for {doc}`kerberos`

- `CERTIFICATE` for {doc}`certificate`

- `JWT` for {doc}`jwt`

- `HEADER` for {doc}`/develop/header-authenticator`

Get started with a basic password authentication configuration backed by a
{doc}`password file <password-file>`:

```properties
http-server.authentication.type=PASSWORD
```

## Multiple authentication types

You can use multiple authentication types, separated with commas in the
configuration:

```properties
http-server.authentication.type=PASSWORD,CERTIFICATE
```

Authentication is performed in order of the entries, and first successful
authentication results in access, using the {doc}`mapped user <user-mapping>`
from that authentication method.

## Multiple password authenticators

You can use multiple password authenticator types by referencing multiple
configuration files:

```properties
http-server.authentication.type=PASSWORD
password-authenticator.config-files=etc/ldap1.properties,etc/ldap2.properties,etc/password.properties
```

In the preceding example, the configuration files `ldap1.properties` and
`ldap2.properties` are regular {doc}`LDAP authenticator configuration files
<ldap>`. The `password.properties` is a {doc}`password file authenticator
configuration file <password-file>`.

Relative paths to the installation directory or absolute paths can be used.

User authentication credentials are first validated against the LDAP server from
`ldap1`, then the separate server from `ldap2`, and finally the password
file. First successful authentication results in access, and no further
authenticators are called.

## Multiple header authenticators

You can use multiple header authenticator types by referencing multiple
configuration files:

```properties
http-server.authentication.type=HEADER
header-authenticator.config-files=etc/xfcc.properties,etc/azureAD.properties
```

Relative paths to the installation directory or absolute paths can be used.

The pre-configured headers are first validated against the `xfcc` authenticator,
then the `azureAD` authenticator. First successful authentication results in access,
and no further authenticators are called.

## Password authentication metrics

Trino counts the results of `PASSWORD` authentication with the following
metrics, available on the [`/metrics` endpoint](/admin/openmetrics) and with
[](/admin/jmx) as the `trino.server.security:name=PasswordAuthenticator` MBean:

- `trino_server_security_name_PasswordAuthenticator_AuthenticationSuccess`:
  number of times the submitted username and password were accepted by a
  password authenticator.
- `trino_server_security_name_PasswordAuthenticator_AuthenticationFailure`:
  number of times the submitted username and password were rejected by all
  configured password authenticators, or the authenticated user could not be
  [mapped](/security/user-mapping).

Clients such as the [](/client/cli) and the [](/client/jdbc) send credentials
with every HTTP request, and a query uses multiple requests. The metrics
therefore count authenticated requests, and not logins or sessions.

A request without credentials receives an HTTP 401 response with an
authentication challenge, and is not counted. This response is a normal step of
the HTTP authentication protocol, and is not a failed login. Requests with
malformed credentials and errors, such as an unreachable LDAP server, are also
not counted.

With multiple password authenticators, a request counts as a success if any
authenticator accepts the credentials, and as a single failure if all of them
reject the credentials. With multiple authentication types, a rejected password
counts as a failure even if an authentication type listed later, such as
`CERTIFICATE`, authenticates the request.

Other authentication types, login with the form of the [](/admin/web-interface),
and internal communication between cluster nodes are not counted.
