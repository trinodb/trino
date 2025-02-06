# Preview Web UI

In addition to the [](/admin/web-interface), Trino includes a preview version of
a new web interface. It changes look and feel, available features, and many
other aspects. In the future this new user interface will replace the existing
user interface.

:::{warning}

The Preview Web UI is not suitable for production usage, and only available for
testing and evaluation purposes. Feedback and assistance with development is
encouraged. Find collaborators and discussions in ongoing pull requests and the
[#web-ui channel](https://trinodb.slack.com/messages/CKCEWGYT0).

:::

## Activation

The Preview Web UI is not available by default, and must be enabled in
[](config-properties) with the following configuration:


```properties
web-ui.preview.enabled=true
```

## Access

Once activated, users can access the interface in the URL context `/ui/preview`
after successful login to the [](/admin/web-interface). For example, the full
URL on a locally running Trino installation or Trino docker container without
TLS configuration is [http://localhost:8080/ui/preview](http://localhost:8080/ui/preview).

## Authentication

The Preview Web UI requires users to authenticate. If Trino is not configured to
require authentication, then any username can be used, and no password is
required or allowed. The UI shows the login dialog for password authentication
with the password input deactivated. This is also automatically the case if the
cluster is only configured to use HTTP. Typically, users login with the same
username that they use for running queries.

If no system access control is installed, then all users are able to view and
kill any query. This can be restricted by using [query rules](query-rules) with
the [](/security/built-in-system-access-control). Users always have permission
to view or kill their own queries.

### Password authentication

Typically, a password-based authentication method such as [LDAP](/security/ldap)
or [password file](/security/password-file) is used to secure both the Trino
server and the Web UI. When the Trino server is configured to use a password
authenticator, the Web UI authentication type is automatically set to `FORM`. In
this case, the Web UI displays a login form that accepts a username and
password. 

### Fixed user authentication

If you require the Preview Web UI to be accessible without authentication, you
can set a fixed username that will be used for all Web UI access by setting the
authentication type to `FIXED` and setting the username with the `web-ui.user`
configuration property. If there is a system access control installed, this user
must have permission to view ,and possibly to kill, queries.

### Other authentication types

The following Preview Web UI authentication types are also supported:

- `CERTIFICATE`, see details in [](/security/certificate)
- `KERBEROS`, see details in [](/security/kerberos)
- `JWT`, see details in [](/security/jwt)
- `OAUTH2`, see details in [](/security/oauth2)

For these authentication types, the username is defined by
[](/security/user-mapping).
