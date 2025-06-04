# Password authenticator

Trino supports authentication with a username and password via a custom
password authenticator that validates the credentials and creates a principal.

## Implementation

`PasswordAuthenticatorFactory` is responsible for creating a
`PasswordAuthenticator` instance. It also defines the name of this
authenticator which is used by the administrator in a Trino configuration.

`PasswordAuthenticator` contains a single method, `createAuthenticatedPrincipal()`,
that validates the credential and returns a `Principal`, which is then
authorized by the {doc}`system-access-control`.

The implementation of `PasswordAuthenticatorFactory` must be wrapped
as a plugin and installed on the Trino cluster.

## Configuration

After a plugin that implements `PasswordAuthenticatorFactory` has been
installed on the coordinator, it is configured using an
`etc/password-authenticator.properties` file. All the
properties other than `password-authenticator.name` are specific to the
`PasswordAuthenticatorFactory` implementation.

The `password-authenticator.name` property is used by Trino to find a
registered `PasswordAuthenticatorFactory` based on the name returned by
`PasswordAuthenticatorFactory.getName()`. The remaining properties are
passed as a map to `PasswordAuthenticatorFactory.create()`.

Example configuration file:

```text
password-authenticator.name=custom-access-control
custom-property1=custom-value1
custom-property2=custom-value2
```

Additionally, the coordinator must be configured to use password authentication
and have HTTPS enabled (or HTTPS forwarding enabled).
