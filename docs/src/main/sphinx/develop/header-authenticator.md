# Header authenticator

Trino supports header authentication over TLS via a custom header authenticator
that extracts the principal from a predefined header(s), performs any validation it needs and creates
an authenticated principal.

## Implementation

`HeaderAuthenticatorFactory` is responsible for creating a
`HeaderAuthenticator` instance. It also defines the name of this
authenticator which is used by the administrator in a Trino configuration.

`HeaderAuthenticator` contains a single method, `createAuthenticatedPrincipal()`,
which validates the request headers wrapped by the Headers interface; has the method getHeader(String name)
and returns a `Principal`, which is then authorized by the {doc}`system-access-control`.

The implementation of `HeaderAuthenticatorFactory` must be wrapped
as a plugin and installed on the Trino cluster.

## Configuration

After a plugin that implements `HeaderAuthenticatorFactory` has been
installed on the coordinator, it is configured using an
`etc/header-authenticator.properties` file. All of the
properties other than `header-authenticator.name` are specific to the
`HeaderAuthenticatorFactory` implementation.

The `header-authenticator.name` property is used by Trino to find a
registered `HeaderAuthenticatorFactory` based on the name returned by
`HeaderAuthenticatorFactory.getName()`. The remaining properties are
passed as a map to `HeaderAuthenticatorFactory.create()`.

Example configuration file:

```none
header-authenticator.name=custom
custom-property1=custom-value1
custom-property2=custom-value2
```

Additionally, the coordinator must be configured to use header authentication
and have HTTPS enabled (or HTTPS forwarding enabled).
