# Certificate authenticator

Trino supports TLS-based authentication with X509 certificates via a custom
certificate authenticator that extracts the principal from a client certificate.

## Implementation

`CertificateAuthenticatorFactory` is responsible for creating a
`CertificateAuthenticator` instance. It also defines the name of this
authenticator which is used by the administrator in a Trino configuration.

`CertificateAuthenticator` contains a single method, `authenticate()`,
which authenticates the client certificate and returns a `Principal`, which is then
authorized by the {doc}`system-access-control`.

The implementation of `CertificateAuthenticatorFactory` must be wrapped
as a plugin and installed on the Trino cluster.

## Configuration

After a plugin that implements `CertificateAuthenticatorFactory` has been
installed on the coordinator, it is configured using an
`etc/certificate-authenticator.properties` file. All of the
properties other than `certificate-authenticator.name` are specific to the
`CertificateAuthenticatorFactory` implementation.

The `certificate-authenticator.name` property is used by Trino to find a
registered `CertificateAuthenticatorFactory` based on the name returned by
`CertificateAuthenticatorFactory.getName()`. The remaining properties are
passed as a map to `CertificateAuthenticatorFactory.create()`.

Example configuration file:

```text
certificate-authenticator.name=custom
custom-property1=custom-value1
custom-property2=custom-value2
```

Additionally, the coordinator must be configured to use certificate authentication
and have HTTPS enabled (or HTTPS forwarding enabled).
