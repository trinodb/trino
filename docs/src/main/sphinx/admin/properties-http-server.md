# HTTP server properties

HTTP server properties allow you to configure the HTTP server of Trino that
handles [](/security) including [](/security/internal-communication),  and
serves the [](/admin/web-interface) and the [client
API](/develop/client-protocol).

## General

(http-server-process-forwarded)=
### `http-server.process-forwarded`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

Enable treating forwarded HTTPS requests over HTTP as secure. Requires the
[`X-Forwarded` headers](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For#see_also)
to be set to `HTTPS` on forwarded requests. This is commonly performed by a load
balancer that terminates HTTPS to HTTP. Set to `true` when using such a load
balancer in front of Trino or [Trino
Gateway](https://trinodb.github.io/trino-gateway/). Find more details in
[](https-load-balancer).

## HTTP and HTTPS

### `http-server.http.port`

- **Type:** [](prop-type-integer)
- **Default value:** `8080`

Specify the HTTP port for the HTTP server.

### `http-server.https.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

Enable [](/security/tls).

### `http-server.https.port`

- **Type:** [](prop-type-integer)
- **Default value:** `8443`

Specify the HTTPS port for the HTTP server.

### `http-server.https.included-cipher` and `http-server.https.excluded-cipher`

Optional configuration for ciphers to use TLS, find details in
[](tls-version-and-ciphers).

### `http-server.https.keystore.path`

- **Type:** [](prop-type-string)

The location of the PEM or Java keystore file used to enable [](/security/tls).

### `http-server.https.keystore.key`

- **Type:** [](prop-type-string)

The password for the PEM or Java keystore.

### `http-server.https.truststore.path`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

The location of the optional PEM or Java truststore file for additional
certificate authorities. Find details in [](/security/tls).

### `http-server.https.truststore.key`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

The password for the optional PEM or Java truststore.

### `http-server.https.keymanager.password`

- **Type:** [](prop-type-string)

Password for a key within a keystore, when a different password is configured
for the specific key. Find details in [](/security/tls).

### `http-server.https.secure-random-algorithm`

- **Type:** [](prop-type-string)

Optional name of the algorithm to generate secure random values for
[internal communication](internal-performance).

### `http-server.https.ssl-session-timeout`

- **Type:** [](prop-type-duration)
- **Default value:** `4h`

Time duration for a valid TLS client session.

### `http-server.https.ssl-session-cache-size`

- **Type:** [](prop-type-integer)
- **Default value:** `10000`

Maximum number of SSL session cache entries.

### `http-server.https.ssl-context.refresh-time`

- **Type:** [](prop-type-duration)
- **Default value:** `1m`

Time between reloading default certificates.

## Authentication

### `http-server.authentication.type`

- **Type:** [](prop-type-string)

Configures the ordered list of enabled [authentication
types](/security/authentication-types).

All authentication requires secure connections using [](/security/tls) or
[process forwarding enabled](http-server-process-forwarded), and [a configured
shared secret](/security/internal-communication).

### `http-server.authentication.allow-insecure-over-http`

- **Type:** [](prop-type-boolean)

Enable HTTP when any authentication is active. Defaults to `true`, but is
automatically set to `false` with active authentication. Overriding the value to
`true` can be useful for testing, but is not secure. More details in
[](/security/tls).

### `http-server.authentication.certificate.*`

Configuration properties for [](/security/certificate).

### `http-server.authentication.jwt.*`

Configuration properties for [](/security/jwt).

### `http-server.authentication.krb5.*`

Configuration properties for [](/security/kerberos).

### `http-server.authentication.oauth2.*`

Configuration properties for [](/security/oauth2).

### `http-server.authentication.password.*`

Configuration properties for the `PASSWORD` authentication types
[](/security/ldap), [](/security/password-file), and [](/security/salesforce).

## Logging

### `http-server.log.*`

Configuration properties for [](/admin/properties-logging).

(props-internal-communication)
## Internal communication

The following properties are used for configuring the [internal
communication](/security/internal-communication) between all
[nodes](trino-concept-node) of a Trino cluster.

### `internal-communication.shared-secret`

- **Type:** [](prop-type-string)

The string to use as secret that only the coordinators and workers in a specific
cluster share and use to authenticate within the cluster. See
[](internal-secret) for details.

### `internal-communication.http2.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `true`

Enable use of the HTTP/2 protocol for internal communication for enhanced
scalability compared to HTTP/1.1. Only turn this feature off if you encounter
issues with HTTP/2 usage within the cluster in your deployment.

### `internal-communication.https.required`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

Enable the use of [SSL/TLS for all internal communication](internal-tls).
