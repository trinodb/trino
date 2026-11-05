# Extra credentials provider

Trino can resolve additional extra credentials for an authenticated user on the
server, rather than accepting them from the client. This resolution is performed
by an `ExtraCredentialsProvider` implementation.

## Implementation

`ExtraCredentialsProviderFactory` is responsible for creating an
`ExtraCredentialsProvider` instance. It also defines the name of the extra
credentials provider as used in the configuration file.

`ExtraCredentialsProvider` contains a single method,
`getExtraCredentials(String user)`, which returns a `Map<String, String>` of
credential names to values. These credentials are merged into the `Identity` and
`ConnectorIdentity` objects representing the user, after the
`X-Trino-Extra-Credential` headers supplied by the client are parsed. Values
returned by the provider therefore take precedence over client-supplied values
with the same credential name.

Connectors that read named extra credentials consume them as usual. For example,
the JDBC-based connectors can be configured with `user-credential-name` and
`password-credential-name` so that a single shared catalog connects to the
backing database using the credentials of the user running the query.

The implementation of `ExtraCredentialsProvider` and its corresponding
`ExtraCredentialsProviderFactory` must be wrapped as a Trino plugin and
installed on the cluster.

## Configuration

After a plugin that implements `ExtraCredentialsProviderFactory` has been
installed on the coordinator, it is configured using an
`etc/extra-credentials-provider.properties` file. All the properties other than
`extra-credentials-provider.name` are specific to the
`ExtraCredentialsProviderFactory` implementation.

The `extra-credentials-provider.name` property is used by Trino to find a
registered `ExtraCredentialsProviderFactory` based on the name returned by
`ExtraCredentialsProviderFactory.getName()`. The remaining properties are passed
as a map to `ExtraCredentialsProviderFactory.create(Map<String, String>)`.

Example configuration file:

```text
extra-credentials-provider.name=custom-extra-credentials-provider
custom-property1=custom-value1
custom-property2=custom-value2
```

If the file is not present, no extra credentials are resolved on the server and
only client-supplied extra credentials are used.
