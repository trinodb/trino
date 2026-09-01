# Secrets provider

Trino supports custom secrets provider plugins that retrieve values from
external secrets management systems. General information about configuring and
using secrets providers is available in [](/security/secrets).

## Implementation

Compile the plugin against the `io.airlift:secrets-spi` artifact version used by
the target Trino release. Use the `provided` dependency scope because Trino
provides the secrets SPI at runtime:

```xml
<dependency>
    <groupId>io.airlift</groupId>
    <artifactId>secrets-spi</artifactId>
    <version>${dep.airlift.version}</version>
    <scope>provided</scope>
</dependency>
```

The plugin must include implementations of the following interfaces from the
`io.airlift.spi.secrets` package:

- `SecretProvider` implements `resolveSecretValue(String key)` to retrieve and
  return a secret. The method must throw a runtime exception when it cannot
  resolve the key.
- `SecretProviderFactory` returns the provider name from `getName()` and creates
  a provider from the configuration map passed to `createSecretProvider()`. The
  name must start with a lowercase letter and contain only lowercase letters,
  numbers, underscores, and hyphens.
- `SecretsPlugin` returns the plugin's factories from
  `getSecretProviderFactories()`:

  ```java
  public final class ExampleSecretsPlugin
          implements SecretsPlugin
  {
      @Override
      public List<SecretProviderFactory> getSecretProviderFactories()
      {
          return List.of(new ExampleSecretProviderFactory());
      }
  }
  ```

## Service registration

Register the `SecretsPlugin` implementation with the Java `ServiceLoader`. Add
a file named `io.airlift.spi.secrets.SecretsPlugin` in the `META-INF/services`
directory of the plugin JAR. Use the fully qualified implementation class name
as its content:

```text
com.example.secrets.ExampleSecretsPlugin
```

## Installation

Place the plugin JAR and all its dependencies in a dedicated directory under
the Trino installation's `secrets-plugin` directory on every applicable node.
You can use the `secrets-plugins-dir` property at the top level of
`secrets.toml` to configure a different parent directory.

Configure and reference an installed provider as described in
{ref}`custom-secrets-provider`.
