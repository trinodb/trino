# Secrets

Trino manages configuration details in static properties files. This
configuration needs to include values such as usernames, passwords and other
strings, that are often required to be kept secret. Only a few select
administrators or the provisioning system has access to the actual value.

The secrets support in Trino allows you to use secret providers for the values
of any configuration property. All properties files used by Trino, including
`config.properties` and catalog properties files, are supported. When loading
the properties, Trino replaces each secret reference with the value returned by
the corresponding provider.

## Environment variables

Environment variables are the most widely-supported means of setting and
retrieving values. Environment variables can be set in the scope of the task
being performed, preventing external access. Most provisioning and configuration
management systems include support for setting environment variables. This
includes systems such as Ansible, often used for virtual machines, and
Kubernetes for container usage. You can also manually set an environment
variable on the command line.

```text
export DB_PASSWORD=my-super-secret-pwd
```

To use this variable in the properties file, you reference it with the syntax
`${ENV:VARIABLE}`. For example, if you want to use the password in a catalog
properties file like `etc/catalog/db.properties`, add the following line:

```properties
connection-password=${ENV:DB_PASSWORD}
```

With this setup in place, the secret is managed by the provisioning system
or by the administrators handling the machines. No secret is stored in the Trino
configuration files on the filesystem or wherever they are managed.

## Keystore secrets

Trino includes a keystore secrets provider that retrieves password entries from
a keystore. This allows you to keep secrets out of Trino properties files and
manage them in a password-protected keystore instead.

Use the `keytool` command included with the JDK to create a PKCS12 keystore and
add a password under the alias `db-password`:

```shell
keytool -importpass \
  -storetype PKCS12 \
  -alias db-password \
  -keystore /etc/trino/secrets.p12
```

The command prompts for a keystore password, and then for the password to store.
Repeat the command with a different alias to add each additional password to the
keystore.

Create `secrets.toml` in the Trino configuration directory with the following
configuration:

```toml
[env]
secrets-provider.name = "env"

[example-keystore]
secrets-provider.name = "keystore"
keystore-file-path = "/etc/trino/secrets.p12"
keystore-type = "pkcs12"
keystore-password = "${ENV:TRINO_KEYSTORE_PASSWORD}"
```

Declaring providers in `secrets.toml` replaces the built-in environment variable
resolution. The `env` declaration in the example retains support for
`${ENV:VARIABLE}` references in Trino configuration files. Environment variable
references in `secrets.toml`, such as the keystore password in the example, are
resolved while Trino loads the provider configuration.

The section name `example-keystore` declares the namespace for secret
references. The `secrets-provider.name` property selects the `keystore`
provider.

Reference the keystore entry in a properties file with the provider namespace
and alias:

```properties
connection-password=${example-keystore:db-password}
```

Every node that loads this `secrets.toml` file must have the keystore file
available at the configured path and `TRINO_KEYSTORE_PASSWORD` set in the Trino
process environment. Restrict the file permissions to the account that runs
Trino. Using the environment variable allows you to avoid storing the password
directly in `secrets.toml`.

(custom-secrets-provider)=
## Custom secrets provider

You can implement a custom secrets provider to retrieve values from another
secrets management system. See [](/develop/secrets-provider) for implementation
and installation instructions.

Add a section to `secrets.toml` to create an instance of the provider:

```toml
[custom]
secrets-provider.name = "example-secrets"
endpoint = "https://secrets.example.com"
region = "us-west-2"
```

The section name `custom` declares the provider namespace. The
`secrets-provider.name` value selects the custom provider implementation. All
remaining properties configure the provider.

Reference a key from the custom provider with the namespace and key:

```properties
connection-password=${custom:database/password}
```

This retrieves the secret identified by `database/password` from the configured
custom provider.
