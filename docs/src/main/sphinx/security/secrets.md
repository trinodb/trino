# Secrets

Trino manages configuration details in static properties files. This
configuration needs to include values such as usernames, passwords and other
strings, that are often required to be kept secret. Only a few select
administrators or the provisioning system has access to the actual value.

The secrets support in Trino allows you to use environment variables as values
for any configuration property. All properties files used by Trino, including
`config.properties` and catalog properties files, are supported. When loading
the properties, Trino replaces the reference to the environment variable with
the value of the environment variable.

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
