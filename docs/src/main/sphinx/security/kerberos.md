# Kerberos authentication

Trino can be configured to enable Kerberos authentication over HTTPS for
clients, such as the {doc}`Trino CLI </client/cli>`, or the JDBC and ODBC
drivers.

To enable Kerberos authentication for Trino, Kerberos-related configuration
changes are made on the Trino coordinator.

Using {doc}`TLS <tls>` and {doc}`a configured shared secret
</security/internal-communication>` is required for Kerberos authentication.

## Environment configuration

(server-kerberos-services)=

### Kerberos services

You will need a Kerberos {abbr}`KDC (Key Distribution Center)` running on a
node that the Trino coordinator can reach over the network. The KDC is
responsible for authenticating principals and issuing session keys that can be
used with Kerberos-enabled services. KDCs typically run on port 88, which is
the IANA-assigned port for Kerberos.

(server-kerberos-configuration)=

### MIT Kerberos configuration

Kerberos needs to be configured on the Trino coordinator. At a minimum, there needs
to be a `kdc` entry in the `[realms]` section of the `/etc/krb5.conf`
file. You may also want to include an `admin_server` entry and ensure that
the Trino coordinator can reach the Kerberos admin server on port 749.

```text
[realms]
  TRINO.EXAMPLE.COM = {
    kdc = kdc.example.com
    admin_server = kdc.example.com
  }

[domain_realm]
  .trino.example.com = TRINO.EXAMPLE.COM
  trino.example.com = TRINO.EXAMPLE.COM
```

The complete [documentation](http://web.mit.edu/kerberos/krb5-latest/doc/admin/conf_files/kdc_conf.html)
for `krb5.conf` is hosted by the MIT Kerberos Project. If you are using a
different implementation of the Kerberos protocol, you will need to adapt the
configuration to your environment.

(server-kerberos-principals)=

### Kerberos principals and keytab files

The Trino coordinator needs a Kerberos principal, as do users who are going to
connect to the Trino coordinator. You need to create these users in Kerberos
using [kadmin](http://web.mit.edu/kerberos/krb5-latest/doc/admin/admin_commands/kadmin_local.html).

In addition, the Trino coordinator needs a [keytab file](http://web.mit.edu/kerberos/krb5-devel/doc/basic/keytab_def.html). After you
create the principal, you can create the keytab file using {command}`kadmin`

```text
kadmin
> addprinc -randkey trino@EXAMPLE.COM
> addprinc -randkey trino/trino-coordinator.example.com@EXAMPLE.COM
> ktadd -k /etc/trino/trino.keytab trino@EXAMPLE.COM
> ktadd -k /etc/trino/trino.keytab trino/trino-coordinator.example.com@EXAMPLE.COM
```

:::{note}
Running {command}`ktadd` randomizes the principal's keys. If you have just
created the principal, this does not matter. If the principal already exists,
and if existing users or services rely on being able to authenticate using a
password or a keytab, use the `-norandkey` option to {command}`ktadd`.
:::

### Configuration for TLS

When using Kerberos authentication, access to the Trino coordinator must be
through {doc}`TLS and HTTPS </security/tls>`.

## System access control plugin

A Trino coordinator with Kerberos enabled probably needs a
{doc}`/develop/system-access-control` plugin to achieve the desired level of
security.

## Trino coordinator node configuration

You must make the above changes to the environment prior to configuring the
Trino coordinator to use Kerberos authentication and HTTPS. After making the
following environment changes, you can make the changes to the Trino
configuration files.

- {doc}`/security/tls`
- {ref}`server-kerberos-services`
- {ref}`server-kerberos-configuration`
- {ref}`server-kerberos-principals`
- {doc}`System Access Control Plugin </develop/system-access-control>`

### config.properties

Kerberos authentication is configured in the coordinator node's
{file}`config.properties` file. The entries that need to be added are listed
below.

```text
http-server.authentication.type=KERBEROS

http-server.authentication.krb5.service-name=trino
http-server.authentication.krb5.principal-hostname=trino.example.com
http-server.authentication.krb5.keytab=/etc/trino/trino.keytab
http.authentication.krb5.config=/etc/krb5.conf

http-server.https.enabled=true
http-server.https.port=7778

http-server.https.keystore.path=/etc/trino/keystore.jks
http-server.https.keystore.key=keystore_password

node.internal-address-source=FQDN
```

| Property                                               | Description                                                                                                                                                                                                                      |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `http-server.authentication.type`                      | Authentication type for the Trino coordinator. Must be set to `KERBEROS`.                                                                                                                                                        |
| `http-server.authentication.krb5.service-name`         | The Kerberos service name for the Trino coordinator. Must match the Kerberos principal.                                                                                                                                          |
| `http-server.authentication.krb5.principal-hostname`   | The Kerberos hostname for the Trino coordinator. Must match the Kerberos principal. This parameter is optional. If included, Trino uses this value in the host part of the Kerberos principal instead of the machine's hostname. |
| `http-server.authentication.krb5.keytab`               | The location of the keytab that can be used to authenticate the Kerberos principal.                                                                                                                                              |
| `http.authentication.krb5.config`                      | The location of the Kerberos configuration file.                                                                                                                                                                                 |
| `http-server.https.enabled`                            | Enables HTTPS access for the Trino coordinator. Should be set to `true`.                                                                                                                                                         |
| `http-server.https.port`                               | HTTPS server port.                                                                                                                                                                                                               |
| `http-server.https.keystore.path`                      | The location of the Java Keystore file that is used to secure TLS.                                                                                                                                                               |
| `http-server.https.keystore.key`                       | The password for the keystore. This must match the password you specified when creating the keystore.                                                                                                                            |
| `http-server.authentication.krb5.user-mapping.pattern` | Regex to match against user.  If matched, user will be replaced with first regex group. If not matched, authentication is denied.  Default is `(.*)`.                                                                            |
| `http-server.authentication.krb5.user-mapping.file`    | File containing rules for mapping user.  See {doc}`/security/user-mapping` for more information.                                                                                                                                 |
| `node.internal-address-source`                         | Kerberos is typically sensitive to DNS names. Setting this property to use `FQDN` ensures correct operation and usage of valid DNS host names.                                                                                   |

See {ref}`Standards supported <tls-version-and-ciphers>` for a discussion of the
supported TLS versions and cipher suites.

### access-control.properties

At a minimum, an {file}`access-control.properties` file must contain an
`access-control.name` property.  All other configuration is specific for the
implementation being configured. See {doc}`/develop/system-access-control` for
details.

(coordinator-troubleshooting)=

## User mapping

After authenticating with Kerberos, the Trino server receives the user's
principal which is typically similar to an email address. For example, when
`alice` logs in Trino might receive `alice@example.com`. By default, Trino
uses the full Kerberos principal name, but this can be mapped to a shorter
name using a user-mapping pattern. For simple mapping rules, the
`http-server.authentication.krb5.user-mapping.pattern` configuration property
can be set to a Java regular expression, and Trino uses the value of the
first matcher group. If the regular expression does not match, the
authentication is denied. For more complex user-mapping rules, see
{doc}`/security/user-mapping`.

## Troubleshooting

Getting Kerberos authentication working can be challenging. You can
independently verify some of the configuration outside of Trino to help narrow
your focus when trying to solve a problem.

### Kerberos verification

Ensure that you can connect to the KDC from the Trino coordinator using
{command}`telnet`:

```text
$ telnet kdc.example.com 88
```

Verify that the keytab file can be used to successfully obtain a ticket using
[kinit](http://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html) and
[klist](http://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/klist.html)

```text
$ kinit -kt /etc/trino/trino.keytab trino@EXAMPLE.COM
$ klist
```

### Java keystore file verification

Verify the password for a keystore file and view its contents using
{ref}`troubleshooting-keystore`.

(kerberos-debug)=

### Additional Kerberos debugging information

You can enable additional Kerberos debugging information for the Trino
coordinator process by adding the following lines to the Trino `jvm.config`
file:

```text
-Dsun.security.krb5.debug=true
-Dlog.enable-console=true
```

`-Dsun.security.krb5.debug=true` enables Kerberos debugging output from the
JRE Kerberos libraries. The debugging output goes to `stdout`, which Trino
redirects to the logging system. `-Dlog.enable-console=true` enables output
to `stdout` to appear in the logs.

The amount and usefulness of the information the Kerberos debugging output
sends to the logs varies depending on where the authentication is failing.
Exception messages and stack traces can provide useful clues about the
nature of the problem.

See [Troubleshooting Security](https://docs.oracle.com/en/java/javase/11/security/troubleshooting-security.html)
in the Java documentation for more details about the `-Djava.security.debug`
flag, and [Troubleshooting](https://docs.oracle.com/en/java/javase/11/security/troubleshooting.html) for
more details about the Java GSS-API and Kerberos issues.

(server-additional-resources)=

### Additional resources

[Common Kerberos Error Messages (A-M)](http://docs.oracle.com/cd/E19253-01/816-4557/trouble-6/index.html)

[Common Kerberos Error Messages (N-Z)](http://docs.oracle.com/cd/E19253-01/816-4557/trouble-27/index.html)

[MIT Kerberos Documentation: Troubleshooting](http://web.mit.edu/kerberos/krb5-latest/doc/admin/troubleshoot.html)
