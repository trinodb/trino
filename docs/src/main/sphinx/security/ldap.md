# LDAP authentication

Trino can be configured to enable frontend LDAP authentication over
HTTPS for clients, such as the {ref}`cli-ldap`, or the JDBC and ODBC
drivers. At present, only simple LDAP authentication mechanism involving
username and password is supported. The Trino client sends a username
and password to the coordinator, and the coordinator validates these
credentials using an external LDAP service.

To enable LDAP authentication for Trino, LDAP-related configuration changes are
made on the Trino coordinator.

Using {doc}`TLS <tls>` and {doc}`a configured shared secret
</security/internal-communication>` is required for LDAP authentication.

## Trino server configuration

### Trino coordinator node configuration

Access to the Trino coordinator should be through HTTPS, configured as described
on {doc}`TLS and HTTPS </security/tls>`.

You also need to make changes to the Trino configuration files.
LDAP authentication is configured on the coordinator in two parts.
The first part is to enable HTTPS support and password authentication
in the coordinator's `config.properties` file. The second part is
to configure LDAP as the password authenticator plugin.

#### Server config properties

The following is an example of the required properties that need to be added
to the coordinator's `config.properties` file:

```text
http-server.authentication.type=PASSWORD

http-server.https.enabled=true
http-server.https.port=8443

http-server.https.keystore.path=/etc/trino/keystore.jks
http-server.https.keystore.key=keystore_password
```

Find detailed description for the available properties in
[](/admin/properties-http-server) and the following table:

:::{list-table}
:widths: 20, 80
:header-rows: 1

* - Property
  - Description
* - `http-server.authentication.password.user-mapping.pattern`
  - Regex to match against user.  If matched, user is replaced with first regex
    group. If not matched, authentication is denied.  Defaults to `(.*)`.
* - `http-server.authentication.password.user-mapping.file`
  - File containing rules for mapping user.  See [](/security/user-mapping)
    for more information.
:::

#### Password authenticator configuration

Password authentication must be configured to use LDAP. Create an
`etc/password-authenticator.properties` file on the coordinator. Example:

```text
password-authenticator.name=ldap
ldap.url=ldaps://ldap-server:636
ldap.ssl.truststore.path=/path/to/ldap_server.pem
ldap.user-bind-pattern=<Refer below for usage>
```

:::{list-table}
:widths: 20, 80
:header-rows: 1

* - Property
  - Description
* - `ldap.url`
  - The URL to the LDAP server. The URL scheme must be `ldap://` or `ldaps://`.
    Connecting to the LDAP server without TLS enabled requires
    `ldap.allow-insecure=true`.
* - `ldap.allow-insecure`
  - Allow using an LDAP connection that is not secured with TLS.
* - `ldap.ssl.keystore.path`
  - The path to the {doc}`PEM </security/inspect-pem>` or {doc}`JKS
    </security/inspect-jks>` keystore file.
* - `ldap.ssl.keystore.password`
  - Password for the key store.
* - `ldap.ssl.truststore.path`
  - The path to the {doc}`PEM </security/inspect-pem>` or {doc}`JKS
    </security/inspect-jks>` truststore file.
* - `ldap.ssl.truststore.password`
  - Password for the truststore.
* - `ldap.user-bind-pattern`
  - This property can be used to specify the LDAP user bind string for password
    authentication. This property must contain the pattern `${USER}`, which is
    replaced by the actual username during the password authentication. The
    property can contain multiple patterns separated by a colon. Each pattern is
    checked in order until a login succeeds or all logins fail. Example:
    `${USER}@corp.example.com:${USER}@corp.example.co.uk`
* - `ldap.ignore-referrals`
  - Ignore referrals to other LDAP servers while performing search queries.
    Defaults to `false`.
* - `ldap.cache-ttl`
  - LDAP cache duration. Defaults to `1h`.
* - `ldap.timeout.connect`
  - Timeout for establishing an LDAP connection.
* - `ldap.timeout.read`
  - Timeout for reading data from an LDAP connection.
:::

Based on the LDAP server implementation type, the property
`ldap.user-bind-pattern` can be used as described below.

##### Active Directory

```text
ldap.user-bind-pattern=${USER}@<domain_name_of_the_server>
```

Example:

```text
ldap.user-bind-pattern=${USER}@corp.example.com
```

##### OpenLDAP

```text
ldap.user-bind-pattern=uid=${USER},<distinguished_name_of_the_user>
```

Example:

```text
ldap.user-bind-pattern=uid=${USER},OU=America,DC=corp,DC=example,DC=com
```

#### Authorization based on LDAP group membership

You can further restrict the set of users allowed to connect to the Trino
coordinator, based on their group membership, by setting the optional
`ldap.group-auth-pattern` and `ldap.user-base-dn` properties, in addition
to the basic LDAP authentication properties.

:::{list-table}
:widths: 35, 65
:header-rows: 1

* - Property
  - Description
* - `ldap.user-base-dn`
  - The base LDAP distinguished name for the user who tries to connect to the
    server. Example: `OU=America,DC=corp,DC=example,DC=com`
* - `ldap.group-auth-pattern`
  - This property is used to specify the LDAP query for the LDAP group
    membership authorization. This query is executed against the LDAP server and
    if successful, the user is authorized.

    This property must contain a pattern `${USER}`, which is replaced by the
    actual username in the group authorization search query. See details in the
    [examples section](ldap-group-auth-examples).
:::

#### Authorization using Trino LDAP service user

Trino server can use dedicated LDAP service user for doing user group membership
queries. In such case Trino first issues a group membership query for a Trino
user that needs to be authenticated. A user distinguished name is extracted from
a group membership query result. Trino then validates user password by creating
LDAP context with user distinguished name and user password. In order to use
this mechanism `ldap.bind-dn`, `ldap.bind-password` and
`ldap.group-auth-pattern` properties need to be defined.

:::{list-table}
:widths: 35, 65
:header-rows: 1

* - Property
  - Description
* - `ldap.bind-dn`
  - Bind distinguished name used by Trino when issuing group membership queries.
    Example: `CN=admin,OU=CITY_OU,OU=STATE_OU,DC=domain`
* - `ldap.bind-password`
  - Bind password used by Trino when issuing group membership queries. Example:
    `password1234`
* - `ldap.group-auth-pattern`
  - This property is used to specify the LDAP query for the LDAP group
    membership authorization. This query is executed against the LDAP server
    during login to check if the user belongs to the specified group. If
    successful, a user distinguished name is extracted from the query result.
    Trino then validates the user and password by creating an LDAP context with
    the user's distinguished name and password.

    This property must contain a pattern `${USER}`, which is replaced by the
    actual username in the group authorization search query. See details in the
    [examples section](ldap-group-auth-examples).
:::

(ldap-group-auth-examples)=
##### LDAP group authorization examples

With Active Directory, the following syntax can be used:

```text
ldap.group-auth-pattern=(&(objectClass=<objectclass_of_user>)(sAMAccountName=${USER})(memberof=<dn_of_the_authorized_group>))
```

Example:

```text
ldap.group-auth-pattern=(&(objectClass=person)(sAMAccountName=${USER})(memberof=CN=AuthorizedGroup,OU=Asia,DC=corp,DC=example,DC=com))
```

With OpenLDAP, the following syntax can be used:

```text
ldap.group-auth-pattern=(&(objectClass=<objectclass_of_user>)(uid=${USER})(memberof=<dn_of_the_authorized_group>))
```

Example:

```text
ldap.group-auth-pattern=(&(objectClass=inetOrgPerson)(uid=${USER})(memberof=CN=AuthorizedGroup,OU=Asia,DC=corp,DC=example,DC=com))
```

For OpenLDAP, for this query to work, make sure you enable the
`memberOf` [overlay](http://www.openldap.org/doc/admin24/overlays.html).

You can use this property for scenarios where you want to authorize a user
based on complex group authorization search queries. For example, if you want to
authorize a user belonging to any one of multiple groups (in OpenLDAP), this
property may be set as follows:

```text
ldap.group-auth-pattern=(&(|(memberOf=CN=normal_group,DC=corp,DC=com)(memberOf=CN=another_group,DC=com))(objectClass=inetOrgPerson)(uid=${USER}))
```

(cli-ldap)=
## Trino CLI

### Environment configuration

#### TLS configuration

When using LDAP authentication, access to the Trino coordinator must be through
{doc}`TLS/HTTPS </security/tls>`.

### Trino CLI execution

In addition to the options that are required when connecting to a Trino
coordinator that does not require LDAP authentication, invoking the CLI
with LDAP support enabled requires a number of additional command line
options. You can either use `--keystore-*` or `--truststore-*` properties
to secure TLS connection. The simplest way to invoke the CLI is with a
wrapper script.

```text
#!/bin/bash

./trino \
--server https://trino-coordinator.example.com:8443 \
--keystore-path /tmp/trino.jks \
--keystore-password password \
--truststore-path /tmp/trino_truststore.jks \
--truststore-password password \
--catalog <catalog> \
--schema <schema> \
--user <LDAP user> \
--password
```

Find details on the options used in {ref}`cli-tls` and
{ref}`cli-username-password-auth`.

## Troubleshooting

### Java keystore file verification

Verify the password for a keystore file and view its contents using
{ref}`troubleshooting-keystore`.

### Debug Trino to LDAP server issues

If you need to debug issues with Trino communicating with the LDAP server,
you can change the [log level](logging-configuration) for the LDAP authenticator:

```none
io.trino.plugin.password=DEBUG
```

### TLS debugging for Trino CLI

If you encounter any TLS related errors when running the Trino CLI, you can run
the CLI using the `-Djavax.net.debug=ssl` parameter for debugging. Use the
Trino CLI executable JAR to enable this. For example:

```text
java -Djavax.net.debug=ssl \
-jar \
trino-cli-<version>-executable.jar \
--server https://coordinator:8443 \
<other_cli_arguments>
```

#### Common TLS/SSL errors

##### java.security.cert.CertificateException: No subject alternative names present

This error is seen when the Trino coordinator’s certificate is invalid, and does not have the IP you provide
in the `--server` argument of the CLI. You have to regenerate the coordinator's TLS certificate
with the appropriate {abbr}`SAN (Subject Alternative Name)` added.

Adding a SAN to this certificate is required in cases where `https://` uses IP address in the URL, rather
than the domain contained in the coordinator's certificate, and the certificate does not contain the
{abbr}`SAN (Subject Alternative Name)` parameter with the matching IP address as an alternative attribute.

#### Authentication or TLS errors with JDK upgrade

Starting with the JDK 8u181 release, to improve the robustness of LDAPS
(secure LDAP over TLS) connections, endpoint identification algorithms were
enabled by default. See release notes
[from Oracle](https://www.oracle.com/technetwork/java/javase/8u181-relnotes-4479407.html#JDK-8200666.).
The same LDAP server certificate on the Trino coordinator, running on JDK
version >= 8u181, that was previously able to successfully connect to an
LDAPS server, may now fail with the following error:

```text
javax.naming.CommunicationException: simple bind failed: ldapserver:636
[Root exception is javax.net.ssl.SSLHandshakeException: java.security.cert.CertificateException: No subject alternative DNS name matching ldapserver found.]
```

If you want to temporarily disable endpoint identification, you can add the
property `-Dcom.sun.jndi.ldap.object.disableEndpointIdentification=true`
to Trino's `jvm.config` file. However, in a production environment, we
suggest fixing the issue by regenerating the LDAP server certificate so that
the certificate {abbr}`SAN (Subject Alternative Name)` or certificate subject
name matches the LDAP server.
