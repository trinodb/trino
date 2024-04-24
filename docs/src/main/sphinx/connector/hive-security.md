# Hive connector security configuration

(hive-security-impersonation)=

## Overview

The Hive connector supports both authentication and authorization.

Trino can impersonate the end user who is running a query. In the case of a
user running a query from the command line interface, the end user is the
username associated with the Trino CLI process or argument to the optional
`--user` option.

Authentication can be configured with or without user impersonation on
Kerberized Hadoop clusters.

## Requirements

End user authentication limited to Kerberized Hadoop clusters. Authentication
user impersonation is available for both Kerberized and non-Kerberized clusters.

You must ensure that you meet the Kerberos, user impersonation and keytab
requirements described in this section that apply to your configuration.

(hive-security-kerberos-support)=

### Kerberos

In order to use the Hive connector with a Hadoop cluster that uses `kerberos`
authentication, you must configure the connector to work with two services on
the Hadoop cluster:

- The Hive metastore Thrift service
- The Hadoop Distributed File System (HDFS)

Access to these services by the Hive connector is configured in the properties
file that contains the general Hive connector configuration.

:::{note}
If your `krb5.conf` location is different from `/etc/krb5.conf` you
must set it explicitly using the `java.security.krb5.conf` JVM property
in `jvm.config` file.

Example: `-Djava.security.krb5.conf=/example/path/krb5.conf`.
:::

:::{warning}
Access to the Trino coordinator must be secured e.g., using Kerberos or
password authentication, when using Kerberos authentication to Hadoop services.
Failure to secure access to the Trino coordinator could result in unauthorized
access to sensitive data on the Hadoop cluster. Refer to {doc}`/security` for
further information.

See {doc}`/security/kerberos` for information on setting up Kerberos authentication.
:::

(hive-security-additional-keytab)=
#### Keytab files

Keytab files contain encryption keys that are used to authenticate principals
to the Kerberos {abbr}`KDC (Key Distribution Center)`. These encryption keys
must be stored securely; you must take the same precautions to protect them
that you take to protect ssh private keys.

In particular, access to keytab files must be limited to only the accounts
that must use them to authenticate. In practice, this is the user that
the Trino process runs as. The ownership and permissions on keytab files
must be set to prevent other users from reading or modifying the files.

Keytab files must be distributed to every node running Trino. Under common
deployment situations, the Hive connector configuration is the same on all
nodes. This means that the keytab needs to be in the same location on every
node.

You must ensure that the keytab files have the correct permissions on every
node after distributing them.

(configuring-hadoop-impersonation)=

### Impersonation in Hadoop

In order to use impersonation, the Hadoop cluster must be
configured to allow the user or principal that Trino is running as to
impersonate the users who log in to Trino. Impersonation in Hadoop is
configured in the file {file}`core-site.xml`. A complete description of the
configuration options can be found in the [Hadoop documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html#Configurations).

## Authentication

The default security configuration of the {doc}`/connector/hive` does not use
authentication when connecting to a Hadoop cluster. All queries are executed as
the user who runs the Trino process, regardless of which user submits the
query.

The Hive connector provides additional security options to support Hadoop
clusters that have been configured to use {ref}`Kerberos
<hive-security-kerberos-support>`.

When accessing {abbr}`HDFS (Hadoop Distributed File System)`, Trino can
{ref}`impersonate<hive-security-impersonation>` the end user who is running the
query. This can be used with HDFS permissions and {abbr}`ACLs (Access Control
Lists)` to provide additional security for data.

### HDFS authentication

In a Kerberized Hadoop cluster, Trino authenticates to HDFS using Kerberos.
Kerberos authentication for HDFS is configured in the connector's properties
file using the following optional properties:

:::{list-table} HDFS authentication properties
:widths: 30, 55, 15
:header-rows: 1

* - Property value
  - Description
  - Default
* - `hive.hdfs.authentication.type`
  - HDFS authentication type; one of `NONE` or `KERBEROS`. When using the
    default value of `NONE`, Kerberos authentication is disabled, and no other
    properties must be configured.

    When set to `KERBEROS`, the Hive connector authenticates to HDFS using
    Kerberos.
  - `NONE`
* - `hive.hdfs.impersonation.enabled`
  - Enable HDFS end-user impersonation. Impersonating the end user can provide
    additional security when accessing HDFS if HDFS permissions or ACLs are
    used.

    HDFS Permissions and ACLs are explained in the [HDFS Permissions
    Guide](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html).
  - `false`
* - `hive.hdfs.trino.principal`
  - The Kerberos principal Trino uses when connecting to HDFS.

    Example: `trino-hdfs-superuser/trino-server-node@EXAMPLE.COM` or
    `trino-hdfs-superuser/_HOST@EXAMPLE.COM`.

    The `_HOST` placeholder can be used in this property value. When connecting
    to HDFS, the Hive connector substitutes in the hostname of the **worker**
    node Trino is running on. This is useful if each worker node has its own
    Kerberos principal.
  -
* - `hive.hdfs.trino.keytab`
  - The path to the keytab file that contains a key for the principal specified
    by `hive.hdfs.trino.principal`. This file must be readable by the operating
    system user running Trino.
  -
* - `hive.hdfs.wire-encryption.enabled`
  - Enable HDFS wire encryption. In a Kerberized Hadoop cluster that uses HDFS
    wire encryption, this must be set to `true` to enable Trino to access HDFS.
    Note that using wire encryption may impact query execution performance.
  -
:::

#### Configuration examples

The following sections describe the configuration properties and values needed
for the various authentication configurations with HDFS and the Hive connector.

(hive-security-simple)=

##### Default `NONE` authentication without impersonation

```text
hive.hdfs.authentication.type=NONE
```

The default authentication type for HDFS is `NONE`. When the authentication
type is `NONE`, Trino connects to HDFS using Hadoop's simple authentication
mechanism. Kerberos is not used.

(hive-security-simple-impersonation)=

##### `NONE` authentication with impersonation

```text
hive.hdfs.authentication.type=NONE
hive.hdfs.impersonation.enabled=true
```

When using `NONE` authentication with impersonation, Trino impersonates
the user who is running the query when accessing HDFS. The user Trino is
running as must be allowed to impersonate this user, as discussed in the
section {ref}`configuring-hadoop-impersonation`. Kerberos is not used.

(hive-security-kerberos)=

##### `KERBEROS` authentication without impersonation

```text
hive.hdfs.authentication.type=KERBEROS
hive.hdfs.trino.principal=hdfs@EXAMPLE.COM
hive.hdfs.trino.keytab=/etc/trino/hdfs.keytab
```

When the authentication type is `KERBEROS`, Trino accesses HDFS as the
principal specified by the `hive.hdfs.trino.principal` property. Trino
authenticates this principal using the keytab specified by the
`hive.hdfs.trino.keytab` keytab.

Keytab files must be distributed to every node in the cluster that runs Trino.

{ref}`Additional Information About Keytab Files.<hive-security-additional-keytab>`

(hive-security-kerberos-impersonation)=

##### `KERBEROS` authentication with impersonation

```text
hive.hdfs.authentication.type=KERBEROS
hive.hdfs.impersonation.enabled=true
hive.hdfs.trino.principal=trino@EXAMPLE.COM
hive.hdfs.trino.keytab=/etc/trino/hdfs.keytab
```

When using `KERBEROS` authentication with impersonation, Trino impersonates
the user who is running the query when accessing HDFS. The principal
specified by the `hive.hdfs.trino.principal` property must be allowed to
impersonate the current Trino user, as discussed in the section
{ref}`configuring-hadoop-impersonation`. Trino authenticates
`hive.hdfs.trino.principal` using the keytab specified by
`hive.hdfs.trino.keytab`.

Keytab files must be distributed to every node in the cluster that runs Trino.

{ref}`Additional Information About Keytab Files.<hive-security-additional-keytab>`
