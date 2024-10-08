# HDFS file system support

Trino includes support to access the [Hadoop Distributed File System
(HDFS)](https://hadoop.apache.org/) with a catalog using the Delta Lake, Hive,
Hudi, or Iceberg connectors.

Support for HDFS is not enabled by default, but can be activated by setting the
`fs.hadoop.enabled` property to `true` in your catalog configuration file.

Apache Hadoop HDFS 2.x and 3.x are supported.

## General configuration

Use the following properties to configure general aspects of HDFS support:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `fs.hadoop.enabled`
  - Activate the support for HDFS access. Defaults to `false`. Set to `true` to
    use HDFS and enable all other properties.
* - `hive.config.resources`
  - An optional, comma-separated list of HDFS configuration files. These files
    must exist on the machines running Trino. For basic setups, Trino configures
    the HDFS client automatically and does not require any configuration files.
    In some cases, such as when using federated HDFS or NameNode high
    availability, it is necessary to specify additional HDFS client options to
    access your HDFS cluster in the HDFS XML configuration files and reference
    them with this parameter:

    ```text
    hive.config.resources=/etc/hadoop/conf/core-site.xml
    ```

    Only specify additional configuration files if necessary for your setup, and
    reduce the configuration files to have the minimum set of required
    properties. Additional properties may cause problems.
* - `hive.fs.new-directory-permissions`
  - Controls the permissions set on new directories created for schemas and
    tables. Value must either be `skip` or an octal number, with a leading 0. If
    set to `skip`, permissions of newly created directories are not set by
    Trino. Defaults to `0777`.
* - `hive.fs.new-file-inherit-ownership`
  - Flag to determine if new files inherit the ownership information from the
    directory. Defaults to `false`.
* - `hive.dfs.verify-checksum`
  - Flag to determine if file checksums must be verified. Defaults to `false`.
* - `hive.dfs.ipc-ping-interval`
  - [Duration](prop-type-duration) between IPC pings from Trino to HDFS.
    Defaults to `10s`.
* - `hive.dfs-timeout`
  - Timeout [duration](prop-type-duration) for access operations on HDFS.
    Defaults to `60s`.
* - `hive.dfs.connect.timeout`
  - Timeout [duration](prop-type-duration) for connection operations to HDFS.
    Defaults to `500ms`.
* - `hive.dfs.connect.max-retries`
  - Maximum number of retries for HDFS connection attempts. Defaults to `5`.
* - `hive.dfs.key-provider.cache-ttl`
  - Caching time [duration](prop-type-duration) for the key provider. Defaults
    to `30min`.
* - `hive.dfs.domain-socket-path`
  - Path to the UNIX domain socket for the DataNode. The path must exist on each
    node. For example, `/var/lib/hadoop-hdfs/dn_socket`.
* - `hive.hdfs.socks-proxy`
  - URL for a SOCKS proxy to use for accessing HDFS. For example,
    `hdfs-master:1180`.
* - `hive.hdfs.wire-encryption.enabled`
  - Enable HDFS wire encryption. In a Kerberized Hadoop cluster that uses HDFS
    wire encryption, this must be set to `true` to enable Trino to access HDFS.
    Note that using wire encryption may impact query execution performance.
    Defaults to `false`.
* - `hive.fs.cache.max-size`
  - Maximum number of cached file system objects in the HDFS cache. Defaults to
    `1000`.
* - `hive.dfs.replication`
  - Integer value to set the HDFS replication factor. By default, no value is
    set.
:::

## Security

HDFS support includes capabilities for user impersonation and Kerberos
authentication. The following properties are available:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property value
  - Description
* - `hive.hdfs.authentication.type`
  - Configure the authentication to use no authentication (`NONE`) or Kerberos
    authentication (`KERBEROS`). Defaults to `NONE`.
* - `hive.hdfs.impersonation.enabled`
  - Enable HDFS end-user impersonation. Defaults to `false`. See details in
    [](hdfs-security-impersonation).
* - `hive.hdfs.trino.principal`
  - The Kerberos principal Trino uses when connecting to HDFS. Example:
    `trino-hdfs-superuser/trino-server-node@EXAMPLE.COM` or
    `trino-hdfs-superuser/_HOST@EXAMPLE.COM`.

    The `_HOST` placeholder can be used in this property value. When connecting
    to HDFS, the Hive connector substitutes in the hostname of the **worker**
    node Trino is running on. This is useful if each worker node has its own
    Kerberos principal.
* - `hive.hdfs.trino.keytab`
  - The path to the keytab file that contains a key for the principal specified
    by `hive.hdfs.trino.principal`. This file must be readable by the operating
    system user running Trino.
* - `hive.hdfs.trino.credential-cache.location`
  - The location of the credential-cachewiuth the credentials for the principal
    to use to access HDFS. Altenative to `hive.hdfs.trino.keytab`.
:::

The default security configuration does not use authentication when connecting
to a Hadoop cluster (`hive.hdfs.authentication.type=NONE`). All queries are
executed as the OS user who runs the Trino process, regardless of which user
submits the query.

Before running any `CREATE TABLE` or `CREATE TABLE AS` statements for Hive
tables in Trino, you must check that the user Trino is using to access HDFS has
access to the Hive warehouse directory. The Hive warehouse directory is
specified by the configuration variable `hive.metastore.warehouse.dir` in
`hive-site.xml`, and the default value is `/user/hive/warehouse`.

For example, if Trino is running as `nobody`, it accesses
HDFS as `nobody`. You can override this username by setting the
`HADOOP_USER_NAME` system property in the Trino [](jvm-config), replacing
`hdfs_user` with the appropriate username:

```text
-DHADOOP_USER_NAME=hdfs_user
```

The `hive` user generally works, since Hive is often started with the `hive`
user and this user has access to the Hive warehouse.

(hdfs-security-impersonation)=
### HDFS impersonation

HDFS impersonation is enabled by adding `hive.hdfs.impersonation.enabled=true`
to the catalog properties file. With this configuration HDFS, Trino can
impersonate the end user who is running the query. This can be used with HDFS
permissions and {abbr}`ACLs (Access Control Lists)` to provide additional
security for data. HDFS permissions and ACLs are explained in the [HDFS
Permissions
Guide](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html).

To use impersonation, the Hadoop cluster must be configured to allow the user or
principal that Trino is running as to impersonate the users who log in to Trino.
Impersonation in Hadoop is configured in the file {file}`core-site.xml`. A
complete description of the configuration options is available in the [Hadoop
documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html#Configurations).

In the case of a user running a query from the [command line
interface](/client/cli), the end user is the username associated with the Trino
CLI process or argument to the optional `--user` option.

(hdfs-security-kerberos)=
### HDFS Kerberos authentication

To use Trino with a Hadoop cluster that uses Kerberos authentication, you must
configure the catalog in the catalog properties file to work with two services
on the Hadoop cluster:

- The Hive metastore Thrift service, see [](hive-thrift-metastore-authentication)
- The Hadoop Distributed File System (HDFS), see examples in
  [](hive-security-kerberos) or [](hive-security-kerberos-impersonation)


Both setups require that Kerberos is configured on each Trino node. Access to
the Trino coordinator must be secured, for example using Kerberos or password
authentication, when using Kerberos authentication to Hadoop services. Failure
to secure access to the Trino coordinator could result in unauthorized access to
sensitive data on the Hadoop cluster. Refer to {doc}`/security` for further
information, and specifically consider configuring [](/security/kerberos).

:::{note}
If your `krb5.conf` location is different from `/etc/krb5.conf` you must set it
explicitly using the `java.security.krb5.conf` JVM property in the `jvm.config`
file. For example, `-Djava.security.krb5.conf=/example/path/krb5.conf`.
:::

(hive-security-additional-keytab)=
#### Keytab files

Keytab files are needed for Kerberos authentication and contain encryption keys
that are used to authenticate principals to the Kerberos {abbr}`KDC (Key
Distribution Center)`. These encryption keys must be stored securely; you must
take the same precautions to protect them that you take to protect ssh private
keys.

In particular, access to keytab files must be limited to only the accounts
that must use them to authenticate. In practice, this is the user that
the Trino process runs as. The ownership and permissions on keytab files
must be set to prevent other users from reading or modifying the files.

Keytab files must be distributed to every node running Trino, and  must have the
correct permissions on every node after distributing them.

## Security configuration examples

The following sections describe the configuration properties and values needed
for the various authentication configurations with HDFS.

(hive-security-simple)=
### Default `NONE` authentication without impersonation

```text
hive.hdfs.authentication.type=NONE
```

The default authentication type for HDFS is `NONE`. When the authentication type
is `NONE`, Trino connects to HDFS using Hadoop's simple authentication
mechanism. Kerberos is not used.

(hive-security-simple-impersonation)=
### `NONE` authentication with impersonation

```text
hive.hdfs.authentication.type=NONE
hive.hdfs.impersonation.enabled=true
```

When using `NONE` authentication with impersonation, Trino impersonates the user
who is running the query when accessing HDFS. The user Trino is running as must
be allowed to impersonate this user, as discussed in the section
[](hdfs-security-impersonation). Kerberos is not used.

(hive-security-kerberos)=
### `KERBEROS` authentication without impersonation

```text
hive.hdfs.authentication.type=KERBEROS
hive.hdfs.trino.principal=trino@EXAMPLE.COM
hive.hdfs.trino.keytab=/etc/trino/trino.keytab
```

When the authentication type is `KERBEROS`, Trino accesses HDFS as the principal
specified by the `hive.hdfs.trino.principal` property. Trino authenticates this
principal using the keytab specified by the `hive.hdfs.trino.keytab` keytab.

(hive-security-kerberos-impersonation)=
### `KERBEROS` authentication with impersonation

```text
hive.hdfs.authentication.type=KERBEROS
hive.hdfs.impersonation.enabled=true
hive.hdfs.trino.principal=trino@EXAMPLE.COM
hive.hdfs.trino.keytab=/etc/trino/trino.keytab
```

When using `KERBEROS` authentication with impersonation, Trino impersonates the
user who is running the query when accessing HDFS. The principal specified by
the `hive.hdfs.trino.principal` property must be allowed to impersonate the
current Trino user, as discussed in the section [](hdfs-security-impersonation). Trino
authenticates `hive.hdfs.trino.principal` using the keytab specified by
`hive.hdfs.trino.keytab`.
