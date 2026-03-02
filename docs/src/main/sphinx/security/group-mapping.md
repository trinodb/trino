# Group mapping

Group providers in Trino map usernames onto groups for easier access control
and resource group management.

Configure a group provider by creating an `etc/group-provider.properties` file
on the coordinator:

```properties
group-provider.name=file
```
The value for `group-provider.name` must be either `file` or `ldap` and the
configuration of the chosen group provider must be included in the same file.

:::{list-table} Group provider configuration
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description

* - `group-provider.name`
  - Name of the group provider to use.
    Supported values are:

      * `file`: [See configuration](file-group-provider)
      * `ldap`: [See configuration](ldap-group-provider)
* - `group-provider.group-case`
  - Optional transformation of the case of the group name.
    Supported values are:

    * `keep`: default, no conversion
    * `upper`: convert group name to _UPPERCASE_
    * `lower`: converts the group name to _lowercase_

    Defaults to `keep`.
:::

## Integration with access control

Groups resolved by the group provider are passed to Trino’s system access
control engine. Access control rules can reference these group names to grant
or restrict permissions.

(file-group-provider)=
## File group provider

The file group provider resolves group memberships with the configuration in
the group-provider.properties file on the coordinator.

### Configuration

Enable the file group provider by creating an `etc/group-provider.properties`
file on the coordinator:

```properties
group-provider.name=file
file.group-file=/path/to/group.txt
```

The following configuration properties are available:

:::{list-table} File group provider configuration
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `file.group-file`
  - Path of the group file.
* - `file.refresh-period`
  - [Duration](prop-type-duration) between refreshing the group mapping
    configuration from the file. Defaults to `5s`.
:::

### Group file format

The group file contains a list of groups and members, one per line,
separated by a colon. Users are separated by a comma.

```text
group_name:user_1,user_2,user_3
```

(ldap-group-provider)=
## LDAP group provider

The LDAP group provider resolves user group memberships from configuration
retrieved from an LDAP server. This allows access rules to be defined based on
LDAP groups instead of individual users.

### Configuration

Enable LDAP group provider by creating an `etc/group-provider.properties` file
on the coordinator and add further configuration for the LDAP server
connections and other information as detailed in the following sections.

```properties
group-provider.name=ldap
```

:::{list-table} Generic LDAP properties
:widths: 40, 60
:header-rows: 1
* - Property name
  - Description
* - `ldap.url`
  - LDAP server URI.  For example, `ldap://host:389` or `ldaps://host:636`.
* - `ldap.allow-insecure`
  - Allow insecure connection to the LDAP server. Defaults to `false`.
* - `ldap.ssl.keystore.path`
  - Path to the PEM or JKS key store.
* - `ldap.ssl.keystore.password`
  - Password for the key store.
* - `ldap.ssl.truststore.path`
  - Path to the PEM or JKS trust store.
* - `ldap.ssl.truststore.password`
  - Password for the trust store.
* - `ldap.ignore-referrals`
  - Referrals allow finding entries across multiple LDAP servers. Ignore them
    to only search within one LDAP server. Defaults to `false`.
* - `ldap.timeout.connect`
  - Timeout [duration](prop-type-duration) for establishing a connection.
    Defaults to `1m`.
* - `ldap.timeout.read`
  - Timeout [duration](prop-type-duration) for reading data from LDAP.
    Defaults to `1m`.
* - `ldap.admin-user`
  - Bind distinguished name for admin user. For example,
    `CN=UserName,OU=City,OU=State,DC=domain,DC=domain_root`
* - `ldap.admin-password`
  - Bind password used for the admin user.
* - `ldap.user-base-dn`
  - Base distinguished name for users. For example, `dc=example,dc=com`.
* - `ldap.user-search-filter`
  - LDAP filter to find user entries; `{0}` is replaced with the Trino username.
    For example, `(cn={0})`
* - `ldap.group-name-attribute`
  - Attribute to extract group name from group entry. For example, `cn`.
* - `ldap.use-group-filter`
  - Whether to use search-based group resolution. Defaults to `true`.
    When `false`, Trino uses the attribute-based method.
:::

Group resolution behavior is controlled by the `ldap.use-group-filter` property.
With search-based group resolution, Trino searches for group entries that
include the user DN. This requires the following properties:

:::{list-table} Search-based group resolution
:widths: 40, 60
:header-rows: 1
* - Property name
  - Description
* - `ldap.group-base-dn`
  - Base distinguished name for groups. For example, `dc=example,dc=com`.
* - `ldap.group-search-filter`
  - Search filter for group documents. For example, `(cn=trino_*)`.
* - `ldap.group-search-member-attribute`
  - Attribute from group documents used for filtering by member. For example,
    `cn`.
:::

In case of attribute-based group resolution, Trino reads the group list
directly from a user attribute. This requires the following property:

:::{list-table} Attribute-based (single query) group resolution
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `ldap.user-member-of-attribute`
  - Group membership attribute in user documents. For example, `memberOf`.
:::

### Example configurations

The following configuration is an example for an OpenLDAP (search-based)
group provider:

```properties
group-provider.name=ldap
group-provider.group-case=lower

ldap.url=ldap://ldap.example.com:389
ldap.admin-user=cn=admin,dc=example,dc=com
ldap.admin-password=your_password
ldap.group-name-attribute=cn
ldap.user-base-dn=ou=users,dc=example,dc=com
ldap.user-search-filter=(uid={0})
ldap.use-group-filter=true

ldap.group-base-dn=ou=groups,dc=example,dc=com
ldap.group-search-filter=(cn=trino_*)
ldap.group-search-member-attribute=member
```

The following configuration is an example for an Active Directory
(single query, attribute-based) group provider:

```properties
group-provider.name=ldap
group-provider.group-case=lower

ldap.url=ldaps://ad.example.com:636
ldap.admin-user=cn=admin,dc=example,dc=com
ldap.admin-password=your_password
ldap.group-name-attribute=cn
ldap.user-base-dn=ou=users,dc=example,dc=com
ldap.user-search-filter=(sAMAccountName={0})
ldap.use-group-filter=false

ldap.user-member-of-attribute=memberOf
```
