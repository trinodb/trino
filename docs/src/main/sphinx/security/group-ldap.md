# LDAP group provider

Trino supports resolving user group memberships from an LDAP server for system access control. This allows access rules to be defined based on LDAP groups instead of individual users.

## Configuration

Enable LDAP group provider by creating an `etc/group-provider.properties` file on the coordinator:
```properties
group-provider.name=ldap
```

### Generic LDAP properties

| Property                       | Description                                                                                                            | Example                                                  |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| `ldap.url`                     | LDAP server URI.                                                                                                       | `ldap://host:389` or `ldaps://host:636`)                 |
| `ldap.allow-insecure`          | Allow insecure connection to the LDAP server                                                                           |                                                          |
| `ldap.ssl.keystore.path`       | Path to the PEM or JKS key store                                                                                       |                                                          |
| `ldap.ssl.keystore.password`   | Password for the key store                                                                                             |                                                          |
| `ldap.ssl.truststore.path`     | Path to the PEM or JKS trust store                                                                                     |                                                          |
| `ldap.ssl.truststore.password` | Password for the trust store                                                                                           |                                                          |
| `ldap.ignore-referrals`        | Referrals allow finding entries across multiple LDAP servers. Ignore them to only search within 1 LDAP server          |                                                          |
| `ldap.timeout.connect`         | Timeout for establishing a connection                                                                                  |                                                          |
| `ldap.timeout.read`            | Timeout for reading data from LDAP                                                                                     |                                                          |
| `ldap.admin-user`              | Bind distinguished name for admin user.                                                                                | `CN=User Name,OU=City,OU=State,DC=domain,DC=domain_root` |
| `ldap.admin-password`          | Bind password used for admin user.                                                                                     | `password1234`                                           |
| `ldap.user-base-dn`            | Base distinguished name for users.                                                                                     | `dc=example,dc=com`                                      |
| `ldap.user-search-filter`      | LDAP filter to find user entries; `{0}` is replaced with the Trino username.                                           | `(cn={0})`                                               |
| `ldap.group-name-attribute`    | Attribute to extract group name from group entry.                                                                      | `cn`                                                     |
| `ldap.use-group-filter`        | Whether to use search-based group resolution. Defaults to `true`. If `false`, Trino uses the attribute-based method.   |                                                          |

## Group resolution strategy

Group resolution behavior is controlled by the `ldap.use-group-filter` property.

| Property                 | Description                                                                                                          |
|--------------------------|----------------------------------------------------------------------------------------------------------------------|
| `ldap.use-group-filter`  | Whether to use search-based group resolution. Defaults to `true`. If `false`, Trino uses the attribute-based method. |

### Search-based group resolution

Trino searches for group entries that include the user DN. This requires the following properties:

| Property                             | Description                                                  | Example             |
|--------------------------------------|--------------------------------------------------------------|---------------------|
| `ldap.group-base-dn`                 | Base distinguished name for groups.                          | `dc=example,dc=com` |
| `ldap.group-search-filter`           | Search filter for group documents.                           | `(cn=trino_*)`      |
| `ldap.group-search-member-attribute` | Attribute from group documents used for filtering by member. | `cn`                |

### attribute-based (single query) group resolution

Trino reads the group list directly from a user attribute. This requires the following property:

| Property                         | Description                                   | Example    |
|----------------------------------|-----------------------------------------------|------------|
| `ldap.user-member-of-attribute`  | Group membership attribute in user documents. | `memberOf` |
 
## Example configurations

### OpenLDAP (search-based)

```properties
group-provider.name=ldap

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

### Active Directory (single query, attribute-based)

```properties
group-provider.name=ldap

ldap.url=ldaps://ad.example.com:636
ldap.admin-user=cn=admin,dc=example,dc=com
ldap.admin-password=your_password
ldap.group-name-attribute=cn
ldap.user-base-dn=ou=users,dc=example,dc=com
ldap.user-search-filter=(sAMAccountName={0})
ldap.use-group-filter=false

ldap.user-member-of-attribute=memberOf
```

## Integration with Access Control

Groups resolved by the LDAP provider are passed to Trino’s system access control engine. Access control rules can reference these group names to grant or restrict permissions.
