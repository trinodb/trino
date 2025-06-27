# LDAP group provider

Trino supports resolving user group memberships from an LDAP server for system access control. This allows access rules to be defined based on LDAP groups instead of individual users.

## Configuration

Enable LDAP group provider by creating an `etc/group-provider.properties` file on the coordinator:

```properties
group-provider.name=ldap
```

## Configuration properties


### Generic LDAP properties

| Property                             | Description                                                                                                          |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| `ldap.url`                           | LDAP server URI. Example: `ldap://host:389` or `ldaps://host:636`)                                                   |
| `ldap.allow-insecure`                | Allow insecure connection to the LDAP server                                                                         |
| `ldap.ssl.keystore.path`             | Path to the PEM or JKS key store                                                                                     |
| `ldap.ssl.keystore.password`         | Password for the key store                                                                                           |
| `ldap.ssl.truststore.path`           | Path to the PEM or JKS trust store                                                                                   |
| `ldap.ssl.truststore.password`       | Password for the trust store                                                                                         |
| `ldap.ignore-referrals`              | Referrals allow finding entries across multiple LDAP servers. Ignore them to only search within 1 LDAP server        |
| `ldap.timeout.connect`               | Timeout for establishing a connection                                                                                |
| `ldap.timeout.read`                  | Timeout for reading data from LDAP                                                                                   |
| `ldap.admin-user`                    | Bind distinguished name for admin user. Example: `CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root`      |
| `ldap.admin-password`                | Bind password used for admin user. Example: `password1234`                                                           |
| `ldap.user-base-dn`                  | Base distinguished name for users. Example: `dc=example,dc=com`                                                      |
| `ldap.user-search-filter`            | LDAP filter to find user entries; `{0}` is replaced with the Trino username. Example: `(cn={0})`                     |
| `ldap.group-name-attribute`          | Attribute to extract group name from group entry. Example: `cn`                                                      |
| `ldap.use-group-filter`              | Whether to use search-based group resolution. Defaults to `true`. If `false`, Trino uses the attribute-based method. |

## Group resolution strategy

Group resolution behavior is controlled by the `ldap.use-group-filter` property:

### Search-based group resolution** (`ldap.use-group-filter=true`, the default):

Trino searches for group entries that include the user DN. This requires:

| Property                             | Description                                                                                                          |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| `ldap.group-base-dn`                 | Base distinguished name for groups. Example: `dc=example,dc=com`                                                     |
| `ldap.group-search-filter`           | Search filter for group documents. Example: `(cn=trino_*)`                                                           |
| `ldap.group-search-member-attribute` | Attribute from group documents used for filtering by member. Example: `cn`                                           |


### Single query group resultion ** (`ldap.use-group-filter=false`):

Trino reads the group list directly from a user attribute (Example, `memberOf`). This requires:

| Property                             | Description                                                                                                          |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| `ldap.user-member-of-attribute`      | Group membership attribute in user documents. Example: `memberOf`                                                    |
 
# Example configurations

### OpenLDAP (search-based)

```properties
group-provider.name=ldap

ldap.url=ldap://ldap.example.com:389
ldap.admin-user=cn=admin,dc=example,dc=com
ldap.admin-password=your_password
ldap.group-name-attribute=cn
ldap.use-group-filter=true
ldap.user-base-dn=ou=users,dc=example,dc=com
ldap.user-search-filter=(uid={0})

ldap.group-base-dn=ou=groups,dc=example,dc=com
ldap.group-search-filter=(cn=trino_*)
ldap.group-search-member-attribute=member
```

### Active Directory (attribute-based)

```properties
group-provider.name=ldap

ldap.url=ldaps://ad.example.com:636
ldap.admin-user=cn=admin,dc=example,dc=com
ldap.admin-password=your_password
ldap.group-name-attribute=cn
ldap.use-group-filter=false
ldap.user-base-dn=ou=users,dc=example,dc=com
ldap.user-search-filter=(sAMAccountName={0})

ldap.user-member-of-attribute=memberOf
```

## Integration with Access Control

Groups resolved by the LDAP provider are passed to Trino’s system access control engine. Access control rules can reference these group names to grant or restrict permissions.
