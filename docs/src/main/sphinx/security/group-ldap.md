# LDAP group provider

Trino supports resolving user group memberships from an LDAP server for system access control. This allows access rules to be defined based on LDAP groups instead of individual users.

## Configuration

Enable LDAP group provider by creating an `etc/group-provider.properties` file on the coordinator:

```properties
group-provider.name=ldap
```

## Configuration properties

| Property                     | Description                                                                                     |
|------------------------------|------------------------------------------------------------------------------------------------|
| `ldap.url`                   | LDAP server URI (e.g., `ldap://host:389` or `ldaps://host:636`)                               |
| `ldap.bind-dn`               | Distinguished Name of the LDAP bind user                                                      |
| `ldap.bind-password`         | Password for the bind user                                                                     |
| `ldap.user-search-base`      | Base DN for user searches                                                                      |
| `ldap.user-search-filter`    | LDAP filter to find user entries; `{0}` is replaced with the Trino username                     |
| `ldap.use-group-filter`      | Whether to use search-based group resolution. Defaults to `true`. If `false`, Trino uses the attribute-based method. |
| `ldap.user-member-of-attribute` | (Required if `ldap.use-group-filter=false`) The attribute on the user entry that lists group DNs (e.g., `memberOf`). |
| `ldap.group-search-base`     | Base DN for group searches (used only when `ldap.use-group-filter=true`)                        |
| `ldap.group-search-filter`   | LDAP filter to find groups containing the user (e.g., `(&(objectClass=groupOfNames)(member={0}))`) |
| `ldap.group-name-attribute`  | Attribute to extract group name from group entry (e.g., `cn`)                                 |
| `ldap.group-member-attribute`| Attribute in group entry listing members (e.g., `member`)                                     |

## Group resolution strategy

Group resolution behavior is controlled by the `ldap.use-group-filter` property:

- **Search-based group resolution** (`ldap.use-group-filter=true`, the default):

  Trino searches for group entries that include the user DN. This requires:

  - `ldap.group-search-base`
  - `ldap.group-search-filter`
  - `ldap.group-name-attribute`
  - `ldap.group-member-attribute`

  Trino uses the `SearchBasedLdapGroupProvider` class for this resolution.

- **Attribute-based group resolution** (`ldap.use-group-filter=false`):

  Trino reads the group list directly from a user attribute (e.g., `memberOf`). This requires:

  - `ldap.user-member-of-attribute`
  - `ldap.group-name-attribute`

  Trino uses the `AttributeBasedLdapGroupProvider` class for this resolution.

:::{note}
If you set `ldap.use-group-filter=false`, you **must** set `ldap.user-member-of-attribute`.
:::

## Example configurations

### Active Directory (attribute-based)

```properties
group-provider.name=ldap

ldap.url=ldaps://ad.example.com:636
ldap.bind-dn=cn=admin,dc=example,dc=com
ldap.bind-password=your_password

ldap.user-search-base=ou=users,dc=example,dc=com
ldap.user-search-filter=(sAMAccountName={0})
ldap.use-group-filter=false
ldap.user-member-of-attribute=memberOf
ldap.group-name-attribute=cn
```

### OpenLDAP (search-based)

```properties
group-provider.name=ldap

ldap.url=ldap://ldap.example.com:389
ldap.bind-dn=cn=admin,dc=example,dc=com
ldap.bind-password=your_password

ldap.user-search-base=ou=users,dc=example,dc=com
ldap.user-search-filter=(uid={0})
ldap.use-group-filter=true

ldap.group-search-base=ou=groups,dc=example,dc=com
ldap.group-search-filter=(&(objectClass=groupOfNames)(member={0}))
ldap.group-member-attribute=member
ldap.group-name-attribute=cn
```

## Integration with Access Control

Groups resolved by the LDAP provider are passed to Trino’s system access control engine. Access control rules can reference these group names to grant or restrict permissions.
