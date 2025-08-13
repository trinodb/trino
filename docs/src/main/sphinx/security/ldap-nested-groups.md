# LDAP Nested Groups Support

Trino supports nested LDAP groups, allowing users to inherit group memberships through group hierarchies. This feature enables more flexible and scalable group management in enterprise environments.

## Implementation

Trino uses `LDAP_MATCHING_RULE_IN_CHAIN` (OID: 1.2.840.113556.1.4.1941) for efficient single-query nested group resolution. This approach leverages the LDAP server's built-in capabilities to handle recursive group resolution in a single query, making it much more performant than client-side recursive approaches.

## Configuration

To enable nested groups support, add the following configuration to your Trino coordinator:

```properties
# Enable nested groups using LDAP_MATCHING_RULE_IN_CHAIN
ldap.use-chain-groups=true

# Base DN for groups
ldap.group-base-dn=ou=groups,dc=example,dc=com

# Attribute used for group membership (default: member)
ldap.group-search-member-attribute=member

# Optional: Filter for specific groups
ldap.group-search-filter=(cn=trino_*)
```

## Configuration Properties

| Property | Description | Default |
|----------|-------------|---------|
| `ldap.use-chain-groups` | Enable nested group resolution using LDAP_MATCHING_RULE_IN_CHAIN | `false` |
| `ldap.group-base-dn` | Base distinguished name for groups | Required |
| `ldap.group-search-member-attribute` | Attribute from group documents used for filtering by member | `member` |
| `ldap.group-search-filter` | Optional search filter for group documents | None |

## How It Works

The implementation uses `LDAP_MATCHING_RULE_IN_CHAIN` (OID: 1.2.840.113556.1.4.1941) to perform recursive group membership queries in a single LDAP operation:

1. **Single Query**: Uses `(member:1.2.840.113556.1.4.1941:=userDN)` to find all groups that contain the user directly or indirectly
2. **Efficient Resolution**: The LDAP server handles the recursive traversal internally
3. **No Depth Limits**: Automatically handles any depth of nesting without configuration
4. **No Cycle Issues**: The LDAP server handles circular references automatically

## Example Group Hierarchy

Consider the following LDAP group structure:

```
Grandparent Group
├── Parent Group 1
│   ├── Child Group A
│   │   └── User Alice
│   └── Child Group B
│       └── User Bob
└── Parent Group 2
    └── User Charlie
```

With nested groups enabled:
- **Alice** would be a member of: `Child Group A`, `Parent Group 1`, `Grandparent Group`
- **Bob** would be a member of: `Child Group B`, `Parent Group 1`, `Grandparent Group`
- **Charlie** would be a member of: `Parent Group 2`, `Grandparent Group`

## LDAP Schema Requirements

For nested groups to work properly, your LDAP schema should support:

1. **Group objects** with a `member` attribute that can contain both users and other groups
2. **LDAP_MATCHING_RULE_IN_CHAIN** support (available in Active Directory and most modern LDAP servers)
3. **Proper object classes** such as `groupOfNames`, `groupOfUniqueNames`, or Active Directory groups

### Active Directory Example

In Active Directory, groups typically use:
- `member` attribute for group members
- `memberOf` attribute for group memberships

### OpenLDAP Example

In OpenLDAP, you might use:
- `member` attribute for group members
- `memberOf` attribute for group memberships (requires overlay)

## Performance Considerations

The implementation is highly efficient:
- **Single LDAP Query**: All nested group resolution happens in one query
- **Server-side Processing**: The LDAP server handles the recursive traversal
- **No Depth Limits**: Automatically handles any nesting depth
- **Optimal Performance**: Best choice for production environments
- **No Configuration Complexity**: No need for depth limits or cycle detection

## Security Considerations

1. **Cycle Detection**: The LDAP server handles circular references automatically
2. **Access Control**: Ensure your LDAP admin user has read access to all relevant groups
3. **Audit Trail**: Consider logging group resolution for security auditing

## Troubleshooting

### Common Issues

1. **No nested groups returned**: Check that `ldap.use-chain-groups=true` is set
2. **Missing parent groups**: Verify `ldap.group-search-member-attribute` matches your LDAP schema
3. **LDAP server compatibility**: Ensure your LDAP server supports `LDAP_MATCHING_RULE_IN_CHAIN`

### Debugging

Enable debug logging to see group resolution details:

```properties
log.levels.io.trino.plugin.ldapgroup=DEBUG
```

This will show:
- User DN lookups
- Chain-based query details
- Performance metrics

## Migration from Non-Nested Groups

To migrate from the existing group providers to nested groups:

1. **Backup configuration**: Save your current LDAP group provider configuration
2. **Enable nested groups**: Add `ldap.use-chain-groups=true`
3. **Test thoroughly**: Verify that all expected groups are returned
4. **Monitor performance**: Watch for any performance impact
5. **Update documentation**: Update any group-based access control documentation

## Compatibility

The nested groups implementation is compatible with:
- Active Directory
- OpenLDAP (with appropriate overlays)
- Other LDAP servers that support `LDAP_MATCHING_RULE_IN_CHAIN`

It works alongside existing Trino security features:
- File-based access control
- System access control
- Resource group management
