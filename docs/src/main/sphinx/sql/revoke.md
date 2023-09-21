# REVOKE

## Synopsis

```text
REVOKE [ GRANT OPTION FOR ]
( privilege [, ...] | ALL PRIVILEGES )
ON ( table_name | TABLE table_name | SCHEMA schema_name )
FROM ( user | USER user | ROLE role )
```

## Description

Revokes the specified privileges from the specified grantee.

Specifying `ALL PRIVILEGES` revokes {doc}`delete`, {doc}`insert` and {doc}`select` privileges.

Specifying `ROLE PUBLIC` revokes privileges from the `PUBLIC` role. Users will retain privileges assigned to them directly or via other roles.

If the optional `GRANT OPTION FOR` clause is specified, only the `GRANT OPTION`
is removed. Otherwise, both the `GRANT` and `GRANT OPTION` are revoked.

For `REVOKE` statement to succeed, the user executing it should possess the specified privileges as well as the `GRANT OPTION` for those privileges.

Revoke on a table revokes the specified privilege on all columns of the table.

Revoke on a schema revokes the specified privilege on all columns of all tables of the schema.

## Examples

Revoke `INSERT` and `SELECT` privileges on the table `orders` from user `alice`:

```
REVOKE INSERT, SELECT ON orders FROM alice;
```

Revoke `DELETE` privilege on the schema `finance` from user `bob`:

```
REVOKE DELETE ON SCHEMA finance FROM bob;
```

Revoke `SELECT` privilege on the table `nation` from everyone, additionally revoking the privilege to grant `SELECT` privilege:

```
REVOKE GRANT OPTION FOR SELECT ON nation FROM ROLE PUBLIC;
```

Revoke all privileges on the table `test` from user `alice`:

```
REVOKE ALL PRIVILEGES ON test FROM alice;
```

## Limitations

Some connectors have no support for `REVOKE`.
See connector documentation for more details.

## See also

{doc}`deny`, {doc}`grant`, {doc}`show-grants`
