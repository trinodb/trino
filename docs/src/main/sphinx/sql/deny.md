# DENY

## Synopsis

```text
DENY ( privilege [, ...] | ( ALL PRIVILEGES ) )
ON [ BRANCH branch_name IN ] ( table_name | TABLE table_name | SCHEMA schema_name)
TO ( user | USER user | ROLE role )
```

## Description

Denies the specified privileges to the specified grantee.

Deny on a table rejects the specified privilege on all current and future
columns of the table.

Deny on a schema rejects the specified privilege on all current and future
columns of all current and future tables of the schema.

## Examples

Deny `INSERT` and `SELECT` privileges on the table `orders`
to user `alice`:

```
DENY INSERT, SELECT ON orders TO alice;
```

Deny `DELETE` privilege on the schema `finance` to user `bob`:

```
DENY DELETE ON SCHEMA finance TO bob;
```

Deny `SELECT` privilege on the table `orders` to everyone:

```
DENY SELECT ON orders TO ROLE PUBLIC;
```

Deny `INSERT` privilege on the `audit` branch of the `orders` table to user `alice`:

```sql
DENY INSERT ON BRANCH audit IN orders TO alice;
```

## Limitations

The system access controls as well as the connectors provided by default
in Trino have no support for `DENY`.

## See also

{doc}`grant`, {doc}`revoke`, {doc}`show-grants`
