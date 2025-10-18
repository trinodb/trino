# CREATE BRANCH

## Synopsis

```text
CREATE [ OR REPLACE ] BRANCH [ IF NOT EXISTS ] branch_name
[ WITH ( property_name = expression [, ...] ) ]
IN TABLE table_name
[ FROM source_branch ]
```

## Description

Create a branch.

The optional `OR REPLACE` clause causes an existing branch with the
specified name to be replaced with the new branch definition. Support
for branch replacement varies across connectors. Refer to the
connector documentation for details.

The optional `IF NOT EXISTS` clause causes the error to be
suppressed if the branch already exists.

The optional `WITH` clause can be used to set properties
on the newly created branch.

The optional `FROM` clause can be used to set the source branch from which the
new branch is created.

## Examples

Create a new branch `audit` in the table `orders`:

```sql
CREATE BRANCH audit IN TABLE orders
```

Create a new branch `audit` in the table `orders` from the branch `dev`:

```sql
CREATE BRANCH audit IN TABLE orders FROM dev
```

## See also

{doc}`drop-branch`
