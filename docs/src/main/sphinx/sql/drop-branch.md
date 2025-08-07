# DROP BRANCH

## Synopsis

```text
DROP BRANCH [ IF EXISTS ] branch_name
IN TABLE table_name
```

## Description

Drops an existing branch.

The optional `IF EXISTS` clause causes the error to be suppressed if the branch
does not exist.

## Examples

Drop the branch `audit` in the table `orders`:

```sql
DROP BRANCH audit IN TABLE orders
```

## See also

{doc}`create-branch`
