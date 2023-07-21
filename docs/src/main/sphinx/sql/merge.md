# MERGE

## Synopsis

```text
MERGE INTO target_table [ [ AS ]  target_alias ]
USING { source_table | query } [ [ AS ] source_alias ]
ON search_condition
when_clause [...]
```

where `when_clause` is one of

```text
WHEN MATCHED [ AND condition ]
    THEN DELETE
```

```text
WHEN MATCHED [ AND condition ]
    THEN UPDATE SET ( column = expression [, ...] )
```

```text
WHEN NOT MATCHED [ AND condition ]
    THEN INSERT [ column_list ] VALUES (expression, ...)
```

## Description

Conditionally update and/or delete rows of a table and/or insert new
rows into a table.

`MERGE` supports an arbitrary number of `WHEN` clauses with different
`MATCHED` conditions, executing the `DELETE`, `UPDATE` or `INSERT`
operation in the first `WHEN` clause selected by the `MATCHED`
state and the match condition.

For each source row, the `WHEN` clauses are processed in order.  Only
the first first matching `WHEN` clause is executed and subsequent clauses
are ignored.  A `MERGE_TARGET_ROW_MULTIPLE_MATCHES` exception is
raised when a single target table row matches more than one source row.

If a source row is not matched by any `WHEN` clause and there is no
`WHEN NOT MATCHED` clause, the source row is ignored.

In `WHEN` clauses with `UPDATE` operations, the column value expressions
can depend on any field of the target or the source.  In the `NOT MATCHED`
case, the `INSERT` expressions can depend on any field of the source.

## Examples

Delete all customers mentioned in the source table:

```
MERGE INTO accounts t USING monthly_accounts_update s
    ON t.customer = s.customer
    WHEN MATCHED
        THEN DELETE
```

For matching customer rows, increment the purchases, and if there is no
match, insert the row from the source table:

```
MERGE INTO accounts t USING monthly_accounts_update s
    ON (t.customer = s.customer)
    WHEN MATCHED
        THEN UPDATE SET purchases = s.purchases + t.purchases
    WHEN NOT MATCHED
        THEN INSERT (customer, purchases, address)
              VALUES(s.customer, s.purchases, s.address)
```

`MERGE` into the target table from the source table, deleting any matching
target row for which the source address is Centreville.  For all other
matching rows, add the source purchases and set the address to the source
address, if there is no match in the target table, insert the source
table row:

```
MERGE INTO accounts t USING monthly_accounts_update s
    ON (t.customer = s.customer)
    WHEN MATCHED AND s.address = 'Centreville'
        THEN DELETE
    WHEN MATCHED
        THEN UPDATE
            SET purchases = s.purchases + t.purchases, address = s.address
    WHEN NOT MATCHED
        THEN INSERT (customer, purchases, address)
              VALUES(s.customer, s.purchases, s.address)
```

## Limitations

Any connector can be used as a source table for a `MERGE` statement.
Only connectors which support the `MERGE` statement can be the target of a
merge operation. See the {doc}`connector documentation </connector>` for more
information.
