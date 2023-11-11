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

`MERGE`changes data in the `target_table` based on the contents of the
`source_table`. The `search_condition` defines a condition, such as a relation
from identical columns, to associate the source and target data.

`MERGE` supports an arbitrary number of `WHEN` clauses. `MATCHED` conditions can
execute `DELETE` or `UPDATE` operations on the target data, while `NOT MATCHED`
conditions can add data from the source to the target table with `INSERT`.
Additional conditions can narrow down the affected rows.

For each source row, the `WHEN` clauses are processed in order. Only the first
matching `WHEN` clause is executed and subsequent clauses are ignored. The query
fails if a single target table row matches more than one source row.

In `WHEN` clauses with `UPDATE` operations, the column value expressions
can depend on any field of the target or the source. In the `NOT MATCHED`
case, the `INSERT` expressions can depend on any field of the source.

Typical usage of `MERGE` involves two tables with similar structure, containing
different data. For example, the source table is part of a transactional usage
in a production system, while the target table is located in a data warehouse
used for analytics. Periodically, `MERGE` operations are run to combine recent
production data with long-term data in the analytics warehouse. As long as you
can define a search condition between the two tables, you can also use very
different tables.

## Examples

Delete all customers mentioned in the source table:

```sql
MERGE INTO accounts t USING monthly_accounts_update s
    ON t.customer = s.customer
    WHEN MATCHED
        THEN DELETE
```

For matching customer rows, increment the purchases, and if there is no
match, insert the row from the source table:

```sql
MERGE INTO accounts t USING monthly_accounts_update s
    ON (t.customer = s.customer)
    WHEN MATCHED
        THEN UPDATE SET purchases = s.purchases + t.purchases
    WHEN NOT MATCHED
        THEN INSERT (customer, purchases, address)
              VALUES(s.customer, s.purchases, s.address)
```

`MERGE` into the target table from the source table, deleting any matching
target row for which the source address is `Centreville`. For all other matching
rows, add the source purchases and set the address to the source address. If
there is no match in the target table, insert the source table row:

```sql
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
