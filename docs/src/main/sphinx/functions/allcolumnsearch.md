# All-column search

Function for searching across multiple string columns simultaneously.

:::{function} allcolumnsearch(search_term) -> boolean
Searches for `search_term` as a substring across all VARCHAR and CHAR columns
in the current scope. Returns `true` if any string column contains the search
term (case-sensitive).

This function can only be used in WHERE clauses and is expanded at query
analysis time into explicit LIKE predicates for each string column.

**Requirements and behavior:**

- `search_term` must be a constant string literal (not a column or expression)
- Search is case-sensitive
- Performs substring matching equivalent to `column LIKE '%search_term%'`
- NULL values in columns are handled correctly (do not match)
- Only searches columns of type VARCHAR or CHAR; other types are ignored
- Can only be used in WHERE clauses, not in SELECT, ORDER BY, or other clauses

**Example - Basic search:**

```sql
SELECT * FROM (
    VALUES
        (1, 'error occurred', 'System message', 100),
        (2, 'Success', 'Operation completed', 200),
        (3, 'Warning', 'error in logs', 300)
) t(id, name, description, value)
WHERE allcolumnsearch('error');
```

```text
 id |      name       |   description    | value
----+-----------------+------------------+-------
  1 | error occurred  | System message   |   100
  3 | Warning         | error in logs    |   300
```

**Example - Combining with other predicates:**

```sql
SELECT * FROM users
WHERE allcolumnsearch('john') AND active = true;
```

**Example - With joins:**

When used with joins, `allcolumnsearch()` searches columns from all joined
tables in the current scope:

```sql
SELECT orders.id
FROM orders
JOIN customers ON orders.customer_id = customers.id
WHERE allcolumnsearch('urgent');
```

**Example - Case sensitivity:**

```sql
SELECT * FROM (
    VALUES
        (1, 'ERROR'),
        (2, 'error'),
        (3, 'Error')
) t(id, message)
WHERE allcolumnsearch('error');
```

```text
 id | message
----+---------
  2 | error
```

**Example - Searching with special characters:**

```sql
SELECT * FROM (
    VALUES
        (1, 'test@example.com'),
        (2, 'user@domain.com'),
        (3, 'noemail')
) t(id, email)
WHERE allcolumnsearch('@');
```

```text
 id |       email
----+-------------------
  1 | test@example.com
  2 | user@domain.com
```

:::{note}
This function is particularly useful for full-text search scenarios where you
want to search across multiple string columns without explicitly listing each
column name. The function is expanded during query planning, so it has no
runtime overhead compared to writing out the equivalent OR'd LIKE predicates
manually.
:::
:::