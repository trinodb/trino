# All-column search

Table function for searching across all string columns in a table.

:::{function} allcolumnsearch(input => TABLE(...), search_term => varchar) -> table
Searches for `search_term` as a pattern across all VARCHAR and CHAR columns
in the input table and returns only rows where at least one string column
matches the pattern.

This is a table function that filters rows at execution time by checking if
any string column contains the search pattern (case-insensitive regex matching).

**Parameters:**

- `input` - The input table to search (specified using TABLE() syntax)
- `search_term` - A string pattern to search for (supports regex patterns)

**Requirements and behavior:**

- Search is **case-insensitive** by default
- Supports full regex pattern matching
- Performs substring matching (pattern can match anywhere in the column)
- NULL values in columns are skipped (do not match)
- Only searches columns of type VARCHAR or CHAR; other types are ignored
- Returns complete rows where at least one string column matches
- Can be used with JOINs, WHERE clauses, subqueries, aggregations, etc.
- Works across all Trino connectors (no connector-specific code)

**Example - Basic search:**

```sql
SELECT * FROM TABLE(allcolumnsearch(
    input => TABLE(tpch.tiny.nation),
    search_term => 'UNITED'
));
```

```text
 nationkey |      name       | regionkey |                   comment
-----------+-----------------+-----------+--------------------------------------------
        24 | UNITED STATES   |         1 | y final packages. slow foxes cajole quickly
        23 | UNITED KINGDOM  |         3 | ously. final, express gifts cajole a
```

**Example - Case-insensitive matching:**

The search is case-insensitive, so 'united', 'UNITED', and 'United' all match:

```sql
SELECT name FROM TABLE(allcolumnsearch(
    input => TABLE(tpch.tiny.nation),
    search_term => 'united'
));
```

```text
      name
-----------------
 UNITED KINGDOM
 UNITED STATES
```

**Example - Combining with WHERE clause:**

```sql
SELECT * FROM TABLE(allcolumnsearch(
    input => TABLE(tpch.tiny.nation),
    search_term => 'A'
))
WHERE regionkey = 1;
```

**Example - With JOINs:**

```sql
SELECT n.name, r.name as region_name
FROM TABLE(allcolumnsearch(
    input => TABLE(tpch.tiny.nation),
    search_term => 'UNITED'
)) n
JOIN tpch.tiny.region r ON n.regionkey = r.regionkey;
```

```text
      name       | region_name
-----------------+-------------
 UNITED STATES   | AMERICA
 UNITED KINGDOM  | EUROPE
```

**Example - Regex patterns:**

Use regex patterns for more sophisticated searches:

```sql
-- Find nations starting with 'UNITED'
SELECT name FROM TABLE(allcolumnsearch(
    input => TABLE(tpch.tiny.nation),
    search_term => '^UNITED'
));
```

**Example - With aggregations:**

```sql
SELECT COUNT(*) as matching_rows
FROM TABLE(allcolumnsearch(
    input => TABLE(tpch.tiny.nation),
    search_term => 'UNITED'
));
```

```text
 matching_rows
---------------
             2
```

**Example - In subqueries:**

```sql
SELECT * FROM (
    SELECT * FROM TABLE(allcolumnsearch(
        input => TABLE(tpch.tiny.nation),
        search_term => 'A'
    ))
)
WHERE regionkey < 3
LIMIT 5;
```

**Example - Fully qualified name:**

```sql
SELECT * FROM TABLE(system.builtin.allcolumnsearch(
    input => TABLE(tpch.tiny.nation),
    search_term => 'UNITED'
));
```

**Error handling:**

The function validates inputs and provides clear error messages:

```sql
-- Empty search term
SELECT * FROM TABLE(allcolumnsearch(
    input => TABLE(tpch.tiny.nation),
    search_term => ''
));
-- Error: Search term cannot be empty

-- Invalid regex pattern
SELECT * FROM TABLE(allcolumnsearch(
    input => TABLE(tpch.tiny.nation),
    search_term => '[invalid'
));
-- Error: Invalid regex pattern: Unclosed character class

-- Table with no string columns
SELECT * FROM TABLE(allcolumnsearch(
    input => TABLE(SELECT 1 as id, 2 as value),
    search_term => 'test'
));
-- Error: No searchable string columns found in input table
```

:::{note}
This function is particularly useful for exploratory data analysis and ad-hoc
queries where you want to find rows containing specific text without knowing
which columns contain it. The function uses efficient page-level filtering
with compiled regex patterns for performance.

Unlike predicate expansion approaches, this is a true table function that
filters data during execution. While it doesn't support predicate pushdown to
connectors, it works universally across all Trino data sources.
:::
:::