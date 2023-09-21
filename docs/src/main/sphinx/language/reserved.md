# Keywords and identifiers

(language-keywords)=
## Reserved keywords

The following table lists all of the keywords that are reserved in Trino,
along with their status in the SQL standard. These reserved keywords must
be quoted (using double quotes) in order to be used as an identifier.

| Keyword             | SQL:2016 | SQL-92   |
| ------------------- | -------- | -------- |
| `ALTER`             | reserved | reserved |
| `AND`               | reserved | reserved |
| `AS`                | reserved | reserved |
| `BETWEEN`           | reserved | reserved |
| `BY`                | reserved | reserved |
| `CASE`              | reserved | reserved |
| `CAST`              | reserved | reserved |
| `CONSTRAINT`        | reserved | reserved |
| `CREATE`            | reserved | reserved |
| `CROSS`             | reserved | reserved |
| `CUBE`              | reserved |          |
| `CURRENT_CATALOG`   | reserved |          |
| `CURRENT_DATE`      | reserved | reserved |
| `CURRENT_PATH`      | reserved |          |
| `CURRENT_ROLE`      | reserved | reserved |
| `CURRENT_SCHEMA`    | reserved |          |
| `CURRENT_TIME`      | reserved | reserved |
| `CURRENT_TIMESTAMP` | reserved | reserved |
| `CURRENT_USER`      | reserved |          |
| `DEALLOCATE`        | reserved | reserved |
| `DELETE`            | reserved | reserved |
| `DESCRIBE`          | reserved | reserved |
| `DISTINCT`          | reserved | reserved |
| `DROP`              | reserved | reserved |
| `ELSE`              | reserved | reserved |
| `END`               | reserved | reserved |
| `ESCAPE`            | reserved | reserved |
| `EXCEPT`            | reserved | reserved |
| `EXECUTE`           | reserved | reserved |
| `EXISTS`            | reserved | reserved |
| `EXTRACT`           | reserved | reserved |
| `FALSE`             | reserved | reserved |
| `FOR`               | reserved | reserved |
| `FROM`              | reserved | reserved |
| `FULL`              | reserved | reserved |
| `GROUP`             | reserved | reserved |
| `GROUPING`          | reserved |          |
| `HAVING`            | reserved | reserved |
| `IN`                | reserved | reserved |
| `INNER`             | reserved | reserved |
| `INSERT`            | reserved | reserved |
| `INTERSECT`         | reserved | reserved |
| `INTO`              | reserved | reserved |
| `IS`                | reserved | reserved |
| `JOIN`              | reserved | reserved |
| `JSON_ARRAY`        | reserved |          |
| `JSON_EXISTS`       | reserved |          |
| `JSON_OBJECT`       | reserved |          |
| `JSON_QUERY`        | reserved |          |
| `JSON_TABLE`        | reserved |          |
| `JSON_VALUE`        | reserved |          |
| `LEFT`              | reserved | reserved |
| `LIKE`              | reserved | reserved |
| `LISTAGG`           | reserved |          |
| `LOCALTIME`         | reserved |          |
| `LOCALTIMESTAMP`    | reserved |          |
| `NATURAL`           | reserved | reserved |
| `NORMALIZE`         | reserved |          |
| `NOT`               | reserved | reserved |
| `NULL`              | reserved | reserved |
| `ON`                | reserved | reserved |
| `OR`                | reserved | reserved |
| `ORDER`             | reserved | reserved |
| `OUTER`             | reserved | reserved |
| `PREPARE`           | reserved | reserved |
| `RECURSIVE`         | reserved |          |
| `RIGHT`             | reserved | reserved |
| `ROLLUP`            | reserved |          |
| `SELECT`            | reserved | reserved |
| `SKIP`              | reserved |          |
| `TABLE`             | reserved | reserved |
| `THEN`              | reserved | reserved |
| `TRIM`              | reserved | reserved |
| `TRUE`              | reserved | reserved |
| `UESCAPE`           | reserved |          |
| `UNION`             | reserved | reserved |
| `UNNEST`            | reserved |          |
| `USING`             | reserved | reserved |
| `VALUES`            | reserved | reserved |
| `WHEN`              | reserved | reserved |
| `WHERE`             | reserved | reserved |
| `WITH`              | reserved | reserved |

(language-identifiers)=
## Identifiers

Tokens that identify names of catalogs, schemas, tables, columns, functions, or
other objects, are identifiers.

Identifiers must start with a letter, and subsequently include alphanumeric
characters and underscores. Identifiers with other characters must be delimited
with double quotes (`"`). When delimited with double quotes, identifiers can use
any character. Escape a `"` with another preceding double quote in a delimited
identifier.

Identifiers are not treated as case sensitive.

Following are some valid examples:

```sql
tablename
SchemaName
example_catalog.a_schema."table$partitions"
"identifierWith""double""quotes"
```

The following identifiers are invalid in Trino and must be quoted when used:

```text
table-name
123SchemaName
colum$name@field
```

