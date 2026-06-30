# COMMENT

## Synopsis

```text
COMMENT ON ( TABLE | VIEW | COLUMN ) name IS 'comments'
```

## Description

Set the comment for an object. The comment can be removed by setting the comment to `NULL`.

## Examples

Change the comment for the `users` table to be `master table`:

```{try-sql}
CREATE TABLE memory.default.users (id integer, name varchar);
---
COMMENT ON TABLE memory.default.users IS 'master table'
```

Change the comment for the `users` view to be `master view`:

```{try-sql}
CREATE VIEW memory.default.users AS SELECT 1 AS id, 'Alice' AS name;
---
COMMENT ON VIEW memory.default.users IS 'master view'
```

Change the comment for the `users.name` column to be `full name`:

```{try-sql}
CREATE TABLE memory.default.users (id integer, name varchar);
---
COMMENT ON COLUMN memory.default.users.name IS 'full name'
```

## See also

[](/language/comments)
