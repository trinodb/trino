# COMMENT

## Synopsis

```text
COMMENT ON ( TABLE | VIEW | MATERIALIZED VIEW | COLUMN ) name IS 'comments'
```

## Description

Set the comment for an object. The comment can be removed by setting the comment to `NULL`.

## Examples

Change the comment for the `users` table to be `master table`:

```
COMMENT ON TABLE users IS 'master table';
```

Change the comment for the `users` view to be `master view`:

```
COMMENT ON VIEW users IS 'master view';
```

Change the comment for the `users_mv` materialized view to be `master materialized view`:

```
COMMENT ON MATERIALIZED VIEW users_mv IS 'master materialized view';
```

Change the comment for the `users.name` column to be `full name`:

```
COMMENT ON COLUMN users.name IS 'full name';
```

## See also

[](/language/comments)
