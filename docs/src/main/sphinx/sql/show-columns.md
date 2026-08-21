# SHOW COLUMNS

## Synopsis

```text
SHOW COLUMNS FROM table [ LIKE pattern ]
```

## Description

List the columns in a `table` along with their data type and other attributes:

```{try-sql}
SHOW COLUMNS FROM tpch.tiny.nation
```

{ref}`Specify a pattern <like-operator>` in the optional `LIKE` clause to
filter the results to the desired subset. For example, the following query
allows you to find columns ending in `key`:

```{try-sql}
SHOW COLUMNS FROM tpch.tiny.nation LIKE '%key'
```
