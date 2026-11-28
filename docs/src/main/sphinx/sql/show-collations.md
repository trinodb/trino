# SHOW COLLATIONS

## Synopsis

```text
SHOW COLLATIONS [ LIKE pattern [ ESCAPE 'escape_character' ] ]
```

## Description

List the collations that string types and `COLLATE` can name, with their pad attribute.
Every collation name is `NO PAD`; its `PAD SPACE` variant is written with the pad
attribute, as in `unicode_ci PAD SPACE`.

{ref}`Specify a pattern <like-operator>` in the optional `LIKE` clause to
filter the results to the desired subset. For example, the following query
lists the Unicode collations:

```
SHOW COLLATIONS LIKE 'unicode%'
```

The same list is available as the `information_schema.collations` table, with the
columns `collation_name` and `pad_attribute`. The `collation_name` column of
`information_schema.columns` shows the collation of each collated column, and is
null for columns with the default collation.

## See also

{ref}`varchar-collations`
