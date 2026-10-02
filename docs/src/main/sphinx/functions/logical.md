(logical-operators)=
# Logical operators

## Logical operators

| Operator | Description                  | Example |
| -------- | ---------------------------- | ------- |
| `AND`    | True if both values are true | a AND b |
| `OR`     | True if either value is true | a OR b  |
| `NOT`    | True if the value is false   | NOT a   |

## Effect of NULL on logical operators

The result of an `AND` comparison may be `NULL` if one or both
sides of the expression are `NULL`. If at least one side of an
`AND` operator is `FALSE` the expression evaluates to `FALSE`:

```{try-sql}
SELECT CAST(null AS boolean) AND true,
       CAST(null AS boolean) AND false,
       CAST(null AS boolean) AND CAST(null AS boolean)
```

The result of an `OR` comparison may be `NULL` if one or both
sides of the expression are `NULL`.  If at least one side of an
`OR` operator is `TRUE` the expression evaluates to `TRUE`:

```{try-sql}
SELECT CAST(null AS boolean) OR CAST(null AS boolean),
       CAST(null AS boolean) OR false,
       CAST(null AS boolean) OR true
```

The following truth table demonstrates the handling of
`NULL` in `AND` and `OR`:

| a       | b       | a AND b | a OR b  |
| ------- | ------- | ------- | ------- |
| `TRUE`  | `TRUE`  | `TRUE`  | `TRUE`  |
| `TRUE`  | `FALSE` | `FALSE` | `TRUE`  |
| `TRUE`  | `NULL`  | `NULL`  | `TRUE`  |
| `FALSE` | `TRUE`  | `FALSE` | `TRUE`  |
| `FALSE` | `FALSE` | `FALSE` | `FALSE` |
| `FALSE` | `NULL`  | `FALSE` | `NULL`  |
| `NULL`  | `TRUE`  | `NULL`  | `TRUE`  |
| `NULL`  | `FALSE` | `FALSE` | `NULL`  |
| `NULL`  | `NULL`  | `NULL`  | `NULL`  |

The logical complement of `NULL` is `NULL` as shown in the following example:

```{try-sql}
SELECT NOT CAST(null AS boolean)
```

The following truth table demonstrates the handling of `NULL` in `NOT`:

| a       | NOT a   |
| ------- | ------- |
| `TRUE`  | `FALSE` |
| `FALSE` | `TRUE`  |
| `NULL`  | `NULL`  |
