# DECLARE

## Synopsis

```text
DECLARE identifier [, ...] type [ DEFAULT expression ]
```

## Description

Use the `DECLARE` statement directly after the [](/routines/begin) keyword in
[](/routines) to define one or more variables with an `identifier` as name. Each
statement must specify the [data type](/language/types) of the variable with
`type`. It can optionally include a default, initial value defined by an
`expression`. The default value is `NULL` if not specified.

## Examples

A simple declaration of the variable `x` with the `tinyint` data type and the
implicit default value of `null`:

```sql
DECLARE x tinyint;
```

A declaration of multiple string variables with length restricted to 25
characters:

```sql
DECLARE first_name, last_name, middle_name varchar(25);
```

A declaration of an exact decimal number with a default value:

```sql
DECLARE uptime_requirement decimal DEFAULT 99.999;
```

A declaration with a default value from an expression:

```sql
DECLARE start_time timestamp(3) with time zone DEFAULT now();
```

Further examples of varying complexity that cover usage of the `DECLARE`
statement in combination with other statements are available in the [SQL
routines examples documentation](/routines/examples).

## See also

* [](/routines/introduction)
* [](/language/types)
