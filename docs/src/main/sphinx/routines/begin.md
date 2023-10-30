# BEGIN

## Synopsis

```text
BEGIN
  [ DECLARE ... ]
  statements
END
```

## Description

Marks the start and end of a block in a [SQL routine](/routines/introduction).
`BEGIN` can be used wherever a statement can be used to group multiple
statements together and to declare variables local to the block. A typical use
case is as first statement within a [](/routines/function). Blocks can also be
nested.

After the `BEGIN` keyword, you can add variable declarations using
[/routines/declare] statements, followed by one or more statements that define
the main body of the routine, separated by `;`. The following statements can be
used:

* [](/routines/case)
* [](/routines/if)
* [](/routines/iterate)
* [](/routines/leave)
* [](/routines/loop)
* [](/routines/repeat)
* [](/routines/return)
* [](/routines/set)
* [](/routines/while)
* Nested [](/routines/begin) blocks

## Examples

The following example computes the value `42`:

```sql
FUNCTION meaning_of_life()
  RETURNS tinyint
  BEGIN
    DECLARE a tinyint DEFAULT 6;
    DECLARE b tinyint DEFAULT 7;
    RETURN a * b;
  END
```

Further examples of varying complexity that cover usage of the `BEGIN` statement
in combination with other statements are available in the [SQL routines examples
documentation](/routines/examples).

## See also

* [](/routines/introduction)
* [](/routines/function)
