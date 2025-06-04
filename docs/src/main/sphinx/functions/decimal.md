# Decimal functions and operators

(decimal-literal)=
## Decimal literals

Use the `DECIMAL 'xxxxxxx.yyyyyyy'` syntax to define a decimal literal.

The precision of a decimal type for a literal will be equal to the number of digits
in the literal (including trailing and leading zeros). The scale will be equal
to the number of digits in the fractional part (including trailing zeros).

:::{list-table}
:widths: 50, 50
:header-rows: 1

* - Example literal
  - Data type
* - `DECIMAL '0'`
  - `DECIMAL(1)`
* - `DECIMAL '12345'`
  - `DECIMAL(5)`
* - `DECIMAL '0000012345.1234500000'`
  - `DECIMAL(20, 10)`
:::

## Binary arithmetic decimal operators

Standard mathematical operators are supported. The table below explains
precision and scale calculation rules for result.
Assuming `x` is of type `DECIMAL(xp, xs)` and `y` is of type `DECIMAL(yp, ys)`.

:::{list-table}
:widths: 30, 40, 30
:header-rows: 1

* - Operation
  - Result type precision
  - Result type scale
* - `x + y` and `x - y`
  -
    ```
    min(38,
        1 +
        max(xs, ys) +
        max(xp - xs, yp - ys)
    )
    ```
  - `max(xs, ys)`
* - `x * y`
  - ```
    min(38, xp + yp)
    ```
  - `xs + ys`
* - `x / y`
  -
    ```
    min(38,
        xp + ys-xs
        + max(0, ys-xs)
        )
    ```
  - `max(xs, ys)`
* - `x % y`
  - ```
    min(xp - xs, yp - ys) +
    max(xs, bs)
    ```
  - `max(xs, ys)`
:::

If the mathematical result of the operation is not exactly representable with
the precision and scale of the result data type,
then an exception condition is raised: `Value is out of range`.

When operating on decimal types with different scale and precision, the values are
first coerced to a common super type. For types near the largest representable precision (38),
this can result in Value is out of range errors when one of the operands doesn't fit
in the common super type. For example, the common super type of decimal(38, 0) and
decimal(38, 1) is decimal(38, 1), but certain values that fit in decimal(38, 0)
cannot be represented as a decimal(38, 1).

## Comparison operators

All standard {doc}`comparison` work for the decimal type.

## Unary decimal operators

The `-` operator performs negation. The type of result is same as type of argument.
