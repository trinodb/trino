# Conversion functions

Trino will implicitly convert numeric and character values to the
correct type if such a conversion is possible. Trino will not convert
between character and numeric types. For example, a query that expects
a varchar will not automatically convert a bigint value to an
equivalent varchar.

When necessary, values can be explicitly cast to a particular type.

## Conversion functions

:::{function} cast(value AS type) -> type
Explicitly cast a value as a type. This can be used to cast a
varchar to a numeric value type and vice versa.
:::

:::{function} try_cast(value AS type) -> type
Like {func}`cast`, but returns null if the cast fails.
:::

## Formatting

:::{function} format(format, args...) -> varchar
Returns a formatted string using the specified [format string](https://docs.oracle.com/en/java/javase/23/docs/api/java.base/java/util/Formatter.html#syntax)
and arguments:

```{try-sql}
SELECT format('%s%%', 123),
       format('%.5f', pi()),
       format('%03d', 8),
       format('%,.2f', 1234567.89),
       format('%-7s,%7s', 'hello', 'world'),
       format('%2$s %3$s %1$s', 'a', 'b', 'c'),
       format('%1$tA, %1$tB %1$te, %1$tY', date '2006-07-04')
```
:::

:::{function} format_number(number) -> varchar
Returns a formatted string using a unit symbol:

```{try-sql}
SELECT format_number(123456),
       format_number(1000000)
```
:::

## Data size

The `parse_data_size` function supports the following units:

:::{list-table}
:widths: 30, 40, 30
:header-rows: 1

* - Unit
  - Description
  - Value
* - ``B``
  - Bytes
  - 1
* - ``kB``
  - Kilobytes
  - 1024
* - ``MB``
  - Megabytes
  - 1024{sup}`2`
* - ``GB``
  - Gigabytes
  - 1024{sup}`3`
* - ``TB``
  - Terabytes
  - 1024{sup}`4`
* - ``PB``
  - Petabytes
  - 1024{sup}`5`
* - ``EB``
  - Exabytes
  - 1024{sup}`6`
* - ``ZB``
  - Zettabytes
  - 1024{sup}`7`
* - ``YB``
  - Yottabytes
  - 1024{sup}`8`
:::

:::{function} parse_data_size(string) -> decimal(38)
Parses `string` of format `value unit` into a number, where
`value` is the fractional number of `unit` values:

```{try-sql}
SELECT parse_data_size('1B'),
       parse_data_size('1kB'),
       parse_data_size('1MB'),
       parse_data_size('2.3MB')
```
:::

## Miscellaneous

:::{function} typeof(expr) -> varchar
Returns the name of the type of the provided expression:

```{try-sql}
SELECT typeof(123),
       typeof('cat'),
       typeof(cos(2) + 1.5)
```
:::
