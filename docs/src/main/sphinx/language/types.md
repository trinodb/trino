# Data types

Trino has a set of built-in data types, described below.
Additional types can be provided by plugins.

(type-mapping-overview)=

## Trino type support and mapping

Connectors to data sources are not required to support all Trino data types
described on this page. If there are data types similar to Trino's that are used
on the data source, the connector may map the Trino and remote data types to
each other as needed.

Depending on the connector and the data source, type mapping may apply
in either direction as follows:

- **Data source to Trino** mapping applies to any operation where columns in the
  data source are read by Trino, such as a {doc}`/sql/select` statement, and the
  underlying source data type needs to be represented by a Trino data type.
- **Trino to data source** mapping applies to any operation where the columns
  or expressions in Trino need to be translated into data types or expressions
  compatible with the underlying data source. For example,
  {doc}`/sql/create-table-as` statements specify Trino types that are then
  mapped to types on the remote data source. Predicates like `WHERE` also use
  these mappings in order to ensure that the predicate is translated to valid
  syntax on the remote data source.

Data type support and mappings vary depending on the connector. Refer to the
{doc}`connector documentation </connector>` for more information.

(boolean-data-types)=

## Boolean

### `BOOLEAN`

This type captures boolean values `true` and `false`.

(integer-data-types)=

## Integer

Integer numbers can be expressed as numeric literals in the following formats:

* Decimal integer. Examples are `-7`, `0`, or `3`.
* Hexadecimal integer composed of `0X` or `0x` and the value. Examples are
  `0x0A` for decimal `10` or `0x11` for decimal `17`.
* Octal integer composed of `0O` or `0o` and the value. Examples are `0o40` for
  decimal `32` or `0o11` for decimal `9`.
* Binary integer composed of `0B` or `0b` and the value. Examples are `0b1001`
  for decimal `9` or `0b101010` for decimal `42``.

Underscore characters are ignored within literal values, and can be used to
increase readability. For example, decimal integer `123_456.789_123` is
equivalent to `123456.789123`. Preceding and trailing underscores are not
permitted.

Integers are supported by the following data types.

### `TINYINT`

A 8-bit signed two's complement integer with a minimum value of
`-2^7` or `-0x80` and a maximum value of `2^7 - 1` or `0x7F`.

### `SMALLINT`

A 16-bit signed two's complement integer with a minimum value of
`-2^15` or `-0x8000` and a maximum value of `2^15 - 1` or `0x7FFF`.

### `INTEGER` or `INT`

A 32-bit signed two's complement integer with a minimum value of `-2^31` or
`-0x80000000` and a maximum value of `2^31 - 1` or `0x7FFFFFFF`.  The names
`INTEGER` and `INT` can both be used for this type.

### `BIGINT`

A 64-bit signed two's complement integer with a minimum value of `-2^63` or
`-0x8000000000000000` and a maximum value of `2^63 - 1` or `0x7FFFFFFFFFFFFFFF`.

(floating-point-data-types)=

## Floating-point

Floating-point, fixed-precision numbers can be expressed as numeric literal
using scientific notation such as `1.03e1` and are cast as `DOUBLE` data type.
Underscore characters are ignored within literal values, and can be used to
increase readability. For example, value `123_456.789e4` is equivalent to
`123456.789e4`. Preceding underscores, trailing underscores, and underscores
beside the comma (`.`) are not permitted.

### `REAL`

A real is a 32-bit inexact, variable-precision implementing the
IEEE Standard 754 for Binary Floating-Point Arithmetic.

Example literals: `REAL '10.3'`, `REAL '10.3e0'`, `REAL '1.03e1'`

### `DOUBLE`

A double is a 64-bit inexact, variable-precision implementing the
IEEE Standard 754 for Binary Floating-Point Arithmetic.

Example literals: `DOUBLE '10.3'`, `DOUBLE '1.03e1'`, `10.3e0`, `1.03e1`

(fixed-precision-data-types)=

## Fixed-precision

Fixed-precision numbers can be expressed as numeric literals such as `1.1`, and
are supported by the `DECIMAL` data type.

Underscore characters are ignored within literal values, and can be used to
increase readability. For example, decimal `123_456.789_123` is equivalent to
`123456.789123`. Preceding underscores, trailing underscores, and underscores
beside the comma (`.`) are not permitted.

Leading zeros in literal values are permitted and ignored. For example,
`000123.456` is equivalent to `123.456`.

### `DECIMAL`

A fixed-precision decimal number. Precision up to 38 digits is supported
but performance is best up to 18 digits.

The decimal type takes two literal parameters:

- **precision** - total number of digits
- **scale** - number of digits in fractional part. Scale is optional and defaults to 0.

Example type definitions: `DECIMAL(10,3)`, `DECIMAL(20)`

Example literals: `DECIMAL '10.3'`, `DECIMAL '1234567890'`, `1.1`

(string-data-types)=

## String

### `VARCHAR`

Variable length character data with an optional maximum length.

Example type definitions: `varchar`, `varchar(20)`

SQL statements support simple literal, as well as Unicode usage:

- literal string : `'Hello winter !'`
- Unicode string with default escape character: `U&'Hello winter \2603 !'`
- Unicode string with custom escape character: `U&'Hello winter #2603 !' UESCAPE '#'`

A Unicode string is prefixed with `U&` and requires an escape character
before any Unicode character usage with 4 digits. In the examples above
`\2603` and `#2603` represent a snowman character. Long Unicode codes
with 6 digits require usage of the plus symbol before the code. For example,
you need to use `\+01F600` for a grinning face emoji.

### `CHAR`

Fixed length character data. A `CHAR` type without length specified has a default length of 1.
A `CHAR(x)` value always has `x` characters. For example, casting `dog` to `CHAR(7)`
adds 4 implicit trailing spaces. Leading and trailing spaces are included in comparisons of
`CHAR` values. As a result, two character values with different lengths (`CHAR(x)` and
`CHAR(y)` where `x != y`) will never be equal.

Example type definitions: `char`, `char(20)`

### `VARBINARY`

Variable length binary data.

SQL statements support usage of binary literal data with the prefix `X` or `x`.
The binary data has to use hexadecimal format. For example, the binary form of
`eh?` is `X'65683F'` as you can confirm with the following statement:

```sql
SELECT from_utf8(x'65683F');
```

:::{note}
Binary strings with length are not yet supported: `varbinary(n)`
:::

### `JSON`

JSON value type, which can be a JSON object, a JSON array, a JSON number, a JSON string,
`true`, `false` or `null`.

(date-time-data-types)=

## Date and time

See also {doc}`/functions/datetime`

(date-data-type)=

### `DATE`

Calendar date (year, month, day).

Example: `DATE '2001-08-22'`

### `TIME`

`TIME` is an alias for `TIME(3)` (millisecond precision).

### `TIME(P)`

Time of day (hour, minute, second) without a time zone with `P` digits of precision
for the fraction of seconds. A precision of up to 12 (picoseconds) is supported.

Example: `TIME '01:02:03.456'`

### `TIME WITH TIME ZONE`

Time of day (hour, minute, second, millisecond) with a time zone.
Values of this type are rendered using the time zone from the value.
Time zones are expressed as the numeric UTC offset value:

```
SELECT TIME '01:02:03.456 -08:00';
-- 1:02:03.456-08:00
```

(timestamp-data-type)=

### `TIMESTAMP`

`TIMESTAMP` is an alias for `TIMESTAMP(3)` (millisecond precision).

### `TIMESTAMP(P)`

Calendar date and time of day without a time zone with `P` digits of precision
for the fraction of seconds. A precision of up to 12 (picoseconds) is supported.
This type is effectively a combination of the `DATE` and `TIME(P)` types.

`TIMESTAMP(P) WITHOUT TIME ZONE` is an equivalent name.

Timestamp values can be constructed with the `TIMESTAMP` literal
expression. Alternatively, language constructs such as
`localtimestamp(p)`, or a number of {doc}`date and time functions and
operators </functions/datetime>` can return timestamp values.

Casting to lower precision causes the value to be rounded, and not
truncated. Casting to higher precision appends zeros for the additional
digits.

The following examples illustrate the behavior:

```
SELECT TIMESTAMP '2020-06-10 15:55:23';
-- 2020-06-10 15:55:23

SELECT TIMESTAMP '2020-06-10 15:55:23.383345';
-- 2020-06-10 15:55:23.383345

SELECT typeof(TIMESTAMP '2020-06-10 15:55:23.383345');
-- timestamp(6)

SELECT cast(TIMESTAMP '2020-06-10 15:55:23.383345' as TIMESTAMP(1));
 -- 2020-06-10 15:55:23.4

SELECT cast(TIMESTAMP '2020-06-10 15:55:23.383345' as TIMESTAMP(12));
-- 2020-06-10 15:55:23.383345000000
```

(timestamp-with-time-zone-data-type)=

### `TIMESTAMP WITH TIME ZONE`

`TIMESTAMP WITH TIME ZONE` is an alias for `TIMESTAMP(3) WITH TIME ZONE`
(millisecond precision).

### `TIMESTAMP(P) WITH TIME ZONE`

Instant in time that includes the date and time of day with `P` digits of
precision for the fraction of seconds and with a time zone. Values of this
type are rendered using the time zone from the value.
Time zones can be expressed in the following ways:

- `UTC`, with `GMT`, `Z`, or `UT` usable as aliases for UTC.
- `+hh:mm` or `-hh:mm` with `hh:mm` as an hour and minute offset from UTC.
  Can be written with or without `UTC`, `GMT`, or `UT` as an alias for
  UTC.
- An [IANA time zone name](https://www.iana.org/time-zones).

The following examples demonstrate some of these syntax options:

```
SELECT TIMESTAMP '2001-08-22 03:04:05.321 UTC';
-- 2001-08-22 03:04:05.321 UTC

SELECT TIMESTAMP '2001-08-22 03:04:05.321 -08:30';
-- 2001-08-22 03:04:05.321 -08:30

SELECT TIMESTAMP '2001-08-22 03:04:05.321 GMT-08:30';
-- 2001-08-22 03:04:05.321 -08:30

SELECT TIMESTAMP '2001-08-22 03:04:05.321 America/New_York';
-- 2001-08-22 03:04:05.321 America/New_York
```

### `INTERVAL YEAR TO MONTH`

Span of years and months.

Example: `INTERVAL '3' MONTH`

### `INTERVAL DAY TO SECOND`

Span of days, hours, minutes, seconds and milliseconds.

Example: `INTERVAL '2' DAY`

(structural-data-types)=

## Structural

(array-type)=

### `ARRAY`

An array of the given component type.

Example: `ARRAY[1, 2, 3]`

(map-type)=

### `MAP`

A map between the given component types.

Example: `MAP(ARRAY['foo', 'bar'], ARRAY[1, 2])`

(row-type)=

### `ROW`

A structure made up of fields that allows mixed types.
The fields may be of any SQL type.

By default, row fields are not named, but names can be assigned.

Example: `CAST(ROW(1, 2e0) AS ROW(x BIGINT, y DOUBLE))`

Named row fields are accessed with field reference operator (`.`).

Example: `CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE)).x`

Named or unnamed row fields are accessed by position with the subscript
operator (`[]`). The position starts at `1` and must be a constant.

Example: `ROW(1, 2.0)[1]`

## Network address

(ipaddress-type)=

### `IPADDRESS`

An IP address that can represent either an IPv4 or IPv6 address. Internally,
the type is a pure IPv6 address. Support for IPv4 is handled using the
*IPv4-mapped IPv6 address* range ({rfc}`4291#section-2.5.5.2`).
When creating an `IPADDRESS`, IPv4 addresses will be mapped into that range.
When formatting an `IPADDRESS`, any address within the mapped range will
be formatted as an IPv4 address. Other addresses will be formatted as IPv6
using the canonical format defined in {rfc}`5952`.

Examples: `IPADDRESS '10.0.0.1'`, `IPADDRESS '2001:db8::1'`

## UUID

(uuid-type)=

### `UUID`

This type represents a UUID (Universally Unique IDentifier), also known as a
GUID (Globally Unique IDentifier), using the format defined in {rfc}`4122`.

Example: `UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'`

## HyperLogLog

Calculating the approximate distinct count can be done much more cheaply than an exact count using the
[HyperLogLog](https://wikipedia.org/wiki/HyperLogLog) data sketch. See {doc}`/functions/hyperloglog`.

(hyperloglog-type)=

### `HyperLogLog`

A HyperLogLog sketch allows efficient computation of {func}`approx_distinct`. It starts as a
sparse representation, switching to a dense representation when it becomes more efficient.

(p4hyperloglog-type)=

### `P4HyperLogLog`

A P4HyperLogLog sketch is similar to {ref}`hyperloglog-type`, but it starts (and remains)
in the dense representation.

## SetDigest

(setdigest-type)=

### `SetDigest`

A SetDigest (setdigest) is a data sketch structure used
in calculating [Jaccard similarity coefficient](https://wikipedia.org/wiki/Jaccard_index)
between two sets.

SetDigest encapsulates the following components:

- [HyperLogLog](https://wikipedia.org/wiki/HyperLogLog)
- [MinHash with a single hash function](http://wikipedia.org/wiki/MinHash#Variant_with_a_single_hash_function)

The HyperLogLog structure is used for the approximation of the distinct elements
in the original set.

The MinHash structure is used to store a low memory footprint signature of the original set.
The similarity of any two sets is estimated by comparing their signatures.

SetDigests are additive, meaning they can be merged together.

## Quantile digest

(qdigest-type)=

### `QDigest`

A quantile digest (qdigest) is a summary structure which captures the approximate
distribution of data for a given input set, and can be queried to retrieve approximate
quantile values from the distribution.  The level of accuracy for a qdigest
is tunable, allowing for more precise results at the expense of space.

A qdigest can be used to give approximate answer to queries asking for what value
belongs at a certain quantile.  A useful property of qdigests is that they are
additive, meaning they can be merged together without losing precision.

A qdigest may be helpful whenever the partial results of `approx_percentile`
can be reused.  For example, one may be interested in a daily reading of the 99th
percentile values that are read over the course of a week.  Instead of calculating
the past week of data with `approx_percentile`, `qdigest`s could be stored
daily, and quickly merged to retrieve the 99th percentile value.

## T-Digest

(tdigest-type)=

### `TDigest`

A T-digest (tdigest) is a summary structure which, similarly to qdigest, captures the
approximate distribution of data for a given input set. It can be queried to retrieve
approximate quantile values from the distribution.

TDigest has the following advantages compared to QDigest:

- higher performance
- lower memory usage
- higher accuracy at high and low percentiles

T-digests are additive, meaning they can be merged together.
