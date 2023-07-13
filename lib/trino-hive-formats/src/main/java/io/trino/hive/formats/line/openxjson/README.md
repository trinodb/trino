# OpenX JSON format

This code in this package implements the OpenX JSON SerDe (`org.openx.data.jsonserde.JsonSerDe`)
with extensions. The behavior is based on the implementations maintained by
[Rcongiu](https://github.com/rcongiu/Hive-JSON-Serde) and [Starburst](https://github.com/starburstdata/hive-json-serde).
The Rcongiu implementation is based on very old Hive systems, has many quirks and bugs, and is not
actively maintained (as of this writing). The Starburst version is much more modern, and actively
maintained, but has it own quirks. This implementation generally a super set of both of these
implementations, including support for reading and writing more types, and generally attempts to
normalize the behavior of type mappers, and line processing. Except for some noted cases below,
this implementation will read JSON that the other implementations can read, and can read the JSON
written by the other implementations. Additionally, the JSON written by this implementation can
be read by the other implementations, assuming they support the data type at all.

# Line Format

Hive SerDe operate on a per-line basis, so multi line JSON can not be supported. Each line is
processed as follows:

1. If the first non-whitespace character of the line is not `{` or `]`, a row with all fields
   set to null is produced.
2. The line is parsed using the very lenient [open-json](https://github.com/tdunning/open-json)
   parser. A parser failure can be ignored by setting the `ignore.malformed.json` table property,
   and in this case a row with all fields set to null is produced.
3. Any text after the close of the JSON object or array is ignored.
4. The JSON is parsed using the Type Mapping rules below.

# Type Mapping

The JSON parser will only produce three Java types: `Map`, `List`, and `JsonString`. A `JsonString`
is normal quoted JSON string, or an unquoted JSON value. In valid JSON, unquoted strings are
limited to numbers, and boolean `true` or `false`, but the lenient parser used in OpenX JSON
SerDe allow for field names and value to be any unquoted string.

Specific type mapping rules are described below, but, general guidelines are:

* Parse failures result in `null`.
* Values that are outside the allowed type range result in `null`.
* JSON object or array values for numeric or temporal types, throw an exception.
* Text types support all JSON values by rendering the value as canonical JSON.

## BOOLEAN

`true` only if quoted or unquoted string is equals ignoring case to `true`. All other values,
including array and object, result in `false`.

### Known Differences

* Rcongiu SerDe fails on array or object value.

## BIGINT, INTEGER, SMALLINT, and TINYINT

Values that fail to parse properly, or that are outside the allowed numeric range, result in `null`.

### Formats

* Any value that can be parsed by `new BigDecimal(String)`
* Hex `0x1234`. Negative numbers are not supported.
* Octal `01234`. Negative numbers are not supported.

### Known Differences

* Rcongiu fails on values with decimals or exponents.
* Starburst performs a narrowing cast to expected size.
* Starburst and Rcongiu do not support octal in map keys or quoted octal values.
* Starburst and Rcongiu both have a bug that allows negative hex and octal values formatted like `0x-123` or `0-123`.

## DOUBLE and REAL

Values that fail to parse properly result in `null`.

### Formats

* Any value that can be parsed by `Double.parseDouble` or `Float.parseFloat`
* `NaN`, `Infinity`, `+Infinity`, and `-Infinity`  are supported

### Known Differences

Rcongiu maps keys to lower case when in case-insensitive mode (default), and that causes a failure when
processing the `NaN` and `Infinity` literals.

## DECIMAL

Values are parsed with `BigDecimal`, rounded (half up) to declared scale. Values that fail to parse properly,
or that exceeds the maximum precision, result in `null`.

### Formats

* Any value that can be parsed by `new BigDecimal(String)`

### Known Differences

* Rcongiu SerDe does not support `DECIMAL` type.
* Starburst fails if the value fails to parse.
* Starburst writes all `DECIMAL` values as `null` .

## VARCHAR and CHAR

All JSON values are supported by rendering canonical JSON to text. JSON will be rendered without any
additional whitespace. Unquoted values that can be parsed to a boolean or a number are canonicalized.
Unquoted field names are not canonicalized.

### Coercions

* **Boolean**: either `true` or `false`
* **Number**: the number parsed and formatted with BigDecimal
* **Array**:  value is rendered back to JSON
* **Object**: value is rendered back to JSON

### Known Differences

* Rcongiu fails on unquoted boolean and numbers in field names, but all other unquoted values are allowed.
* Starburst and fails for unquoted field names.
* Rcongiu and Starburst will use `Double` in some cases for canonicalization of numbers which results in
  a slightly different format from `BigDecimal`.
* Rcongiu and Starburst do not support conversions for `CHAR` or bounded `VARCHAR`.
* Rcongiu and Starburst write `CHAR` or bounded `VARCHAR` values as `null` regardless of the actual value.

## VARBINARY

Values are decoded with RFC 4648 Base64. Unquoted strings are not supported and result in `null`.

### Formats

* Base64 decode; `null` if the value is not a quoted string, or cannot be decoded.

### Known Differences

* Rcongiu SerDe does not support `VARBINARY` type.

## DATE

Values that fail to parse properly, or that are outside the allowed range, result in `null`.

### Formats

* `YYYY-MM-DD` with any trailing text after a space
* Decimal number of days since `1970-01-01`
* Hex `0x1234` days. Negative numbers are not supported.
* Octal `01234` days. Negative numbers are not supported.

### Known Differences

* Rcongiu SerDe parses dates using the broken `java.text.DateFormat` code which produces
  incorrect results for dates before 1582-10-15.
* Rcongiu does not support integer days since epoch format.
* Starburst fails for boolean
* Starburst and Rcongiu writes all `DATE` values as `null`

## TIMESTAMP

Values that fail to parse properly, or that are outside the allowed range, result in `null`.

### Text Formats

Then time zone is present the value is converted to the same instant in UTC, and then that local
time is used. The parser uses lenient mode, so values too large for a field are wrapped into the
next field

* `yyyy-MM-dd'T'HH:mm:ss.ffff'Z'` (default)
* `yyyy-MM-dd'T'HH:mm:ss.ffff+HHMM`
* `yyyy-MM-dd hh:mm:ss.ffff`

### Numeric Formats

Numeric formats are the number of seconds or milliseconds since 1970-01-01. Numbers are always
in base 10. Sign and leading zeros are supported. When counting digits sign and leading zeros
are included.

* decimal number: `seconds.millis`
* 13 or more digits: `millis`
* 12 or fewer digits: `seconds`

### Table Properties

* `timestamp.formats`: A comma separated list of `java.time.format.DateTimeFormatter` patterns.
  If the value does not match one of the patterns, the default patterns are used.

### Known Differences

* Rcongiu SerDe parses dates using the broken `java.text.DateFormat` code which produces
  incorrect results for dates before 1582-10-15.
* Rcongiu does not support the `timestamp.formats` property.
* Starburst does not use built in formats when timestamp formats are provided.
* Starburst allows unquoted octal values in numeric formats.
* Rcongiu and Starburst both fail when patterns do not match.
* Rcongiu and Starburst both allow values with exponents in numeric formats.
* Rcongiu and Starburst uses a more strict timestamp parser mode.

## ARRAY

### Coercions

* Empty string is coerced to `null`.
* Primitive or object value (without array wrapper) is coerced to a single element array.

### Known Differences

None

## MAP

Entries where the key evaluates to `null` are skipped. For maps with duplicate keys values
(after parsing), only last value parsed and used. For serialization, map keys must be a
primitive type, but for legacy reasons, there is very limited support for reading structural
map keys due to the implicit string coercions in ARRAY, MAP, and ROW types. It is not
recommended that structural keys are used as they can generally only result in empty maps,
or for ARRAY, a single element array key.

### Coercions

* String containing only whitespace is `null`
* All other types fail

### Known Differences

* Rcongiu SerDe map all keys to lower case during parsing, which affected all parsers.
* Rcongiu fails for duplicate map keys during parse, but duplicates are supported for keys
  that parse to the same value (e.g., doubles with trailing zeros).

## ROW

Keys that do not map to a row field are ignored. For objects with duplicate keys values,
only last value parsed and used.

### Coercions

* String containing only whitespace is coerced to a row with all fields set to `null`
* All other types fail

### Table Properties

* `case.insensitive` (default true): Keys are mapped to fields using a case-insensitive name match
  unless disabled. When case-insensitivity is disabled, fields containing uppercase names will
  never match as Hive field names are always lowercase.
* `dots.in.keys` (default false): If key does not have an exact match, find the first key that
  matches after replacing `.` with `_` in the key. This behavior is affected by the
  `case.insensitivity` property, but not the `mapping` properties.
* `mapping.{field-name}=key`: Matches the field to the specified key. This behavior is affected
  by the `case.insensitivity` property, but not the `dots.in.keys` property.
* `explicit.null` (default false): When enabled, a `null` field is rendered with an explicit JSON
  `null`, otherwise the field is not rendered at all.

### Coercions

* String containing only whitespace is `null`

### Known Differences

* Rcongiu SerDe map all keys to lower case during parsing, which affected all parsers.
* Rcongiu fails for duplicate map keys during parse, but duplicates are supported for keys that
  parse to the same value (e.g., doubles with trailing zeros).
* Rcongiu does not support `explicit.null` option.
