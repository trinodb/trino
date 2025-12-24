# VARIANT functions and operators

The `VARIANT` type represents a semi-structured value as defined by the
[Apache Iceberg Variant specification](https://iceberg.apache.org/spec/#variant).

`VARIANT` values are created using casts, decoded using casts, and dereferenced
using the SQL subscript operator (`[]`).

## Subscript operator

Elements of a `VARIANT` value can be accessed using the SQL subscript operator
(`[]`). The result of a subscript operation is always a `VARIANT` value.

### Objects

When the underlying value is an object, the subscript operator uses a string
key:

```sql
variant_expression['key']
```

If the specified key does not exist in the object, the result is SQL `NULL`.

### Arrays

When the underlying value is an array, the subscript operator uses one-based
indexing, following the SQL standard for arrays:

```sql
variant_expression[index]
```

The following rules apply:

* Indexes start at `1`
* Index `0` or negative indexes are invalid and result in an error
* An index greater than the array length results in an error

## Functions

:::{function} variant_is_null(variant) -> boolean
Returns `true` if the input value represents a *variant null*.

This function distinguishes a variant null value from SQL `NULL`.

* Returns `true` if the value is a variant null
* Returns `false` for all other variant values
* Returns SQL `NULL` if the input is SQL `NULL`

Example:

```sql
SELECT variant_is_null(CAST(JSON 'null' AS VARIANT)); -- true
SELECT variant_is_null(CAST(42 AS VARIANT));          -- false
SELECT variant_is_null(NULL);                         -- NULL
```

:::

## Cast to VARIANT

The following SQL types can be cast to `VARIANT`:

### Scalar types

* `BOOLEAN`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `BIGINT`
* `REAL`
* `DOUBLE`
* `DECIMAL`
* `VARCHAR`
* `VARBINARY`
* `DATE`
* `TIME(p)`
* `TIMESTAMP(p)`
* `TIMESTAMP(p) WITH TIME ZONE`
* `UUID`
* `JSON`
* `VARIANT`

### Container types

* `ARRAY`
* `MAP` (with `VARCHAR` key type)
* `ROW`

Container values may contain any supported scalar or container type, including
nested containers, `JSON`, and `VARIANT` values.

## Cast from VARIANT

A `VARIANT` value can be cast to the following SQL types when the underlying
value is compatible with the target type.

Standard Trino cast coercions apply. For example, a `VARIANT` value containing
a string can be cast to a numeric type if the string represents a valid value
for the target type and fits within its range.

### Scalar types

* `BOOLEAN`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `BIGINT`
* `REAL`
* `DOUBLE`
* `DECIMAL`
* `VARCHAR`
* `VARBINARY`
* `DATE`
* `TIME(p)`
* `TIMESTAMP(p)`
* `TIMESTAMP(p) WITH TIME ZONE`
* `UUID`
* `JSON`
* `VARIANT`

### Container types

* `ARRAY`
* `MAP` (with `VARCHAR` key type)
* `ROW`

Casting to container types is supported when the structure of the target type
is compatible with the contents of the `VARIANT` value. If the underlying value
is incompatible with the requested type, the cast fails.
