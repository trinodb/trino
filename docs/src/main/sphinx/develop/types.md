# Types

The `Type` interface in Trino is used to implement a type in the SQL language.
Trino ships with a number of built-in types, like `VarcharType` and `BigintType`.
The `ParametricType` interface is used to provide type parameters for types, to
allow types like `VARCHAR(10)` or `DECIMAL(22, 5)`. A `Plugin` can provide
new `Type` objects by returning them from `getTypes()` and new `ParametricType`
objects by returning them from `getParametricTypes()`.

Below is a high level overview of the `Type` interface. For more details, see the
JavaDocs for `Type`.

## Native container type

All types define the `getJavaType()` method, frequently referred to as the
"native container type". This is the Java type used to hold values during execution
and to store them in a `Block`. For example, this is the type used in
the Java code that implements functions that produce or consume this `Type`.

### JSON connector values

The native Java type of SQL `JSON` is `io.trino.json.Json`, provided by the
`io.trino:trino-json` library. It is no longer `Slice`. Connectors using
`RecordCursor` must return JSON values from `getObject`, and native block writes
such as `TypeUtils.writeNativeValue` must receive a `Json` value. Returning text
from `getSlice` alone does not satisfy the native cursor contract.

Use `JsonItems.fromText` to parse UTF-8 JSON text. `Json.unchecked` wraps text that
the connector has already validated and defers structural parsing. The type's
`writeSlice` method also accepts JSON text; `getSlice` returns JSON text. These
text APIs do not expose the private typed encoding. Text cannot retain SQL
datetime tags or all native numeric metadata, so keep `Json` values intact when
passing them between native execution components.

## Native encoding

The interpretation of a value in its native container type form is defined by its
`Type`. For some types, such as `BigintType`, it matches the Java
interpretation of the native container type (64bit 2's complement). However, for other
types such as `TimestampWithTimeZoneType`, which also uses `long` for its
native container type, the value stored in the `long` is a 8byte binary value
combining the timezone and the milliseconds since the unix epoch. In particular,
this means that you cannot compare two native values and expect a meaningful
result, without knowing the native encoding.

## Type descriptor

A type's descriptor (`TypeDescriptor`) defines its identity, and also encodes some
general information about the type, such as its type parameters (if it's parametric)
and its literal parameters. The literal parameters are used in types like
`VARCHAR(10)`. A descriptor is always *ground*: it denotes one concrete type, such as
`varchar(10)` or `array(bigint)`.

## Type template

Where a `TypeDescriptor` denotes one concrete type, a `TypeTemplate` denotes a
*family* of types parameterized by variables — it is the open counterpart of the
ground descriptor. Function signatures (`Signature`) carry their argument and return
types as templates: `array(E)` has a type variable `E`, and `decimal(p, s)` has
numeric variables `p` and `s`.

Binding a template's variables against a call site — see `SignatureBinder` —
substitutes the bound types and evaluates any calculated numeric expressions (for
example the `x + y` in `char(x + y)`), producing a ground `TypeDescriptor`. The
reverse, lifting a variable-free `TypeDescriptor` into a `TypeTemplate`, is also
supported. A `Signature` therefore declares its type and numeric variables once and
expresses every argument and return position as a template over them.

## Type id

Use `TypeSyntax.toSql` or `Type.getDisplayName()` for SQL spelling.
`TypeDescriptor.toString()` and `TypeTemplate.render()` use an internal
structural representation, which can differ from SQL syntax. `Type.getTypeId()`
continues to use SQL spelling.

The internal base names of zoned datetime and interval types are
`$timestamp_tz`, `$time_tz`, `$interval_day_time`, and `$interval_year_month`.
They must not be exposed as SQL type names. The corresponding `StandardTypes`
and zoned datetime `NAME` constants changed values. Java inlines these constants,
so plugins compiled against their previous values must be rebuilt.

A `TypeId` is the opaque identifier under which a type is persisted, for example in
the catalog properties of a materialized view. It wraps the rendered form of the
type's descriptor but guarantees nothing about its structure: where a `TypeDescriptor`
is structural identity that can be inspected and compared piecewise, a `TypeId` is
only ever compared for equality and resolved back to a type through `TypeManager`.
