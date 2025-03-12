# JSON functions and operators

The SQL standard describes functions and operators to process JSON data. They
allow you to access JSON data according to its structure, generate JSON data,
and store it persistently in SQL tables.

Importantly, the SQL standard imposes that there is no dedicated data type to
represent JSON data in SQL. Instead, JSON data is represented as character or
binary strings. Although Trino supports `JSON` type, it is not used or
produced by the following functions.

Trino supports three functions for querying JSON data:
{ref}`json_exists<json-exists>`,
{ref}`json_query<json-query>`, and {ref}`json_value<json-value>`. Each of them
is based on the same mechanism of exploring and processing JSON input using
JSON path.

Trino also supports two functions for generating JSON data --
{ref}`json_array<json-array>`, and {ref}`json_object<json-object>`.

(json-path-language)=
## JSON path language

The JSON path language is a special language, used exclusively by certain SQL
operators to specify the query to perform on the JSON input. Although JSON path
expressions are embedded in SQL queries, their syntax significantly differs
from SQL. The semantics of predicates, operators, etc. in JSON path expressions
generally follow the semantics of SQL. The JSON path language is case-sensitive
for keywords and identifiers.

(json-path-syntax-and-semantics)=
### JSON path syntax and semantics

JSON path expressions are recursive structures. Although the name "path"
suggests a linear sequence of operations going step by step deeper into the JSON
structure, a JSON path expression is in fact a tree. It can access the input
JSON item multiple times, in multiple ways, and combine the results. Moreover,
the result of a JSON path expression is not a single item, but an ordered
sequence of items. Each of the sub-expressions takes one or more input
sequences, and returns a sequence as the result.

:::{note}
In the lax mode, most path operations first unnest all JSON arrays in the
input sequence. Any divergence from this rule is mentioned in the following
listing. Path modes are explained in {ref}`json-path-modes`.
:::

The JSON path language features are divided into: literals, variables,
arithmetic binary expressions, arithmetic unary expressions, and a group of
operators collectively known as accessors.

#### literals

- numeric literals

  They include exact and approximate numbers, and are interpreted as if they
  were SQL values.

```text
-1, 1.2e3, NaN
```

- string literals

  They are enclosed in double quotes.

```text
"Some text"
```

- boolean literals

```text
true, false
```

- null literal

  It has the semantics of the JSON null, not of SQL null. See {ref}`json-comparison-rules`.

```text
null
```

#### variables

- context variable

  It refers to the currently processed input of the JSON
  function.

```text
$
```

- named variable

  It refers to a named parameter by its name.

```text
$param
```

- current item variable

  It is used inside the filter expression to refer to the currently processed
  item from the input sequence.

```text
@
```

- last subscript variable

  It refers to the last index of the innermost enclosing array. Array indexes
  in JSON path expressions are zero-based.

```text
last
```

#### arithmetic binary expressions

The JSON path language supports five arithmetic binary operators:

```text
<path1> + <path2>
<path1> - <path2>
<path1> * <path2>
<path1> / <path2>
<path1> % <path2>
```

Both operands, `<path1>` and `<path2>`, are evaluated to sequences of
items. For arithmetic binary operators, each input sequence must contain a
single numeric item. The arithmetic operation is performed according to SQL
semantics, and it returns a sequence containing a single element with the
result.

The operators follow the same precedence rules as in SQL arithmetic operations,
and parentheses can be used for grouping.

#### arithmetic unary expressions

```text
+ <path>
- <path>
```

The operand `<path>` is evaluated to a sequence of items. Every item must be
a numeric value. The unary plus or minus is applied to every item in the
sequence, following SQL semantics, and the results form the returned sequence.

#### member accessor

The member accessor returns the value of the member with the specified key for
each JSON object in the input sequence.

```text
<path>.key
<path>."key"
```

The condition when a JSON object does not have such a member is called a
structural error. In the lax mode, it is suppressed, and the faulty object is
excluded from the result.

Let `<path>` return a sequence of three JSON objects:

```text
{"customer" : 100, "region" : "AFRICA"},
{"region" : "ASIA"},
{"customer" : 300, "region" : "AFRICA", "comment" : null}
```

the expression `<path>.customer` succeeds in the first and the third object,
but the second object lacks the required member. In strict mode, path
evaluation fails. In lax mode, the second object is silently skipped, and the
resulting sequence is `100, 300`.

All items in the input sequence must be JSON objects.

:::{note}
Trino does not support JSON objects with duplicate keys.
:::

#### wildcard member accessor

Returns values from all key-value pairs for each JSON object in the input
sequence. All the partial results are concatenated into the returned sequence.

```text
<path>.*
```

Let `<path>` return a sequence of three JSON objects:

```text
{"customer" : 100, "region" : "AFRICA"},
{"region" : "ASIA"},
{"customer" : 300, "region" : "AFRICA", "comment" : null}
```

The results is:

```text
100, "AFRICA", "ASIA", 300, "AFRICA", null
```

All items in the input sequence must be JSON objects.

The order of values returned from a single JSON object is arbitrary. The
sub-sequences from all JSON objects are concatenated in the same order in which
the JSON objects appear in the input sequence.

(json-descendant-member-accessor)=
#### descendant member accessor

Returns the values associated with the specified key in all JSON objects on all
levels of nesting in the input sequence.

```text
<path>..key
<path>.."key"
```

The order of returned values is that of preorder depth first search. First, the
enclosing object is visited, and then all child nodes are visited.

This method does not perform array unwrapping in the lax mode. The results
are the same in the lax and strict modes. The method traverses into JSON
arrays and JSON objects. Non-structural JSON items are skipped.

Let `<path>` be a sequence containing a JSON object:

```text
{
    "id" : 1,
    "notes" : [{"type" : 1, "comment" : "foo"}, {"type" : 2, "comment" : null}],
    "comment" : ["bar", "baz"]
}
```

```text
<path>..comment --> ["bar", "baz"], "foo", null
```

#### array accessor

Returns the elements at the specified indexes for each JSON array in the input
sequence. Indexes are zero-based.

```text
<path>[ <subscripts> ]
```

The `<subscripts>` list contains one or more subscripts. Each subscript
specifies a single index or a range (ends inclusive):

```text
<path>[<path1>, <path2> to <path3>, <path4>,...]
```

In lax mode, any non-array items resulting from the evaluation of the input
sequence are wrapped into single-element arrays. Note that this is an exception
to the rule of automatic array wrapping.

Each array in the input sequence is processed in the following way:

- The variable `last` is set to the last index of the array.
- All subscript indexes are computed in order of declaration. For a
  singleton subscript `<path1>`, the result must be a singleton numeric item.
  For a range subscript `<path2> to <path3>`, two numeric items are expected.
- The specified array elements are added in order to the output sequence.

Let `<path>` return a sequence of three JSON arrays:

```text
[0, 1, 2], ["a", "b", "c", "d"], [null, null]
```

The following expression returns a sequence containing the last element from
every array:

```text
<path>[last] --> 2, "d", null
```

The following expression returns the third and fourth element from every array:

```text
<path>[2 to 3] --> 2, "c", "d"
```

Note that the first array does not have the fourth element, and the last array
does not have the third or fourth element. Accessing non-existent elements is a
structural error. In strict mode, it causes the path expression to fail. In lax
mode, such errors are suppressed, and only the existing elements are returned.

Another example of a structural error is an improper range specification such
as `5 to 3`.

Note that the subscripts may overlap, and they do not need to follow the
element order. The order in the returned sequence follows the subscripts:

```text
<path>[1, 0, 0] --> 1, 0, 0, "b", "a", "a", null, null, null
```

#### wildcard array accessor

Returns all elements of each JSON array in the input sequence.

```text
<path>[*]
```

In lax mode, any non-array items resulting from the evaluation of the input
sequence are wrapped into single-element arrays. Note that this is an exception
to the rule of automatic array wrapping.

The output order follows the order of the original JSON arrays. Also, the order
of elements within the arrays is preserved.

Let `<path>` return a sequence of three JSON arrays:

```text
[0, 1, 2], ["a", "b", "c", "d"], [null, null]
<path>[*] --> 0, 1, 2, "a", "b", "c", "d", null, null
```

#### filter

Retrieves the items from the input sequence which satisfy the predicate.

```text
<path>?( <predicate> )
```

JSON path predicates are syntactically similar to boolean expressions in SQL.
However, the semantics are different in many aspects:

- They operate on sequences of items.
- They have their own error handling (they never fail).
- They behave different depending on the lax or strict mode.

The predicate evaluates to `true`, `false`, or `unknown`. Note that some
predicate expressions involve nested JSON path expression. When evaluating the
nested path, the variable `@` refers to the currently examined item from the
input sequence.

The following predicate expressions are supported:

- Conjunction

```text
<predicate1> && <predicate2>
```

- Disjunction

```text
<predicate1> || <predicate2>
```

- Negation

```text
! <predicate>
```

- `exists` predicate

```text
exists( <path> )
```

Returns `true` if the nested path evaluates to a non-empty sequence, and
`false` when the nested path evaluates to an empty sequence. If the path
evaluation throws an error, returns `unknown`.

- `starts with` predicate

```text
<path> starts with "Some text"
<path> starts with $variable
```

The nested `<path>` must evaluate to a sequence of textual items, and the
other operand must evaluate to a single textual item. If evaluating of either
operand throws an error, the result is `unknown`. All items from the sequence
are checked for starting with the right operand. The result is `true` if a
match is found, otherwise `false`. However, if any of the comparisons throws
an error, the result in the strict mode is `unknown`. The result in the lax
mode depends on whether the match or the error was found first.

- `is unknown` predicate

```text
( <predicate> ) is unknown
```

Returns `true` if the nested predicate evaluates to `unknown`, and
`false` otherwise.

- Comparisons

```text
<path1> == <path2>
<path1> <> <path2>
<path1> != <path2>
<path1> < <path2>
<path1> > <path2>
<path1> <= <path2>
<path1> >= <path2>
```

Both operands of a comparison evaluate to sequences of items. If either
evaluation throws an error, the result is `unknown`. Items from the left and
right sequence are then compared pairwise. Similarly to the `starts with`
predicate, the result is `true` if any of the comparisons returns `true`,
otherwise `false`. However, if any of the comparisons throws an error, for
example because the compared types are not compatible, the result in the strict
mode is `unknown`. The result in the lax mode depends on whether the `true`
comparison or the error was found first.

(json-comparison-rules)=
##### Comparison rules

Null values in the context of comparison behave different than SQL null:

- null == null --> `true`
- null != null, null \< null, ... --> `false`
- null compared to a scalar value --> `false`
- null compared to a JSON array or a JSON object --> `false`

When comparing two scalar values, `true` or `false` is returned if the
comparison is successfully performed. The semantics of the comparison is the
same as in SQL. In case of an error, e.g. comparing text and number,
`unknown` is returned.

Comparing a scalar value with a JSON array or a JSON object, and comparing JSON
arrays/objects is an error, so `unknown` is returned.

##### Examples of filter

Let `<path>` return a sequence of three JSON objects:

```text
{"customer" : 100, "region" : "AFRICA"},
{"region" : "ASIA"},
{"customer" : 300, "region" : "AFRICA", "comment" : null}
```

```text
<path>?(@.region != "ASIA") --> {"customer" : 100, "region" : "AFRICA"},
                                {"customer" : 300, "region" : "AFRICA", "comment" : null}
<path>?(!exists(@.customer)) --> {"region" : "ASIA"}
```

The following accessors are collectively referred to as **item methods**.

#### double()

Converts numeric or text values into double values.

```text
<path>.double()
```

Let `<path>` return a sequence `-1, 23e4, "5.6"`:

```text
<path>.double() --> -1e0, 23e4, 5.6e0
```

#### ceiling(), floor(), and abs()

Gets the ceiling, the floor or the absolute value for every numeric item in the
sequence. The semantics of the operations is the same as in SQL.

Let `<path>` return a sequence `-1.5, -1, 1.3`:

```text
<path>.ceiling() --> -1.0, -1, 2.0
<path>.floor() --> -2.0, -1, 1.0
<path>.abs() --> 1.5, 1, 1.3
```

#### keyvalue()

Returns a collection of JSON objects including one object per every member of
the original object for every JSON object in the sequence.

```text
<path>.keyvalue()
```

The returned objects have three members:

- "name", which is the original key,
- "value", which is the original bound value,
- "id", which is the unique number, specific to an input object.

Let `<path>` be a sequence of three JSON objects:

```text
{"customer" : 100, "region" : "AFRICA"},
{"region" : "ASIA"},
{"customer" : 300, "region" : "AFRICA", "comment" : null}
```

```text
<path>.keyvalue() --> {"name" : "customer", "value" : 100, "id" : 0},
                      {"name" : "region", "value" : "AFRICA", "id" : 0},
                      {"name" : "region", "value" : "ASIA", "id" : 1},
                      {"name" : "customer", "value" : 300, "id" : 2},
                      {"name" : "region", "value" : "AFRICA", "id" : 2},
                      {"name" : "comment", "value" : null, "id" : 2}
```

It is required that all items in the input sequence are JSON objects.

The order of the returned values follows the order of the original JSON
objects. However, within objects, the order of returned entries is arbitrary.

#### type()

Returns a textual value containing the type name for every item in the
sequence.

```text
<path>.type()
```

This method does not perform array unwrapping in the lax mode.

The returned values are:

- `"null"` for JSON null,
- `"number"` for a numeric item,
- `"string"` for a textual item,
- `"boolean"` for a boolean item,
- `"date"` for an item of type date,
- `"time without time zone"` for an item of type time,
- `"time with time zone"` for an item of type time with time zone,
- `"timestamp without time zone"` for an item of type timestamp,
- `"timestamp with time zone"` for an item of type timestamp with time zone,
- `"array"` for JSON array,
- `"object"` for JSON object,

#### size()

Returns a numeric value containing the size for every JSON array in the
sequence.

```text
<path>.size()
```

This method does not perform array unwrapping in the lax mode. Instead, all
non-array items are wrapped in singleton JSON arrays, so their size is `1`.

It is required that all items in the input sequence are JSON arrays.

Let `<path>` return a sequence of three JSON arrays:

```text
[0, 1, 2], ["a", "b", "c", "d"], [null, null]
<path>.size() --> 3, 4, 2
```

### Limitations

The SQL standard describes the `datetime()` JSON path item method and the
`like_regex()` JSON path predicate. Trino does not support them.

(json-path-modes)=
### JSON path modes

The JSON path expression can be evaluated in two modes: strict and lax. In the
strict mode, it is required that the input JSON data strictly fits the schema
required by the path expression. In the lax mode, the input JSON data can
diverge from the expected schema.

The following table shows the differences between the two modes.

:::{list-table}
:widths: 40 20 40
:header-rows: 1

* - Condition
  - strict mode
  - lax mode
* - Performing an operation which requires a non-array on an array, e.g.:

    `$.key` requires a JSON object

    `$.floor()` requires a numeric value
  - ERROR
  - The array is automatically unnested, and the operation is performed on
    each array element.
* - Performing an operation which requires an array on an non-array, e.g.:

    `$[0]`, `$[*]`, `$.size()`
  - ERROR
  - The non-array item is automatically wrapped in a singleton array, and
    the operation is performed on the array.
* - A structural error: accessing a non-existent element of an array or a
    non-existent member of a JSON object, e.g.:

    `$[-1]` (array index out of bounds)

    `$.key`, where the input JSON object does not have a member `key`
  - ERROR
  - The error is suppressed, and the operation results in an empty sequence.
:::

#### Examples of the lax mode behavior

Let `<path>` return a sequence of three items, a JSON array, a JSON object,
and a scalar numeric value:

```text
[1, "a", null], {"key1" : 1.0, "key2" : true}, -2e3
```

The following example shows the wildcard array accessor in the lax mode. The
JSON array returns all its elements, while the JSON object and the number are
wrapped in singleton arrays and then unnested, so effectively they appear
unchanged in the output sequence:

```text
<path>[*] --> 1, "a", null, {"key1" : 1.0, "key2" : true}, -2e3
```

When calling the `size()` method, the JSON object and the number are also
wrapped in singleton arrays:

```text
<path>.size() --> 3, 1, 1
```

In some cases, the lax mode cannot prevent failure. In the following example,
even though the JSON array is unwrapped prior to calling the `floor()`
method, the item `"a"` causes type mismatch.

```text
<path>.floor() --> ERROR
```

(json-exists)=
## json_exists

The `json_exists` function determines whether a JSON value satisfies a JSON
path specification.

```text
JSON_EXISTS(
    json_input [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ],
    json_path
    [ PASSING json_argument [, ...] ]
    [ { TRUE | FALSE | UNKNOWN | ERROR } ON ERROR ]
    )
```

The `json_path` is evaluated using the `json_input` as the context variable
(`$`), and the passed arguments as the named variables (`$variable_name`).
The returned value is `true` if the path returns a non-empty sequence, and
`false` if the path returns an empty sequence. If an error occurs, the
returned value depends on the `ON ERROR` clause. The default value returned
`ON ERROR` is `FALSE`. The `ON ERROR` clause is applied for the following
kinds of errors:

- Input conversion errors, such as malformed JSON
- JSON path evaluation errors, e.g. division by zero

`json_input` is a character string or a binary string. It should contain
a single JSON item. For a binary string, you can specify encoding.

`json_path` is a string literal, containing the path mode specification, and
the path expression, following the syntax rules described in
{ref}`json-path-syntax-and-semantics`.

```text
'strict ($.price + $.tax)?(@ > 99.9)'
'lax $[0 to 1].floor()?(@ > 10)'
```

In the `PASSING` clause you can pass arbitrary expressions to be used by the
path expression.

```text
PASSING orders.totalprice AS O_PRICE,
        orders.tax % 10 AS O_TAX
```

The passed parameters can be referenced in the path expression by named
variables, prefixed with `$`.

```text
'lax $?(@.price > $O_PRICE || @.tax > $O_TAX)'
```

Additionally to SQL values, you can pass JSON values, specifying the format and
optional encoding:

```text
PASSING orders.json_desc FORMAT JSON AS o_desc,
        orders.binary_record FORMAT JSON ENCODING UTF16 AS o_rec
```

Note that the JSON path language is case-sensitive, while the unquoted SQL
identifiers are upper-cased. Therefore, it is recommended to use quoted
identifiers in the `PASSING` clause:

```text
'lax $.$KeyName' PASSING nation.name AS KeyName --> ERROR; no passed value found
'lax $.$KeyName' PASSING nation.name AS "KeyName" --> correct
```

### Examples

Let `customers` be a table containing two columns: `id:bigint`,
`description:varchar`.

| id  | description                                           |
| --- | ----------------------------------------------------- |
| 101 | '{"comment" : "nice", "children" : \[10, 13, 16\]}'   |
| 102 | '{"comment" : "problematic", "children" : \[8, 11\]}' |
| 103 | '{"comment" : "knows best", "children" : \[2\]}'      |

The following query checks which customers have children above the age of 10:

```text
SELECT
      id,
      json_exists(
                  description,
                  'lax $.children[*]?(@ > 10)'
                 ) AS children_above_ten
FROM customers
```

| id  | children_above_ten |
| --- | ------------------ |
| 101 | true               |
| 102 | true               |
| 103 | false              |

In the following query, the path mode is strict. We check the third child for
each customer. This should cause a structural error for the customers who do
not have three or more children. This error is handled according to the `ON
ERROR` clause.

```text
SELECT
      id,
      json_exists(
                  description,
                  'strict $.children[2]?(@ > 10)'
                  UNKNOWN ON ERROR
                 ) AS child_3_above_ten
FROM customers
```

| id  | child_3_above_ten |
| --- | ----------------- |
| 101 | true              |
| 102 | NULL              |
| 103 | NULL              |

(json-query)=
## json_query

The `json_query` function extracts a JSON value from a JSON value.

```text
JSON_QUERY(
    json_input [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ],
    json_path
    [ PASSING json_argument [, ...] ]
    [ RETURNING type [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ] ]
    [ WITHOUT [ ARRAY ] WRAPPER |
      WITH [ { CONDITIONAL | UNCONDITIONAL } ] [ ARRAY ] WRAPPER ]
    [ { KEEP | OMIT } QUOTES [ ON SCALAR STRING ] ]
    [ { ERROR | NULL | EMPTY ARRAY | EMPTY OBJECT } ON EMPTY ]
    [ { ERROR | NULL | EMPTY ARRAY | EMPTY OBJECT } ON ERROR ]
    )
```

The constant string `json_path` is evaluated using the `json_input` as the
context variable (`$`), and the passed arguments as the named variables
(`$variable_name`).

The returned value is a JSON item returned by the path. By default, it is
represented as a character string (`varchar`). In the `RETURNING` clause,
you can specify other character string type or `varbinary`. With
`varbinary`, you can also specify the desired encoding.

`json_input` is a character string or a binary string. It should contain
a single JSON item. For a binary string, you can specify encoding.

`json_path` is a string literal, containing the path mode specification, and
the path expression, following the syntax rules described in
{ref}`json-path-syntax-and-semantics`.

```text
'strict $.keyvalue()?(@.name == $cust_id)'
'lax $[5 to last]'
```

In the `PASSING` clause you can pass arbitrary expressions to be used by the
path expression.

```text
PASSING orders.custkey AS CUST_ID
```

The passed parameters can be referenced in the path expression by named
variables, prefixed with `$`.

```text
'strict $.keyvalue()?(@.value == $CUST_ID)'
```

Additionally to SQL values, you can pass JSON values, specifying the format and
optional encoding:

```text
PASSING orders.json_desc FORMAT JSON AS o_desc,
        orders.binary_record FORMAT JSON ENCODING UTF16 AS o_rec
```

Note that the JSON path language is case-sensitive, while the unquoted SQL
identifiers are upper-cased. Therefore, it is recommended to use quoted
identifiers in the `PASSING` clause:

```text
'lax $.$KeyName' PASSING nation.name AS KeyName --> ERROR; no passed value found
'lax $.$KeyName' PASSING nation.name AS "KeyName" --> correct
```

The `ARRAY WRAPPER` clause lets you modify the output by wrapping the results
in a JSON array. `WITHOUT ARRAY WRAPPER` is the default option. `WITH
CONDITIONAL ARRAY WRAPPER` wraps every result which is not a singleton JSON
array or JSON object. `WITH UNCONDITIONAL ARRAY WRAPPER` wraps every result.

The `QUOTES` clause lets you modify the result for a scalar string by
removing the double quotes being part of the JSON string representation.

### Examples

Let `customers` be a table containing two columns: `id:bigint`,
`description:varchar`.

| id  | description                                           |
| --- | ----------------------------------------------------- |
| 101 | '{"comment" : "nice", "children" : \[10, 13, 16\]}'   |
| 102 | '{"comment" : "problematic", "children" : \[8, 11\]}' |
| 103 | '{"comment" : "knows best", "children" : \[2\]}'      |

The following query gets the `children` array for each customer:

```text
SELECT
      id,
      json_query(
                 description,
                 'lax $.children'
                ) AS children
FROM customers
```

| id  | children       |
| --- | -------------- |
| 101 | '\[10,13,16\]' |
| 102 | '\[8,11\]'     |
| 103 | '\[2\]'        |

The following query gets the collection of children for each customer.
Note that the `json_query` function can only output a single JSON item. If
you don't use array wrapper, you get an error for every customer with multiple
children. The error is handled according to the `ON ERROR` clause.

```text
SELECT
      id,
      json_query(
                 description,
                 'lax $.children[*]'
                 WITHOUT ARRAY WRAPPER
                 NULL ON ERROR
                ) AS children
FROM customers
```

| id  | children |
| --- | -------- |
| 101 | NULL     |
| 102 | NULL     |
| 103 | '2'      |

The following query gets the last child for each customer, wrapped in a JSON
array:

```text
SELECT
      id,
      json_query(
                 description,
                 'lax $.children[last]'
                 WITH ARRAY WRAPPER
                ) AS last_child
FROM customers
```

| id  | last_child |
| --- | ---------- |
| 101 | '\[16\]'   |
| 102 | '\[11\]'   |
| 103 | '\[2\]'    |

The following query gets all children above the age of 12 for each customer,
wrapped in a JSON array. The second and the third customer don't have children
of this age. Such case is handled according to the `ON EMPTY` clause. The
default value returned `ON EMPTY` is `NULL`. In the following example,
`EMPTY ARRAY ON EMPTY` is specified.

```text
SELECT
      id,
      json_query(
                 description,
                 'strict $.children[*]?(@ > 12)'
                 WITH ARRAY WRAPPER
                 EMPTY ARRAY ON EMPTY
                ) AS children
FROM customers
```

| id  | children    |
| --- | ----------- |
| 101 | '\[13,16\]' |
| 102 | '\[\]'      |
| 103 | '\[\]'      |

The following query shows the result of the `QUOTES` clause. Note that `KEEP
QUOTES` is the default.

```text
SELECT
      id,
      json_query(description, 'strict $.comment' KEEP QUOTES) AS quoted_comment,
      json_query(description, 'strict $.comment' OMIT QUOTES) AS unquoted_comment
FROM customers
```

| id  | quoted_comment  | unquoted_comment |
| --- | --------------- | ---------------- |
| 101 | '"nice"'        | 'nice'           |
| 102 | '"problematic"' | 'problematic'    |
| 103 | '"knows best"'  | 'knows best'     |

If an error occurs, the returned value depends on the `ON ERROR` clause. The
default value returned `ON ERROR` is `NULL`. One example of error is
multiple items returned by the path. Other errors caught and handled according
to the `ON ERROR` clause are:

- Input conversion errors, such as malformed JSON
- JSON path evaluation errors, e.g. division by zero
- Output conversion errors

(json-value)=
## json_value

The `json_value` function extracts a scalar SQL value from a JSON value.

```text
JSON_VALUE(
    json_input [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ],
    json_path
    [ PASSING json_argument [, ...] ]
    [ RETURNING type ]
    [ { ERROR | NULL | DEFAULT expression } ON EMPTY ]
    [ { ERROR | NULL | DEFAULT expression } ON ERROR ]
    )
```

The `json_path` is evaluated using the `json_input` as the context variable
(`$`), and the passed arguments as the named variables (`$variable_name`).

The returned value is the SQL scalar returned by the path. By default, it is
converted to string (`varchar`). In the `RETURNING` clause, you can specify
other desired type: a character string type, numeric, boolean or datetime type.

`json_input` is a character string or a binary string. It should contain
a single JSON item. For a binary string, you can specify encoding.

`json_path` is a string literal, containing the path mode specification, and
the path expression, following the syntax rules described in
{ref}`json-path-syntax-and-semantics`.

```text
'strict $.price + $tax'
'lax $[last].abs().floor()'
```

In the `PASSING` clause you can pass arbitrary expressions to be used by the
path expression.

```text
PASSING orders.tax AS O_TAX
```

The passed parameters can be referenced in the path expression by named
variables, prefixed with `$`.

```text
'strict $[last].price + $O_TAX'
```

Additionally to SQL values, you can pass JSON values, specifying the format and
optional encoding:

```text
PASSING orders.json_desc FORMAT JSON AS o_desc,
        orders.binary_record FORMAT JSON ENCODING UTF16 AS o_rec
```

Note that the JSON path language is case-sensitive, while the unquoted SQL
identifiers are upper-cased. Therefore, it is recommended to use quoted
identifiers in the `PASSING` clause:

```text
'lax $.$KeyName' PASSING nation.name AS KeyName --> ERROR; no passed value found
'lax $.$KeyName' PASSING nation.name AS "KeyName" --> correct
```

If the path returns an empty sequence, the `ON EMPTY` clause is applied. The
default value returned `ON EMPTY` is `NULL`. You can also specify the
default value:

```text
DEFAULT -1 ON EMPTY
```

If an error occurs, the returned value depends on the `ON ERROR` clause. The
default value returned `ON ERROR` is `NULL`. One example of error is
multiple items returned by the path. Other errors caught and handled according
to the `ON ERROR` clause are:

- Input conversion errors, such as malformed JSON
- JSON path evaluation errors, e.g. division by zero
- Returned scalar not convertible to the desired type

### Examples

Let `customers` be a table containing two columns: `id:bigint`,
`description:varchar`.

| id  | description                                           |
| --- | ----------------------------------------------------- |
| 101 | '{"comment" : "nice", "children" : \[10, 13, 16\]}'   |
| 102 | '{"comment" : "problematic", "children" : \[8, 11\]}' |
| 103 | '{"comment" : "knows best", "children" : \[2\]}'      |

The following query gets the `comment` for each customer as `char(12)`:

```text
SELECT id, json_value(
                      description,
                      'lax $.comment'
                      RETURNING char(12)
                     ) AS comment
FROM customers
```

| id  | comment        |
| --- | -------------- |
| 101 | 'nice        ' |
| 102 | 'problematic ' |
| 103 | 'knows best  ' |

The following query gets the first child's age for each customer as
`tinyint`:

```text
SELECT id, json_value(
                      description,
                      'lax $.children[0]'
                      RETURNING tinyint
                     ) AS child
FROM customers
```

| id  | child |
| --- | ----- |
| 101 | 10    |
| 102 | 8     |
| 103 | 2     |

The following query gets the third child's age for each customer. In the strict
mode, this should cause a structural error for the customers who do not have
the third child. This error is handled according to the `ON ERROR` clause.

```text
SELECT id, json_value(
                      description,
                      'strict $.children[2]'
                      DEFAULT 'err' ON ERROR
                     ) AS child
FROM customers
```

| id  | child |
| --- | ----- |
| 101 | '16'  |
| 102 | 'err' |
| 103 | 'err' |

After changing the mode to lax, the structural error is suppressed, and the
customers without a third child produce empty sequence. This case is handled
according to the `ON EMPTY` clause.

```text
SELECT id, json_value(
                      description,
                      'lax $.children[2]'
                      DEFAULT 'missing' ON EMPTY
                     ) AS child
FROM customers
```

| id  | child     |
| --- | --------- |
| 101 | '16'      |
| 102 | 'missing' |
| 103 | 'missing' |


(json-table)=
## json_table

The `json_table` clause extracts a table from a JSON value. Use this clause to
transform JSON data into a relational format, making it easier to query and
analyze. Use `json_table` in the `FROM` clause of a
[`SELECT`](select-json-table) statement to create a table from JSON data.

```text
JSON_TABLE(
    json_input,
    json_path [ AS path_name ]
    [ PASSING value AS parameter_name [, ...] ]
    COLUMNS (
        column_definition [, ...] )
    [ PLAN ( json_table_specific_plan )
      | PLAN DEFAULT ( json_table_default_plan ) ]
    [ { ERROR | EMPTY } ON ERROR ]
)
```

The `COLUMNS` clause supports the following `column_definition` arguments:

```text
column_name FOR ORDINALITY
| column_name type
    [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ]
    [ PATH json_path ]
    [ { WITHOUT | WITH { CONDITIONAL | UNCONDITIONAL } } [ ARRAY ] WRAPPER ]
    [ { KEEP | OMIT } QUOTES [ ON SCALAR STRING ] ]
    [ { ERROR | NULL | EMPTY { [ARRAY] | OBJECT } | DEFAULT expression } ON EMPTY ]
    [ { ERROR | NULL | DEFAULT expression } ON ERROR ]
| NESTED [ PATH ] json_path [ AS path_name ] COLUMNS ( column_definition [, ...] )
```

`json_input` is a character string or a binary string. It must contain a single
JSON item.

`json_path` is a string literal containing the path mode specification and the
path expression. It follows the syntax rules described in
[](json-path-syntax-and-semantics).

```text
'strict ($.price + $.tax)?(@ > 99.9)'
'lax $[0 to 1].floor()?(@ > 10)'
```

In the `PASSING` clause, pass values as named parameters that the `json_path`
expression can reference.

```text
PASSING orders.totalprice AS o_price,
        orders.tax % 10 AS o_tax
```

Use named parameters to reference the values in the path expression. Prefix
named parameters with `$`.

```text
'lax $?(@.price > $o_price || @.tax > $o_tax)'
```

You can also pass JSON values in the `PASSING` clause. Use `FORMAT JSON` to
specify the format and `ENCODING` to specify the encoding:

```text
PASSING orders.json_desc FORMAT JSON AS o_desc,
        orders.binary_record FORMAT JSON ENCODING UTF16 AS o_rec
```

The `json_path` value is case-sensitive. The SQL identifiers are uppercase. Use
quoted identifiers in the `PASSING` clause:

```text
'lax $.$KeyName' PASSING nation.name AS KeyName --> ERROR; no passed value found
'lax $.$KeyName' PASSING nation.name AS "KeyName" --> correct
```

The `PLAN` clause specifies how to join columns from different paths. Use
`OUTER` or `INNER` to define how to join parent paths with their child paths.
Use `CROSS` or `UNION` to join siblings.

`COLUMNS` defines the schema of your table. Each `column_definition` specifies
how to extract and format your `json_input` value into a relational column.

`PLAN` is an optional clause to control how to process and join nested JSON
data.

`ON ERROR` specifies how to handle processing errors. `ERROR ON ERROR` throws an
error. `EMPTY ON ERROR` returns an empty result set.

`column_name` specifies a column name.

`FOR ORDINALITY` adds a row number column to the output table, starting at `1`.
Specify the column name in the column definition:

```text
row_num FOR ORDINALITY
```

`NESTED PATH` extracts data from nested levels of a `json_input` value. Each
`NESTED PATH` clause can contain `column_definition` values.

The `json_table` function returns a result set that you can use like any other
table in your queries. You can join the result set with other tables or
combine multiple arrays from your JSON data.

You can also process nested JSON objects without parsing the data multiple
times.

Use `json_table` as a lateral join to process JSON data from another table.

### Examples

The following query uses `json_table` to extract values from a JSON array and
return them as rows in a table with three columns:

```sql
SELECT
      *
FROM
      json_table(
                '[
                  {"id":1,"name":"Africa","wikiDataId":"Q15"},
                  {"id":2,"name":"Americas","wikiDataId":"Q828"},
                  {"id":3,"name":"Asia","wikiDataId":"Q48"},
                  {"id":4,"name":"Europe","wikiDataId":"Q51"}
                ]',
                'strict $' COLUMNS (
                  NESTED PATH 'strict $[*]' COLUMNS (
                    id integer PATH 'strict $.id',
                    name varchar PATH 'strict $.name',
                    wiki_data_id varchar PATH 'strict $."wikiDataId"'
                  )
                )
              );
```

| id | child     | wiki_data_id  |
| -- | --------- | ------------- |
|  1 | Africa    | Q1            |
|  2 | Americas  | Q828          |
|  3 | Asia      | Q48           |
|  4 | Europe    | Q51           |

The following query uses `json_table` to extract values from an array of nested
JSON objects. It flattens the nested JSON data into a single table. The example
query processes an array of continent names, where each continent contains an
array of countries and their populations.

The `NESTED PATH 'lax $[*]'` clause iterates through the continent objects,
while the `NESTED PATH 'lax $.countries[*]'` iterates through each country
within each continent. This creates a flat table structure with four rows
combining each continent with each of its countries. Continent values repeat for
each of their countries.

```sql
SELECT
      *
FROM
      json_table(
                '[
                    {"continent": "Asia", "countries": [
                        {"name": "Japan", "population": 125.7},
                        {"name": "Thailand", "population": 71.6}
                    ]},
                    {"continent": "Europe", "countries": [
                        {"name": "France", "population": 67.4},
                        {"name": "Germany", "population": 83.2}
                    ]}
                ]',
                'lax $' COLUMNS (
                    NESTED PATH 'lax $[*]' COLUMNS (
                        continent varchar PATH 'lax $.continent',
                        NESTED PATH 'lax $.countries[*]' COLUMNS (
                            country varchar PATH 'lax $.name',
                            population double PATH 'lax $.population'
                        )
                    )
                ));
```

| continent  | country   | population    |
| ---------- | --------- | ------------- |
| Asia       | Japan     | 125.7         |
| Asia       | Thailand  | 71.6          |
| Europe     | France    | 67.4          |
| Europe     | Germany   | 83.2          |

The following query uses `PLAN` to specify an `OUTER` join between a parent path
and a child path:

```sql
SELECT
      *
FROM
      JSON_TABLE(
                '[]',
                'lax $' AS "root_path"
                COLUMNS(
                    a varchar(1) PATH 'lax "A"',
                    NESTED PATH 'lax $[*]' AS "nested_path"
                            COLUMNS (b varchar(1) PATH 'lax "B"'))
                PLAN ("root_path" OUTER "nested_path")
                );
```

| a    | b    |
| ---- | ---- |
| A    | null |

The following query uses `PLAN` to specify an `INNER` join between a parent path
and a child path:

```sql
SELECT
      *
FROM
      JSON_TABLE(
                '[]',
                'lax $' AS "root_path"
                COLUMNS(
                    a varchar(1) PATH 'lax "A"',
                    NESTED PATH 'lax $[*]' AS "nested_path"
                            COLUMNS (b varchar(1) PATH 'lax "B"'))
                PLAN ("root_path" INNER "nested_path")
                );
```

| a    | b    |
| ---- | ---- |
| null | null |

(json-array)=
## json_array

The `json_array` function creates a JSON array containing given elements.

```text
JSON_ARRAY(
    [ array_element [, ...]
      [ { NULL ON NULL | ABSENT ON NULL } ] ],
    [ RETURNING type [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ] ]
    )
```

### Argument types

The array elements can be arbitrary expressions. Each passed value is converted
into a JSON item according to its type, and optional `FORMAT` and
`ENCODING` specification.

You can pass SQL values of types boolean, numeric, and character string. They
are converted to corresponding JSON literals:

```
SELECT json_array(true, 12e-1, 'text')
--> '[true,1.2,"text"]'
```

Additionally to SQL values, you can pass JSON values. They are character or
binary strings with a specified format and optional encoding:

```
SELECT json_array(
                  '[  "text"  ] ' FORMAT JSON,
                  X'5B0035005D00' FORMAT JSON ENCODING UTF16
                 )
--> '[["text"],[5]]'
```

You can also nest other JSON-returning functions. In that case, the `FORMAT`
option is implicit:

```
SELECT json_array(
                  json_query('{"key" : [  "value"  ]}', 'lax $.key')
                 )
--> '[["value"]]'
```

Other passed values are cast to varchar, and they become JSON text literals:

```
SELECT json_array(
                  DATE '2001-01-31',
                  UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'
                 )
--> '["2001-01-31","12151fd2-7586-11e9-8f9e-2a86e4085a59"]'
```

You can omit the arguments altogether to get an empty array:

```
SELECT json_array() --> '[]'
```

### Null handling

If a value passed for an array element is `null`, it is treated according to
the specified null treatment option. If `ABSENT ON NULL` is specified, the
null element is omitted in the result. If `NULL ON NULL` is specified, JSON
`null` is added to the result. `ABSENT ON NULL` is the default
configuration:

```
SELECT json_array(true, null, 1)
--> '[true,1]'

SELECT json_array(true, null, 1 ABSENT ON NULL)
--> '[true,1]'

SELECT json_array(true, null, 1 NULL ON NULL)
--> '[true,null,1]'
```

### Returned type

The SQL standard imposes that there is no dedicated data type to represent JSON
data in SQL. Instead, JSON data is represented as character or binary strings.
By default, the `json_array` function returns varchar containing the textual
representation of the JSON array. With the `RETURNING` clause, you can
specify other character string type:

```
SELECT json_array(true, 1 RETURNING VARCHAR(100))
--> '[true,1]'
```

You can also specify to use varbinary and the required encoding as return type.
The default encoding is UTF8:

```
SELECT json_array(true, 1 RETURNING VARBINARY)
--> X'5b 74 72 75 65 2c 31 5d'

SELECT json_array(true, 1 RETURNING VARBINARY FORMAT JSON ENCODING UTF8)
--> X'5b 74 72 75 65 2c 31 5d'

SELECT json_array(true, 1 RETURNING VARBINARY FORMAT JSON ENCODING UTF16)
--> X'5b 00 74 00 72 00 75 00 65 00 2c 00 31 00 5d 00'

SELECT json_array(true, 1 RETURNING VARBINARY FORMAT JSON ENCODING UTF32)
--> X'5b 00 00 00 74 00 00 00 72 00 00 00 75 00 00 00 65 00 00 00 2c 00 00 00 31 00 00 00 5d 00 00 00'
```

(json-object)=
## json_object

The `json_object` function creates a JSON object containing given key-value pairs.

```text
JSON_OBJECT(
    [ key_value [, ...]
      [ { NULL ON NULL | ABSENT ON NULL } ] ],
      [ { WITH UNIQUE [ KEYS ] | WITHOUT UNIQUE [ KEYS ] } ]
    [ RETURNING type [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ] ]
    )
```

### Argument passing conventions

There are two conventions for passing keys and values:

```
SELECT json_object('key1' : 1, 'key2' : true)
--> '{"key1":1,"key2":true}'

SELECT json_object(KEY 'key1' VALUE 1, KEY 'key2' VALUE true)
--> '{"key1":1,"key2":true}'
```

In the second convention, you can omit the `KEY` keyword:

```
SELECT json_object('key1' VALUE 1, 'key2' VALUE true)
--> '{"key1":1,"key2":true}'
```

### Argument types

The keys can be arbitrary expressions. They must be of character string type.
Each key is converted into a JSON text item, and it becomes a key in the
created JSON object. Keys must not be null.

The values can be arbitrary expressions. Each passed value is converted
into a JSON item according to its type, and optional `FORMAT` and
`ENCODING` specification.

You can pass SQL values of types boolean, numeric, and character string. They
are converted to corresponding JSON literals:

```
SELECT json_object('x' : true, 'y' : 12e-1, 'z' : 'text')
--> '{"x":true,"y":1.2,"z":"text"}'
```

Additionally to SQL values, you can pass JSON values. They are character or
binary strings with a specified format and optional encoding:

```
SELECT json_object(
                   'x' : '[  "text"  ] ' FORMAT JSON,
                   'y' : X'5B0035005D00' FORMAT JSON ENCODING UTF16
                  )
--> '{"x":["text"],"y":[5]}'
```

You can also nest other JSON-returning functions. In that case, the `FORMAT`
option is implicit:

```
SELECT json_object(
                   'x' : json_query('{"key" : [  "value"  ]}', 'lax $.key')
                  )
--> '{"x":["value"]}'
```

Other passed values are cast to varchar, and they become JSON text literals:

```
SELECT json_object(
                   'x' : DATE '2001-01-31',
                   'y' : UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'
                  )
--> '{"x":"2001-01-31","y":"12151fd2-7586-11e9-8f9e-2a86e4085a59"}'
```

You can omit the arguments altogether to get an empty object:

```
SELECT json_object() --> '{}'
```

### Null handling

The values passed for JSON object keys must not be null. It is allowed to pass
`null` for JSON object values. A null value is treated according to the
specified null treatment option. If `NULL ON NULL` is specified, a JSON
object entry with `null` value is added to the result. If `ABSENT ON NULL`
is specified, the entry is omitted in the result. `NULL ON NULL` is the
default configuration.:

```
SELECT json_object('x' : null, 'y' : 1)
--> '{"x":null,"y":1}'

SELECT json_object('x' : null, 'y' : 1 NULL ON NULL)
--> '{"x":null,"y":1}'

SELECT json_object('x' : null, 'y' : 1 ABSENT ON NULL)
--> '{"y":1}'
```

### Key uniqueness

If a duplicate key is encountered, it is handled according to the specified key
uniqueness constraint.

If `WITH UNIQUE KEYS` is specified, a duplicate key results in a query
failure:

```
SELECT json_object('x' : null, 'x' : 1 WITH UNIQUE KEYS)
--> failure: "duplicate key passed to JSON_OBJECT function"
```

Note that this option is not supported if any of the arguments has a
`FORMAT` specification.

If `WITHOUT UNIQUE KEYS` is specified, duplicate keys are not supported due
to implementation limitation. `WITHOUT UNIQUE KEYS` is the default
configuration.

### Returned type

The SQL standard imposes that there is no dedicated data type to represent JSON
data in SQL. Instead, JSON data is represented as character or binary strings.
By default, the `json_object` function returns varchar containing the textual
representation of the JSON object. With the `RETURNING` clause, you can
specify other character string type:

```
SELECT json_object('x' : 1 RETURNING VARCHAR(100))
--> '{"x":1}'
```

You can also specify to use varbinary and the required encoding as return type.
The default encoding is UTF8:

```
SELECT json_object('x' : 1 RETURNING VARBINARY)
--> X'7b 22 78 22 3a 31 7d'

SELECT json_object('x' : 1 RETURNING VARBINARY FORMAT JSON ENCODING UTF8)
--> X'7b 22 78 22 3a 31 7d'

SELECT json_object('x' : 1 RETURNING VARBINARY FORMAT JSON ENCODING UTF16)
--> X'7b 00 22 00 78 00 22 00 3a 00 31 00 7d 00'

SELECT json_object('x' : 1 RETURNING VARBINARY FORMAT JSON ENCODING UTF32)
--> X'7b 00 00 00 22 00 00 00 78 00 00 00 22 00 00 00 3a 00 00 00 31 00 00 00 7d 00 00 00'
```

:::{warning}
The following functions and operators are not compliant with the SQL
standard, and should be considered deprecated. According to the SQL
standard, there shall be no `JSON` data type. Instead, JSON values
should be represented as string values. The remaining functionality of the
following functions is covered by the functions described previously.
:::

## Cast to JSON

The following types can be cast to JSON:

- `BOOLEAN`
- `TINYINT`
- `SMALLINT`
- `INTEGER`
- `BIGINT`
- `REAL`
- `DOUBLE`
- `VARCHAR`

Additionally, `ARRAY`, `MAP`, and `ROW` types can be cast to JSON when
the following requirements are met:

- `ARRAY` types can be cast when the element type of the array is one
  of the supported types.
- `MAP` types can be cast when the key type of the map is `VARCHAR` and
  the value type of the map is a supported type,
- `ROW` types can be cast when every field type of the row is a supported
  type.

:::{note}
Cast operations with supported {ref}`character string types
<string-data-types>` treat the input as a string, not validated as JSON.
This means that a cast operation with a string-type input of invalid JSON
results in a successful cast to invalid JSON.

Instead, consider using the {func}`json_parse` function to
create validated JSON from a string.
:::

The following examples show the behavior of casting to JSON with these types:

```
SELECT CAST(NULL AS JSON);
-- NULL

SELECT CAST(1 AS JSON);
-- JSON '1'

SELECT CAST(9223372036854775807 AS JSON);
-- JSON '9223372036854775807'

SELECT CAST('abc' AS JSON);
-- JSON '"abc"'

SELECT CAST(true AS JSON);
-- JSON 'true'

SELECT CAST(1.234 AS JSON);
-- JSON '1.234'

SELECT CAST(ARRAY[1, 23, 456] AS JSON);
-- JSON '[1,23,456]'

SELECT CAST(ARRAY[1, NULL, 456] AS JSON);
-- JSON '[1,null,456]'

SELECT CAST(ARRAY[ARRAY[1, 23], ARRAY[456]] AS JSON);
-- JSON '[[1,23],[456]]'

SELECT CAST(MAP(ARRAY['k1', 'k2', 'k3'], ARRAY[1, 23, 456]) AS JSON);
-- JSON '{"k1":1,"k2":23,"k3":456}'

SELECT CAST(CAST(ROW(123, 'abc', true) AS
            ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN)) AS JSON);
-- JSON '{"v1":123,"v2":"abc","v3":true}'
```

Casting from NULL to `JSON` is not straightforward. Casting
from a standalone `NULL` will produce SQL `NULL` instead of
`JSON 'null'`. However, when casting from arrays or map containing
`NULL`s, the produced `JSON` will have `null`s in it.

## Cast from JSON

Casting to `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`,
`BIGINT`, `REAL`, `DOUBLE` or `VARCHAR` is supported.
Casting to `ARRAY` and `MAP` is supported when the element type of
the array is one of the supported types, or when the key type of the map
is `VARCHAR` and value type of the map is one of the supported types.
Behaviors of the casts are shown with the examples below:

```
SELECT CAST(JSON 'null' AS VARCHAR);
-- NULL

SELECT CAST(JSON '1' AS INTEGER);
-- 1

SELECT CAST(JSON '9223372036854775807' AS BIGINT);
-- 9223372036854775807

SELECT CAST(JSON '"abc"' AS VARCHAR);
-- abc

SELECT CAST(JSON 'true' AS BOOLEAN);
-- true

SELECT CAST(JSON '1.234' AS DOUBLE);
-- 1.234

SELECT CAST(JSON '[1,23,456]' AS ARRAY(INTEGER));
-- [1, 23, 456]

SELECT CAST(JSON '[1,null,456]' AS ARRAY(INTEGER));
-- [1, NULL, 456]

SELECT CAST(JSON '[[1,23],[456]]' AS ARRAY(ARRAY(INTEGER)));
-- [[1, 23], [456]]

SELECT CAST(JSON '{"k1":1,"k2":23,"k3":456}' AS MAP(VARCHAR, INTEGER));
-- {k1=1, k2=23, k3=456}

SELECT CAST(JSON '{"v1":123,"v2":"abc","v3":true}' AS
            ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN));
-- {v1=123, v2=abc, v3=true}

SELECT CAST(JSON '[123,"abc",true]' AS
            ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN));
-- {v1=123, v2=abc, v3=true}
```

JSON arrays can have mixed element types and JSON maps can have mixed
value types. This makes it impossible to cast them to SQL arrays and maps in
some cases. To address this, Trino supports partial casting of arrays and maps:

```
SELECT CAST(JSON '[[1, 23], 456]' AS ARRAY(JSON));
-- [JSON '[1,23]', JSON '456']

SELECT CAST(JSON '{"k1": [1, 23], "k2": 456}' AS MAP(VARCHAR, JSON));
-- {k1 = JSON '[1,23]', k2 = JSON '456'}

SELECT CAST(JSON '[null]' AS ARRAY(JSON));
-- [JSON 'null']
```

When casting from `JSON` to `ROW`, both JSON array and JSON object are supported.

## Other JSON functions

In addition to the functions explained in more details in the preceding
sections, the following functions are available:

:::{function} is_json_scalar(json) -> boolean
Determine if `json` is a scalar (i.e. a JSON number, a JSON string, `true`, `false` or `null`):

```
SELECT is_json_scalar('1');         -- true
SELECT is_json_scalar('[1, 2, 3]'); -- false
```
:::

:::{function} json_array_contains(json, value) -> boolean
Determine if `value` exists in `json` (a string containing a JSON array):

```
SELECT json_array_contains('[1, 2, 3]', 2); -- true
```
:::

::::{function} json_array_get(json_array, index) -> json

:::{warning}
The semantics of this function are broken. If the extracted element
is a string, it will be converted into an invalid `JSON` value that
is not properly quoted (the value will not be surrounded by quotes
and any interior quotes will not be escaped).

We recommend against using this function. It cannot be fixed without
impacting existing usages and may be removed in a future release.
:::

Returns the element at the specified index into the `json_array`.
The index is zero-based:

```
SELECT json_array_get('["a", [3, 9], "c"]', 0); -- JSON 'a' (invalid JSON)
SELECT json_array_get('["a", [3, 9], "c"]', 1); -- JSON '[3,9]'
```

This function also supports negative indexes for fetching element indexed
from the end of an array:

```
SELECT json_array_get('["c", [3, 9], "a"]', -1); -- JSON 'a' (invalid JSON)
SELECT json_array_get('["c", [3, 9], "a"]', -2); -- JSON '[3,9]'
```

If the element at the specified index doesn't exist, the function returns null:

```
SELECT json_array_get('[]', 0);                -- NULL
SELECT json_array_get('["a", "b", "c"]', 10);  -- NULL
SELECT json_array_get('["c", "b", "a"]', -10); -- NULL
```
::::

:::{function} json_array_length(json) -> bigint
Returns the array length of `json` (a string containing a JSON array):

```
SELECT json_array_length('[1, 2, 3]'); -- 3
```
:::

:::{function} json_extract(json, json_path) -> json
Evaluates the [JSONPath]-like expression `json_path` on `json`
(a string containing JSON) and returns the result as a JSON string:

```
SELECT json_extract(json, '$.store.book');
SELECT json_extract(json, '$.store[book]');
SELECT json_extract(json, '$.store["book name"]');
```

The {ref}`json_query function<json-query>` provides a more powerful and
feature-rich alternative to parse and extract JSON data.
:::

:::{function} json_extract_scalar(json, json_path) -> varchar
Like {func}`json_extract`, but returns the result value as a string (as opposed
to being encoded as JSON). The value referenced by `json_path` must be a
scalar (boolean, number or string).

```
SELECT json_extract_scalar('[1, 2, 3]', '$[2]');
SELECT json_extract_scalar(json, '$.store.book[0].author');
```
:::

::::{function} json_format(json) -> varchar
Returns the JSON text serialized from the input JSON value.
This is inverse function to {func}`json_parse`.

```
SELECT json_format(JSON '[1, 2, 3]'); -- '[1,2,3]'
SELECT json_format(JSON '"a"');       -- '"a"'
```

:::{note}
{func}`json_format` and `CAST(json AS VARCHAR)` have completely
different semantics.

{func}`json_format` serializes the input JSON value to JSON text conforming to
{rfc}`7159`. The JSON value can be a JSON object, a JSON array, a JSON string,
a JSON number, `true`, `false` or `null`.

```
SELECT json_format(JSON '{"a": 1, "b": 2}'); -- '{"a":1,"b":2}'
SELECT json_format(JSON '[1, 2, 3]');        -- '[1,2,3]'
SELECT json_format(JSON '"abc"');            -- '"abc"'
SELECT json_format(JSON '42');               -- '42'
SELECT json_format(JSON 'true');             -- 'true'
SELECT json_format(JSON 'null');             -- 'null'
```

`CAST(json AS VARCHAR)` casts the JSON value to the corresponding SQL VARCHAR value.
For JSON string, JSON number, `true`, `false` or `null`, the cast
behavior is same as the corresponding SQL type. JSON object and JSON array
cannot be cast to VARCHAR.

```
SELECT CAST(JSON '{"a": 1, "b": 2}' AS VARCHAR); -- ERROR!
SELECT CAST(JSON '[1, 2, 3]' AS VARCHAR);        -- ERROR!
SELECT CAST(JSON '"abc"' AS VARCHAR);            -- 'abc' (the double quote is gone)
SELECT CAST(JSON '42' AS VARCHAR);               -- '42'
SELECT CAST(JSON 'true' AS VARCHAR);             -- 'true'
SELECT CAST(JSON 'null' AS VARCHAR);             -- NULL
```
:::
::::

::::{function} json_parse(string) -> json
Returns the JSON value deserialized from the input JSON text.
This is inverse function to {func}`json_format`:

```
SELECT json_parse('[1, 2, 3]');   -- JSON '[1,2,3]'
SELECT json_parse('"abc"');       -- JSON '"abc"'
```

:::{note}
{func}`json_parse` and `CAST(string AS JSON)` have completely
different semantics.

{func}`json_parse` expects a JSON text conforming to {rfc}`7159`, and returns
the JSON value deserialized from the JSON text.
The JSON value can be a JSON object, a JSON array, a JSON string, a JSON number,
`true`, `false` or `null`.

```
SELECT json_parse('not_json');         -- ERROR!
SELECT json_parse('["a": 1, "b": 2]'); -- JSON '["a": 1, "b": 2]'
SELECT json_parse('[1, 2, 3]');        -- JSON '[1,2,3]'
SELECT json_parse('"abc"');            -- JSON '"abc"'
SELECT json_parse('42');               -- JSON '42'
SELECT json_parse('true');             -- JSON 'true'
SELECT json_parse('null');             -- JSON 'null'
```

`CAST(string AS JSON)` takes any VARCHAR value as input, and returns
a JSON string with its value set to input string.

```
SELECT CAST('not_json' AS JSON);         -- JSON '"not_json"'
SELECT CAST('["a": 1, "b": 2]' AS JSON); -- JSON '"[\"a\": 1, \"b\": 2]"'
SELECT CAST('[1, 2, 3]' AS JSON);        -- JSON '"[1, 2, 3]"'
SELECT CAST('"abc"' AS JSON);            -- JSON '"\"abc\""'
SELECT CAST('42' AS JSON);               -- JSON '"42"'
SELECT CAST('true' AS JSON);             -- JSON '"true"'
SELECT CAST('null' AS JSON);             -- JSON '"null"'
```
:::
::::

:::{function} json_size(json, json_path) -> bigint
Like {func}`json_extract`, but returns the size of the value.
For objects or arrays, the size is the number of members,
and the size of a scalar value is zero.

```
SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x');   -- 2
SELECT json_size('{"x": [1, 2, 3]}', '$.x');          -- 3
SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x.a'); -- 0
```
:::

[jsonpath]: http://goessner.net/articles/JsonPath/
