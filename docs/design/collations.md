# Collations

Trino compares character strings by their UTF-8 bytes, which orders them by Unicode
code point. That is correct and fast, but it is not alphabetical order in any natural
language, and it cannot express case-insensitive matching. Users work around it with
`lower(x) = lower(y)`, which defeats predicate pushdown and still mishandles characters
whose case mapping is not one-to-one.

A **collation** is a named rule for comparing strings. Trino makes the collation part
of the character string type: `varchar(10) COLLATE unicode_ci` is a different type from
`varchar(10)`. The type's equality, hashing, and ordering operators implement its
collation, so every operation that obtains operators from the type, such as joins,
grouping, sorting, set operations, `IN`, and `min`/`max`, follows the collation without
code of its own. How an expression came to have its collation is a separate property
of the expression, which the analyzer tracks.

The SQL surface is documented under [Collations](../src/main/sphinx/language/types.md#varchar-collations)
and in [Collation functions](../src/main/sphinx/functions/collation.md). This document
describes the design behind it.

## Scope

The design covers collated `varchar` types, the `COLLATE` clause in types and
expressions, the `PAD SPACE` pad attribute, `COLLATION FOR`, `SHOW COLLATIONS`, and
`information_schema.collations`. Queries that name no collation keep their results,
plans, and performance: the default collation is the existing type with its existing
operators.

The design does not cover character sets other than Unicode, user-defined collations
(`CREATE COLLATION`), the SQL-session collation (`SET COLLATION`), collations on `char`,
or collation-aware pattern matching. `LIKE` and the functions that search a string's
contents reject non-default collations instead of silently comparing code points.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Collation | A named, immutable comparison rule over strings, with a pad attribute. |
| Default collation | `ucs_basic`: code-point order and byte equality, the behavior of every existing string type. |
| Collated type | A `varchar` type whose collation is not the default. |
| Collation key | A byte string derived from a value so that comparing keys as unsigned bytes gives the collation's order, and equal values have equal keys. |
| Derivation | How an expression's collation was established: explicit, implicit, coercible, or none. An analysis fact, not part of the type. |
| Determination | Choosing the collation an operation compares with, from its operands' collations and derivations. |

## Collation catalog

The catalog is a fixed, explicitly listed set of well-known collations:

| Name | Ordering | Distinguishes |
| --- | --- | --- |
| `ucs_basic` | Unicode code points | Every code point |
| `unicode` | The Unicode Collation Algorithm with the root ordering (ISO/IEC 14651), tertiary strength | Base letters, accents, and case |
| `unicode_ci` | The same, at secondary strength | Base letters and accents |

ISO/IEC 9075-2 defines `ucs_basic` and `unicode`. `unicode_ci` is the case-insensitive
comparison that remote systems commonly offer, such as MySQL's `_ci` collations. The
catalog does not follow what ICU happens to provide: it is the stable target onto
which remote collations can later be mapped, and a collation joins it by name, with its
definition in code, when a need justifies it. An unknown name fails analysis with
`COLLATION_NOT_FOUND`. Trino never falls back to another collation, because the results
would silently differ from what the user asked for.

A collation name is an identifier, canonicalized as a row field name is: unquoted, it
folds to lower case, and quoted, it is taken as written. `COLLATE UNICODE_CI` and
`COLLATE "unicode_ci"` name `unicode_ci`, and `COLLATE "UNICODE_CI"` names no collation.

`unicode` and `unicode_ci` are implemented with ICU4J in `trino-main`. The SPI exposes
collation names only, so plugins do not depend on ICU. Spaces and punctuation are
significant in every collation (ICU's non-ignorable alternate handling), so trailing
spaces are governed only by the pad attribute.

## Architecture

```text
SQL text ──parser──▶ COLLATE expressions and CollatedDataType
                          │
                          ▼
ExpressionAnalyzer ──────▶ CollationSemantics (derivation, combination, determination)
  types, coercions,                 │
  derivations                       ▼
                         collated VarcharType (SPI): names its collation only
                                    │ operators resolved by the TypeOperators asking
                                    ▼
          ┌─────────────────────────┼──────────────────────────┐
          ▼                         ▼                          ▼
   planner and optimizer     execution operators          connectors
   (guarded rewrites,        (hash, sort, compare         (capability-gated
   collation-key rules)       through type operators)      column types)
```

The **type** owns comparison behavior. The **analyzer** owns derivation: it applies the
standard's rules, relabels operands to the collation an operation uses, and reports
conflicts. The **planner** treats a collated type as a type whose equality does not
imply identity. **Connectors** opt in to storing collated columns.

## Types

### Representation

`Collation` is a final SPI value: a canonical name and a pad attribute, with no
comparison code and no SQL rendering. `VarcharType.getCollation()` returns it. The type
descriptor carries the collation as a text type parameter after the length, using the
new `TypeParameter.Text`:

| Descriptor | SQL spelling |
| --- | --- |
| `varchar(10)` | `varchar(10)` |
| `varchar(10,'unicode_ci')` | `varchar(10) COLLATE "unicode_ci"` |
| `varchar(10,'unicode_ci pad space')` | `varchar(10) COLLATE "unicode_ci" PAD SPACE` |
| `varchar(2147483647,'unicode')` | `varchar COLLATE "unicode"` |

The text parameter is the collation's **descriptor**: its name, followed by
` pad space` for a `PAD SPACE` collation. The default collation is never written, so every
existing descriptor, `TypeId`, and serialized plan is unchanged, and
`varchar(10) COLLATE ucs_basic` is the same type as `varchar(10)`. The descriptor is
persisted in type ids, serialized types, and view definitions, so its format is a
compatibility commitment from the first release that includes collated types.

A type's SQL spelling is also its `TypeId`, which the engine reads back through the SQL
type parser. `TypeSyntax` therefore always quotes the collation name, as it quotes row
field names, so the spelling parses back for any name. That spelling appears in
`information_schema.columns.data_type`, `DESCRIBE`, and messages. SQL the engine formats
from the AST, such as `SHOW CREATE TABLE`, quotes the name only where needed.

### Type operators

| Operator | Default collation | Collated type |
| --- | --- | --- |
| Equal, identical | Byte equality | The comparison returns zero |
| Hash code, xxHash64 | Hash of the bytes | Hash of the collation key |
| Comparison, less than | Byte order | The collation's order |
| Read value | The stored bytes | The stored bytes |

Hash and equality must agree: strings equal under the collation must hash equally,
which is why the hash is computed from the collation key. A value is still the original
string. `GROUP BY` returns one of the equal input values as the group's value; which one
is not specified, as the standard says for character-string grouping.

A collated type names its collation only. Its operators are built for the
`TypeOperators` that asks for them, through `Type.getTypeOperatorDeclaration(TypeOperators)`,
from `TypeOperators.collationComparison(collation)`. The engine creates its type
operators with its catalog as their `CollationComparisons`. The comparison contract is
documented on `CollationComparison`: arguments are never null and may hold invalid
UTF-8, implementations are deterministic and thread-safe, `equal` holds exactly when
`compare` returns zero, and collation keys order as `compare` does.

Resolving the comparison where operators are built keeps the type a value, and keeps
plugins from supplying comparison code through a type. The cost is a rule: **collated
strings stay out of SPI and library type operators.** Type operators created without
collation comparisons, such as those behind `TupleDomain`, `ValueSet`, and
`NullableValue`, and those some file-format libraries create for themselves, can read
collated strings but fail with an error when they compare, hash, or order them. They
never fall back to comparing bytes. The planner keeps collated strings away from those
operators (see Optimizer correctness), and a connector that stores collated columns must
compare them with the engine's type operators.

### Pad attribute

A `NO PAD` collation treats every character as significant. A `PAD SPACE` collation
compares the shorter of two strings as though it were extended with spaces to the length
of the longer (ISO/IEC 9075-2 4.3.2): `'abc  '` equals `'abc'`, and `'abc' || chr(9)`
sorts before `'abc'`, because a tab sorts below a space. Leading and inner spaces stay
significant. Collations that differ only in their pad attribute are different
collations, and combining them implicitly is a conflict.

`ucs_basic PAD SPACE` compares as `char(n)` does: the bytes past the end of the shorter
string are compared with `0x20`, and equality and hashing remove trailing spaces.

ICU collations pad collation-element weights rather than characters: at each level, the
shorter weight sequence is compared as though extended with the space's weight at that
level. Padding characters would make equality depend on the other operand's length,
because ICU gives some characters no weight, and so it would have no per-value hash or
key. Padding weights agrees with padding characters for characters that have one weight
at each level. A character the collation ignores does not count toward the padding.

The engine reads ICU's collation elements and builds its own keys, because ICU's sort
keys compress weights. A key encodes the end of a sequence as an unbounded run of pad
weights, so key order is the padded order (`PadSpaceKeys`). This relies on ICU's weight
layout, which `TestPadSpaceComparisons` cross-checks against `Collator.compare` on padded
inputs, so an ICU upgrade that changes the layout fails tests rather than results.

### Casts and coercions

Changing collation is a cast: `CAST(x AS varchar(10) COLLATE unicode)`, or the shorthand
`x COLLATE unicode`, which keeps the length. A cast between collations does not change
the value. There is no implicit coercion between different collations, which would make
comparison operators ambiguous and hide conflicts the standard requires to be errors.
The analyzer inserts collation casts itself, after determination, and only where
determination allows. Length coercions keep the collation.

String casts are declared over default-collation types, and their implementations
operate on bytes. Cast resolution therefore matches a cast involving collated types
against the declarations as if the types had the default collation, and binds it with
the actual collated types. A stored value takes the column's collation, so store
assignment ignores collations.

### SQL syntax

The type grammar accepts `type COLLATE identifier [PAD SPACE | NO PAD]` as a
left-recursive type suffix, as it accepts `type ARRAY`, and builds a `CollatedDataType`
node around the type it follows. ISO/IEC 9075-2 allows a collate clause only for
character string types. The grammar is generic and plugin-extensible, so it accepts the
clause after any type, and analysis rejects it except for `varchar` with `TYPE_MISMATCH`.
The clause binds before a following `ARRAY`, so `varchar COLLATE unicode_ci ARRAY` is an
array of collated strings.

The expression form, `expression COLLATE identifier [PAD SPACE | NO PAD]`, binds tighter
than `||`. `COLLATE`, `COLLATION`, `COLLATIONS`, `PAD`, and `SPACE` are non-reserved
keywords, so existing identifiers with those names keep working.

## Derivation and determination

The rules live in `CollationSemantics`, which takes operand collations and derivations
and returns a result or an error, with no dependency on the analyzer. The expression
visitor calls it at the existing common-type and comparison sites.

### Derivation

| Expression | Derivation |
| --- | --- |
| `expr COLLATE c` | Explicit |
| A column, field, or parameter, or a cast | Implicit, the type's collation |
| A string literal, typed literal, or `NULL` | Coercible, `ucs_basic` |
| A combination of operands | The combination rule's result |

Derivations are ordered coercible < implicit < explicit. **None** is the result of
combining implicit operands of different collations; it has no collation.

The standard makes a literal implicit in the default collation, under which
`ci_column = 'abc'` would be an error, because two implicit collations differ. That is
unusable, and other systems let literals yield to the other operand. Trino adds the
**coercible** derivation, weaker than implicit, for literals and for expressions
computed only from them. This is a deliberate departure from the standard.

### Combination and determination

The combination rule (ISO/IEC 9075-2 9.5) applies wherever operand types combine into
one: `CASE`, `COALESCE`, `ARRAY[...]`, string function results, and the columns of
`VALUES` and set operations. Explicit operands must agree, and their collation wins.
Otherwise implicit operands decide, and when they differ the result has derivation
none. Otherwise the operands are coercible. A value with derivation none can be returned
and stored.

Determination (9.15) chooses the collation for operations that compare: comparisons,
`BETWEEN`, `IN`, `IS DISTINCT FROM`, `NULLIF`, join criteria, `GROUP BY`, `DISTINCT`,
`ORDER BY`, window partitioning and ordering, set operations other than `UNION ALL`,
`IN` subqueries and quantified comparisons, and the arguments of aggregations. It applies
the combination rule and fails with `COLLATION_MISMATCH` when the result has derivation
none. Operands are then relabeled to the determined collation, and the operation uses
that type's operators.

```sql
-- u: varchar COLLATE unicode; ci: varchar COLLATE unicode_ci
u = 'x'                    -- unicode: the literal yields
u = ci                     -- error: implicit collations differ
u = ci COLLATE unicode_ci  -- unicode_ci: explicit wins
```

The analyzer records each relabeled operand as a coercion to the collated type, so the
existing common-type and operator resolution sees operands of one collation, and one
coercion carries both a collation change and a length change.

### Composite values and relations

A value of an array, map, or row type has a **collation shape**: the derivation of each
string it contains, mirroring its type. Combination and determination apply at each
string position, as comparing rows compares their fields pairwise, so
`ROW(1, ci_value) = ROW(1, 'a')` compares the second fields under `unicode_ci`.

A relation's output column records its derivation when it is not implicit from its type,
so a literal stays coercible through subqueries, `WITH` queries, joins, and aliases.
An explicitly collated value is implicit once it is a relation column, as the standard
specifies for a reference to a derived column.

### Functions

String functions are declared over default-collation types, and the engine handles
collated arguments without per-function annotations:

1. Binding first uses the collated argument types, so a declaration over a type
   variable, such as `max`, `element_at`, or `array_sort`, carries the collation.
2. When no declaration accepts the collated arguments, the binder binds them again as
   default-collation strings, including strings nested in arrays, maps, and rows. This is
   how `upper(x)` binds `upper(varchar(x))` and `concat_ws(',', array)` binds
   `concat_ws(varchar, array(varchar))`.
3. A string result of a function with collated string arguments takes their combined
   collation, so `upper` and `substr` keep their input's collation. Aggregate and
   window functions that return strings, such as `max` and `listagg`, return their
   argument's collation.

The second step is safe only because the analyzer aligns the operands of every
comparison to one collation before resolving the comparison. A `char` has no collation,
so the analyzer cannot align a collated string with one; it rejects such a comparison
with `COLLATION_MISMATCH`, directly or through a function that compares a type variable,
such as `greatest`. Without that check, the second step would bind the comparison as a
comparison of uncollated strings and compare code points. For functions over type
variables, before resolution the analyzer relabels the strings bound to each variable to
their combined collation, so a literal yields (`contains(ci_array, 'a')`). After
resolution it determines the strings at every type variable the function compares: one
that is comparable or orderable, or that a comparison or hash operator dependency uses.
Lambda parameters take the shapes of the values bound to their variables.

Built-in functions that search or compare their arguments' contents by code point, such
as `strpos`, `starts_with`, `replace`, `split`, the `regexp_*` functions, and
`approx_set`, fail with `NOT_SUPPORTED` for a collated argument. The check matches the
resolved built-in function, so a plugin function of the same name is unaffected.

## Optimizer correctness

A collated type is comparable and orderable, but equality under it does not identify a
string: under `unicode_ci`, `'Alice'` and `'ALICE'` are equal and distinguishable. Every
rewrite that replaces a value by an equal one, or derives facts about one value from an
equal one, changes results. An inner join of `'Alice'` and `'ALICE'` under `unicode_ci`
must project both strings; merging the right key's symbol into the left one projects
`'Alice'` twice.

The planner identifies these types with `Collations.hasNonDefaultCollation`, which is
true for a collated `varchar` and for any type containing one. The rewrites that check it:

| Rewrite | Behavior for collated types |
| --- | --- |
| Equality inference | Does not substitute one side of an equality for the other. |
| Join symbol aliasing (`UnaliasSymbolReferences`) | Does not merge the join keys' symbols. |
| Domain extraction (`DomainTranslator`) and `EffectivePredicateExtractor` | Produce no domain. The predicate stays a filter. |
| Dynamic filtering | Creates no dynamic filter on a collated join key. |
| Function preimages, comparison preimages, cast unwrapping | Decline. A collation does not order a value as a code-point range bounds it. |
| Connector expression translation | Does not translate. Collated expressions are evaluated by the engine. |

A missed guard fails in one of two ways. Where it hands collated strings to type
operators without collation comparisons, the query fails with an error. Where it
substitutes equal values, results are silently wrong, which is why every substituting
rewrite must check the type. Rewrites that rely only on hash compatibility, such as
partitioned exchanges, stay valid, because the type's hash already follows the
collation. Value sets never order a collated string by its bytes: their type operators
have no collation comparisons, so a value set of collated strings fails rather than
comparing them.

The guard is specific to collations. Every other type keeps its current treatment, so
collations add no plan changes to queries that do not use them.

## Execution cost

Comparing or hashing a collated string applies the collation. ICU compares Java strings,
so each comparison decodes both values, and a hash builds a complete collation key. For
strings of about a dozen characters, a collated comparison costs about nine times a byte
comparison and a collated hash about twenty times a byte hash, while comparing two
precomputed keys costs about the same as comparing bytes. ICU collators are used through
per-thread copies, because a shared frozen collator serializes comparisons on an
internal lock.

Grouping and sorting therefore use collation keys, through the hidden `$collation_key`
function, which returns `varbinary`:

- `GroupByCollationKey` groups an aggregation by `$collation_key(x)` and produces the
  group's value with `any_value(x)`. Equal keys identify strings equal under the
  collation, so the groups are the same, and the value is a member of its group. It
  applies to single-step aggregations with one grouping set, including `DISTINCT` and
  `count(DISTINCT x)` once they are rewritten to grouping. It nearly halves a `GROUP BY`
  over 150,000 collated rows.
- `SortByCollationKey` rewrites sorts, TopN, windows, ranking TopN, and row numbering
  whose order or partition keys include a collated string. Each projects the key below
  the node, orders or partitions by it, and projects it away above the node, which still
  receives and produces the strings. A null string has a null key, so null ordering is
  unchanged. A complete sort of 150,000 rows takes about half as long.

The rules always apply: they preserve results, so there is no setting to disable them.
Joins still compare with the collation, because a join on keys computed on both sides
measured no faster than the collated join.

## Connectors and clients

A connector stores collated columns only if it declares the `COLLATED_COLUMNS` connector
capability, which promises that it compares the column under its collation wherever it
compares values itself. Without it, a collated type fails with `NOT_SUPPORTED` wherever a
column type reaches the connector, before the connector maps the type, so a connector
cannot drop a collation by storing an uncollated type. The memory connector declares the
capability. Views persist column types by their SQL spelling and work with any connector
that stores views.

Clients parse type names, so the protocol reports a collated column as its type without
the collation, including in nested types. The collation is available through SQL:
`information_schema.columns.collation_name` and `COLLATION FOR (expr)` report it, and
`DESCRIBE`, `SHOW CREATE TABLE`, and `information_schema.columns.data_type` show the SQL
spelling with `COLLATE`.

## Compatibility

- Queries without `COLLATE` or collated columns keep their results, plans, and
  performance.
- Existing type descriptors and plans are unchanged. A coordinator and its workers must
  run the same version, as for any type change.
- A view that uses a collation is not readable by an older version.
- A collation's order can change when Trino upgrades ICU. Trino persists no order, and a
  connector must not advertise collated sort order or bucketing without recording the
  ICU version.

## Limitations

- `PAD SPACE` under ICU collations pads weights, not characters, which differs from the
  standard for characters with no weight or several weights at a level.
- Grouping or sorting an array, map, or row that contains collated strings compares with
  the collation, without collation keys.
- `LIKE` supports only the default collation.
- Joins, `IN` lists, and comparisons apply the collation per comparison and are several
  times slower than byte comparisons.
- Predicates on collated columns are not pushed to connectors and produce no domains.
  The strings equal to a value under a collation form an equivalence class, not a
  code-point range. Collated domains would need domains that compare under the
  collation, values that mean equal rather than identical, and pushdown only to a
  connector with the same collation.
