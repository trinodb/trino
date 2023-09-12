# Regular expression functions

All of the regular expression functions use the [Java pattern] syntax,
with a few notable exceptions:

- When using multi-line mode (enabled via the `(?m)` flag),
  only `\n` is recognized as a line terminator. Additionally,
  the `(?d)` flag is not supported and must not be used.

- Case-insensitive matching (enabled via the `(?i)` flag) is always
  performed in a Unicode-aware manner. However, context-sensitive and
  local-sensitive matching is not supported. Additionally, the
  `(?u)` flag is not supported and must not be used.

- Surrogate pairs are not supported. For example, `\uD800\uDC00` is
  not treated as `U+10000` and must be specified as `\x{10000}`.

- Boundaries (`\b`) are incorrectly handled for a non-spacing mark
  without a base character.

- `\Q` and `\E` are not supported in character classes
  (such as `[A-Z123]`) and are instead treated as literals.

- Unicode character classes (`\p{prop}`) are supported with
  the following differences:

  - All underscores in names must be removed. For example, use
    `OldItalic` instead of `Old_Italic`.

  - Scripts must be specified directly, without the
    `Is`, `script=` or `sc=` prefixes.
    Example: `\p{Hiragana}`

  - Blocks must be specified with the `In` prefix.
    The `block=` and `blk=` prefixes are not supported.
    Example: `\p{Mongolian}`

  - Categories must be specified directly, without the `Is`,
    `general_category=` or `gc=` prefixes.
    Example: `\p{L}`

  - Binary properties must be specified directly, without the `Is`.
    Example: `\p{NoncharacterCodePoint}`

:::{function} regexp_count(string, pattern) -> bigint
Returns the number of occurrence of `pattern` in `string`:

```
SELECT regexp_count('1a 2b 14m', '\s*[a-z]+\s*'); -- 3
```
:::

:::{function} regexp_extract_all(string, pattern) -> array(varchar)
Returns the substring(s) matched by the regular expression `pattern`
in `string`:

```
SELECT regexp_extract_all('1a 2b 14m', '\d+'); -- [1, 2, 14]
```
:::

:::{function} regexp_extract_all(string, pattern, group) -> array(varchar)
:noindex: true

Finds all occurrences of the regular expression `pattern` in `string`
and returns the [capturing group number] `group`:

```
SELECT regexp_extract_all('1a 2b 14m', '(\d+)([a-z]+)', 2); -- ['a', 'b', 'm']
```
:::

:::{function} regexp_extract(string, pattern) -> varchar
Returns the first substring matched by the regular expression `pattern`
in `string`:

```
SELECT regexp_extract('1a 2b 14m', '\d+'); -- 1
```
:::

:::{function} regexp_extract(string, pattern, group) -> varchar
:noindex: true

Finds the first occurrence of the regular expression `pattern` in
`string` and returns the [capturing group number] `group`:

```
SELECT regexp_extract('1a 2b 14m', '(\d+)([a-z]+)', 2); -- 'a'
```
:::

:::{function} regexp_like(string, pattern) -> boolean
Evaluates the regular expression `pattern` and determines if it is
contained within `string`.

The `pattern` only needs to be contained within
`string`, rather than needing to match all of `string`. In other words,
this performs a *contains* operation rather than a *match* operation. You can
match the entire string by anchoring the pattern using `^` and `$`:

```
SELECT regexp_like('1a 2b 14m', '\d+b'); -- true
```
:::

:::{function} regexp_position(string, pattern) -> integer
Returns the index of the first occurrence (counting from 1) of `pattern` in `string`.
Returns -1 if not found:

```
SELECT regexp_position('I have 23 apples, 5 pears and 13 oranges', '\b\d+\b'); -- 8
```
:::

:::{function} regexp_position(string, pattern, start) -> integer
:noindex: true

Returns the index of the first occurrence of `pattern` in `string`,
starting from `start` (include `start`). Returns -1 if not found:

```
SELECT regexp_position('I have 23 apples, 5 pears and 13 oranges', '\b\d+\b', 5); -- 8
SELECT regexp_position('I have 23 apples, 5 pears and 13 oranges', '\b\d+\b', 12); -- 19
```
:::

:::{function} regexp_position(string, pattern, start, occurrence) -> integer
:noindex: true

Returns the index of the nth `occurrence` of `pattern` in `string`,
starting from `start` (include `start`). Returns -1 if not found:

```
SELECT regexp_position('I have 23 apples, 5 pears and 13 oranges', '\b\d+\b', 12, 1); -- 19
SELECT regexp_position('I have 23 apples, 5 pears and 13 oranges', '\b\d+\b', 12, 2); -- 31
SELECT regexp_position('I have 23 apples, 5 pears and 13 oranges', '\b\d+\b', 12, 3); -- -1
```
:::

:::{function} regexp_replace(string, pattern) -> varchar
Removes every instance of the substring matched by the regular expression
`pattern` from `string`:

```
SELECT regexp_replace('1a 2b 14m', '\d+[ab] '); -- '14m'
```
:::

:::{function} regexp_replace(string, pattern, replacement) -> varchar
:noindex: true

Replaces every instance of the substring matched by the regular expression
`pattern` in `string` with `replacement`. [Capturing groups] can be
referenced in `replacement` using `$g` for a numbered group or
`${name}` for a named group. A dollar sign (`$`) may be included in the
replacement by escaping it with a backslash (`\$`):

```
SELECT regexp_replace('1a 2b 14m', '(\d+)([ab]) ', '3c$2 '); -- '3ca 3cb 14m'
```
:::

:::{function} regexp_replace(string, pattern, function) -> varchar
:noindex: true

Replaces every instance of the substring matched by the regular expression
`pattern` in `string` using `function`. The {doc}`lambda expression <lambda>`
`function` is invoked for each match with the [capturing groups] passed as an
array. Capturing group numbers start at one; there is no group for the entire match
(if you need this, surround the entire expression with parenthesis).

```
SELECT regexp_replace('new york', '(\w)(\w*)', x -> upper(x[1]) || lower(x[2])); --'New York'
```
:::

:::{function} regexp_split(string, pattern) -> array(varchar)
Splits `string` using the regular expression `pattern` and returns an
array. Trailing empty strings are preserved:

```
SELECT regexp_split('1a 2b 14m', '\s*[a-z]+\s*'); -- [1, 2, 14, ]
```
:::

[capturing group number]: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/regex/Pattern.html#gnumber
[capturing groups]: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/regex/Pattern.html#cg
[java pattern]: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/regex/Pattern.html
