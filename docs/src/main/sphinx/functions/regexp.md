# Regular expression functions

The {func}`multi_match_any` function uses RE2 syntax. The `regexp_*` functions
use the [Java pattern] syntax, with a few notable exceptions:

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

:::{function} multi_match_any(string, patterns) -> boolean
Returns `true` if any regular expression in the `array(varchar)` `patterns`
matches a substring of `string`. Use `^` and `$` to match the whole string.
The function compiles the patterns into one expression and searches the string
with that expression rather than running a separate search for each pattern.

```sql
SELECT multi_match_any('WARN: request timed out', ARRAY['^ERROR:', 'timed out']); -- true
SELECT multi_match_any('INFO: request complete', ARRAY['^ERROR:', 'timed out']); -- false
SELECT multi_match_any('INFO: request complete', ARRAY[]); -- false
```

A `NULL` string or `NULL` array returns `NULL`. For a valid string, an empty
array returns `false`. An empty regular expression matches any string,
including the empty string. A `NULL` array element, invalid UTF-8 in either
argument, or an invalid pattern raises `INVALID_FUNCTION_ARGUMENT`. The
function validates all patterns before searching, even if an earlier pattern
would match. Both arguments can contain NUL characters.

Patterns use the RE2J implementation of [RE2 syntax], regardless of the
`regex-library` setting.
Backreferences and lookaround are not supported. Matching is case-sensitive by
default and operates on Unicode code points. Unicode character classes such as
`\p{L}` are supported, but `\w` and `\b` use ASCII word characters. Unlike
`regexp_like`, `.` matches newlines by default. Inline flags such as `(?i)`,
`(?m)`, and `(?-s)` apply only within their array entry. Capturing groups do not
affect the boolean result, and names may be reused in different entries.

The limits per call are 10,000 patterns, 64 KiB of UTF-8 bytes per pattern,
1 MiB of total pattern bytes, and 16 MiB of string bytes. Exceeding any of these
limits raises `INVALID_FUNCTION_ARGUMENT`.

Each function instance caches only its most recent compiled pattern set. The
function reuses that expression for consecutive calls with the same array
contents. It uses the `re2j.dfa-states-limit` and `re2j.dfa-retries` settings
described in
{doc}`/admin/properties-regexp-function`, including fallback to an NFA.
Neither those settings nor the input limits bound total compilation memory:
counted repetitions can expand the compiled expression beyond the size of the
pattern text. Compiled expressions and matching state use JVM heap outside
Trino's per-query memory accounting.
:::

[capturing group number]: https://docs.oracle.com/en/java/javase/23/docs/api/java.base/java/util/regex/Pattern.html#gnumber
[capturing groups]: https://docs.oracle.com/en/java/javase/23/docs/api/java.base/java/util/regex/Pattern.html#cg
[java pattern]: https://docs.oracle.com/en/java/javase/23/docs/api/java.base/java/util/regex/Pattern.html
[re2 syntax]: https://github.com/google/re2/wiki/Syntax
