# Regular expression function properties

These properties allow tuning the {doc}`/functions/regexp`.

## `regex-library`

- **Type:** {ref}`prop-type-string`
- **Allowed values:** `JONI`, `REGULATOR`, `RE2J`
- **Default value:** `JONI`

Selects the regular expression engine for the server, including JSON path
`like_regex` predicates. Set the same value on the coordinator and all workers.
There is no session override. For example, to use Regulator:

```properties
regex-library=REGULATOR
```

`JONI` uses Trino's fork of Joni. It supports backtracking, which can require
exponential time for some patterns. `REGULATOR` uses Airlift Regulator 1.1 and
provides linear-time matching with bounded memory. It operates directly on
Trino's UTF-8 strings. `RE2J` remains available as a legacy option.

Regulator supports the regular subset of Trino's regex language. It rejects
lookahead, lookbehind, backreferences, atomic groups, possessive quantifiers,
`\G`, `\Z`, and repetition counts above 1,000. Unsupported patterns fail with
an error; the server does not fall back to Joni. Supported patterns can also
differ in Unicode case folding and capture-sensitive loops that match empty
input. See the [Regulator 1.1 language reference](https://github.com/airlift/regulator/blob/1.1/docs/reference/languages/TRINO_REGEXP.md)
for the full compatibility details.

The former `deprecated.regex-library` property is accepted as an alias.

## `re2j.dfa-states-limit`

- **Type:** {ref}`prop-type-integer`
- **Minimum value:** `2`
- **Default value:** `2147483647`

The maximum number of states to use when RE2J builds the fast,
but potentially memory intensive, deterministic finite automaton (DFA)
for regular expression matching. If the limit is reached, RE2J falls
back to the algorithm that uses the slower, but less memory intensive
non-deterministic finite automaton (NFA). Decreasing this value decreases the
maximum memory footprint of a regular expression search at the cost of speed.

## `re2j.dfa-retries`

- **Type:** {ref}`prop-type-integer`
- **Minimum value:** `0`
- **Default value:** `5`

The number of times that RE2J retries the DFA algorithm, when
it reaches a states limit before using the slower, but less memory
intensive NFA algorithm, for all future inputs for that search. If hitting the
limit for a given input row is likely to be an outlier, you want to be able
to process subsequent rows using the faster DFA algorithm. If you are likely
to hit the limit on matches for subsequent rows as well, you want to use the
correct algorithm from the beginning so as not to waste time and resources.
The more rows you are processing, the larger this value should be.
