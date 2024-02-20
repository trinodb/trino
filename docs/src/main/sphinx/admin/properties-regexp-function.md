# Regular expression function properties

These properties allow tuning the {doc}`/functions/regexp`.

## `regex-library`

- **Type:** {ref}`prop-type-string`
- **Allowed values:** `JONI`, `RE2J`
- **Default value:** `JONI`

Which library to use for regular expression functions.
`JONI` is generally faster for common usage, but can require exponential
time for certain expression patterns. `RE2J` uses a different algorithm,
which guarantees linear time, but is often slower.

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
