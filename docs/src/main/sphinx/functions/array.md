# Array functions and operators

Array functions and operators use the [ARRAY type](array-type). Create an array
with the data type constructor.

Create an array of integer numbers:

```sql
SELECT ARRAY[1, 2, 4];
-- [1, 2, 4]
```

Create an array of character values:

```sql
SELECT ARRAY['foo', 'bar', 'bazz'];
-- [foo, bar, bazz]
```

Array elements must use the same type or it must be possible to coerce values to
a common type. The following example uses integer and decimal values and the
resulting array contains decimals:

```sql
SELECT ARRAY[1, 1.2, 4];
-- [1.0, 1.2, 4.0]
```

Null values are allowed:

```sql
SELECT ARRAY[1, 2, NULL, -4, NULL];
-- [1, 2, NULL, -4, NULL]
```

(subscript-operator)=
## Subscript operator: \[\]

The `[]` operator is used to access an element of an array and is indexed
starting from one:

```sql
SELECT my_array[1] AS first_element
```

The following example constructs an array and then accesses the second element:

```sql
SELECT ARRAY[1, 1.2, 4][2];
-- 1.2
```

(concatenation-operator)=
## Concatenation operator: ||

The `||` operator is used to concatenate an array with an array or an element of the same type:

```
SELECT ARRAY[1] || ARRAY[2];
-- [1, 2]

SELECT ARRAY[1] || 2;
-- [1, 2]

SELECT 2 || ARRAY[1];
-- [2, 1]
```

## Array functions

:::{function} all_match(array(T), function(T,boolean)) -> boolean
Returns whether all elements of an array match the given predicate. Returns `true` if all the elements
match the predicate (a special case is when the array is empty); `false` if one or more elements don't
match; `NULL` if the predicate function returns `NULL` for one or more elements and `true` for all
other elements.
:::

:::{function} any_match(array(T), function(T,boolean)) -> boolean
Returns whether any elements of an array match the given predicate. Returns `true` if one or more
elements match the predicate; `false` if none of the elements matches (a special case is when the
array is empty); `NULL` if the predicate function returns `NULL` for one or more elements and `false`
for all other elements.
:::

:::{function} array_distinct(x) -> array
Remove duplicate values from the array `x`.
:::

:::{function} array_intersect(x, y) -> array
Returns an array of the elements in the intersection of `x` and `y`, without duplicates.
:::

:::{function} array_union(x, y) -> array
Returns an array of the elements in the union of `x` and `y`, without duplicates.
:::

:::{function} array_except(x, y) -> array
Returns an array of elements in `x` but not in `y`, without duplicates.
:::

:::{function} array_histogram(x) -> map<K, bigint>
Returns a map where the keys are the unique elements in the input array
`x` and the values are the number of times that each element appears in
`x`. Null values are ignored.

```
SELECT array_histogram(ARRAY[42, 7, 42, NULL]);
-- {42=2, 7=1}
```

Returns an empty map if the input array has no non-null elements.

```
SELECT array_histogram(ARRAY[NULL, NULL]);
-- {}
```
:::

:::{function} array_join(x, delimiter) -> varchar
Concatenates the elements of the given array using the delimiter.
Null elements are omitted in the result.
:::

:::{function} array_join(x, delimiter, null_replacement) -> varchar
:noindex: true

Concatenates the elements of the given array using the delimiter and an optional string to replace nulls.
:::

:::{function} array_max(x) -> x
Returns the maximum value of input array.
:::

:::{function} array_min(x) -> x
Returns the minimum value of input array.
:::

:::{function} array_position(x, element) -> bigint
Returns the position of the first occurrence of the `element` in array `x` (or 0 if not found).
:::

:::{function} array_remove(x, element) -> array
Remove all elements that equal `element` from array `x`.
:::

:::{function} array_sort(x) -> array
Sorts and returns the array `x`. The elements of `x` must be orderable.
Null elements will be placed at the end of the returned array.
:::

:::{function} array_sort(array(T), function(T,T,int)) -> array(T)
:noindex: true

Sorts and returns the `array` based on the given comparator `function`.
The comparator will take two nullable arguments representing two nullable
elements of the `array`. It returns -1, 0, or 1 as the first nullable
element is less than, equal to, or greater than the second nullable element.
If the comparator function returns other values (including `NULL`), the
query will fail and raise an error.

```
SELECT array_sort(ARRAY[3, 2, 5, 1, 2],
                  (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)));
-- [5, 3, 2, 2, 1]

SELECT array_sort(ARRAY['bc', 'ab', 'dc'],
                  (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)));
-- ['dc', 'bc', 'ab']


SELECT array_sort(ARRAY[3, 2, null, 5, null, 1, 2],
                  -- sort null first with descending order
                  (x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END);
-- [null, null, 5, 3, 2, 2, 1]

SELECT array_sort(ARRAY[3, 2, null, 5, null, 1, 2],
                  -- sort null last with descending order
                  (x, y) -> CASE WHEN x IS NULL THEN 1
                                 WHEN y IS NULL THEN -1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END);
-- [5, 3, 2, 2, 1, null, null]

SELECT array_sort(ARRAY['a', 'abcd', 'abc'],
                  -- sort by string length
                  (x, y) -> IF(length(x) < length(y), -1,
                               IF(length(x) = length(y), 0, 1)));
-- ['a', 'abc', 'abcd']

SELECT array_sort(ARRAY[ARRAY[2, 3, 1], ARRAY[4, 2, 1, 4], ARRAY[1, 2]],
                  -- sort by array length
                  (x, y) -> IF(cardinality(x) < cardinality(y), -1,
                               IF(cardinality(x) = cardinality(y), 0, 1)));
-- [[1, 2], [2, 3, 1], [4, 2, 1, 4]]
```
:::

:::{function} arrays_overlap(x, y) -> boolean
Tests if arrays `x` and `y` have any non-null elements in common.
Returns null if there are no non-null elements in common but either array contains null.
:::

:::{function} cardinality(x) -> bigint
Returns the cardinality (size) of the array `x`.
:::

:::{function} concat(array1, array2, ..., arrayN) -> array
:noindex: true

Concatenates the arrays `array1`, `array2`, `...`, `arrayN`.
This function provides the same functionality as the SQL-standard concatenation operator (`||`).
:::

:::{function} combinations(array(T), n) -> array(array(T))
Returns n-element sub-groups of input array. If the input array has no duplicates,
`combinations` returns n-element subsets.

```
SELECT combinations(ARRAY['foo', 'bar', 'baz'], 2);
-- [['foo', 'bar'], ['foo', 'baz'], ['bar', 'baz']]

SELECT combinations(ARRAY[1, 2, 3], 2);
-- [[1, 2], [1, 3], [2, 3]]

SELECT combinations(ARRAY[1, 2, 2], 2);
-- [[1, 2], [1, 2], [2, 2]]
```

Order of sub-groups is deterministic but unspecified. Order of elements within
a sub-group deterministic but unspecified. `n` must be not be greater than 5,
and the total size of sub-groups generated must be smaller than 100,000.
:::

:::{function} contains(x, element) -> boolean
Returns true if the array `x` contains the `element`.
:::

:::{function} contains_sequence(x, seq) -> boolean
Return true if array `x` contains all of array `seq` as a subsequence (all values in the same consecutive order).
:::

:::{function} element_at(array(E), index) -> E
Returns element of `array` at given `index`.
If `index` > 0, this function provides the same functionality as the SQL-standard subscript operator (`[]`),
except that the function returns `NULL` when accessing an `index` larger than array length, whereas
the subscript operator would fail in such a case.
If `index` \< 0, `element_at` accesses elements from the last to the first.
:::

:::{function} filter(array(T), function(T,boolean)) -> array(T)
Constructs an array from those elements of `array` for which `function` returns true:

```
SELECT filter(ARRAY[], x -> true);
-- []

SELECT filter(ARRAY[5, -6, NULL, 7], x -> x > 0);
-- [5, 7]

SELECT filter(ARRAY[5, NULL, 7, NULL], x -> x IS NOT NULL);
-- [5, 7]
```
:::

:::{function} flatten(x) -> array
Flattens an `array(array(T))` to an `array(T)` by concatenating the contained arrays.
:::

:::{function} ngrams(array(T), n) -> array(array(T))
Returns `n`-grams (sub-sequences of adjacent `n` elements) for the `array`.
The order of the `n`-grams in the result is unspecified.

```
SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 2);
-- [['foo', 'bar'], ['bar', 'baz'], ['baz', 'foo']]

SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 3);
-- [['foo', 'bar', 'baz'], ['bar', 'baz', 'foo']]

SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 4);
-- [['foo', 'bar', 'baz', 'foo']]

SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 5);
-- [['foo', 'bar', 'baz', 'foo']]

SELECT ngrams(ARRAY[1, 2, 3, 4], 2);
-- [[1, 2], [2, 3], [3, 4]]
```
:::

:::{function} none_match(array(T), function(T,boolean)) -> boolean
Returns whether no elements of an array match the given predicate. Returns `true` if none of the elements
matches the predicate (a special case is when the array is empty); `false` if one or more elements match;
`NULL` if the predicate function returns `NULL` for one or more elements and `false` for all other elements.
:::

:::{function} reduce(array(T), initialState S, inputFunction(S,T,S), outputFunction(S,R)) -> R
Returns a single value reduced from `array`. `inputFunction` will
be invoked for each element in `array` in order. In addition to taking
the element, `inputFunction` takes the current state, initially
`initialState`, and returns the new state. `outputFunction` will be
invoked to turn the final state into the result value. It may be the
identity function (`i -> i`).

```
SELECT reduce(ARRAY[], 0,
              (s, x) -> s + x,
              s -> s);
-- 0

SELECT reduce(ARRAY[5, 20, 50], 0,
              (s, x) -> s + x,
              s -> s);
-- 75

SELECT reduce(ARRAY[5, 20, NULL, 50], 0,
              (s, x) -> s + x,
              s -> s);
-- NULL

SELECT reduce(ARRAY[5, 20, NULL, 50], 0,
              (s, x) -> s + coalesce(x, 0),
              s -> s);
-- 75

SELECT reduce(ARRAY[5, 20, NULL, 50], 0,
              (s, x) -> IF(x IS NULL, s, s + x),
              s -> s);
-- 75

SELECT reduce(ARRAY[2147483647, 1], BIGINT '0',
              (s, x) -> s + x,
              s -> s);
-- 2147483648

-- calculates arithmetic average
SELECT reduce(ARRAY[5, 6, 10, 20],
              CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)),
              (s, x) -> CAST(ROW(x + s.sum, s.count + 1) AS
                             ROW(sum DOUBLE, count INTEGER)),
              s -> IF(s.count = 0, NULL, s.sum / s.count));
-- 10.25
```
:::

:::{function} repeat(element, count) -> array
Repeat `element` for `count` times.
:::

:::{function} reverse(x) -> array
:noindex: true

Returns an array which has the reversed order of array `x`.
:::

:::{function} sequence(start, stop) -> array(bigint)
Generate a sequence of integers from `start` to `stop`, incrementing
by `1` if `start` is less than or equal to `stop`, otherwise `-1`.
:::

:::{function} sequence(start, stop, step) -> array(bigint)
:noindex: true

Generate a sequence of integers from `start` to `stop`, incrementing by `step`.
:::

:::{function} sequence(start, stop) -> array(date)
:noindex: true

Generate a sequence of dates from `start` date to `stop` date, incrementing
by `1` day if `start` date is less than or equal to `stop` date, otherwise `-1` day.
:::

:::{function} sequence(start, stop, step) -> array(date)
:noindex: true

Generate a sequence of dates from `start` to `stop`, incrementing by `step`.
The type of `step` can be either `INTERVAL DAY TO SECOND` or `INTERVAL YEAR TO MONTH`.
:::

:::{function} sequence(start, stop, step) -> array(timestamp)
:noindex: true

Generate a sequence of timestamps from `start` to `stop`, incrementing by `step`.
The type of `step` can be either `INTERVAL DAY TO SECOND` or `INTERVAL YEAR TO MONTH`.
:::

:::{function} shuffle(x) -> array
Generate a random permutation of the given array `x`.
:::

:::{function} slice(x, start, length) -> array
Subsets array `x` starting from index `start` (or starting from the end
if `start` is negative) with a length of `length`.
:::

:::{function} trim_array(x, n) -> array
Remove `n` elements from the end of array:

```
SELECT trim_array(ARRAY[1, 2, 3, 4], 1);
-- [1, 2, 3]

SELECT trim_array(ARRAY[1, 2, 3, 4], 2);
-- [1, 2]
```
:::

:::{function} transform(array(T), function(T,U)) -> array(U)
Returns an array that is the result of applying `function` to each element of `array`:

```
SELECT transform(ARRAY[], x -> x + 1);
-- []

SELECT transform(ARRAY[5, 6], x -> x + 1);
-- [6, 7]

SELECT transform(ARRAY[5, NULL, 6], x -> coalesce(x, 0) + 1);
-- [6, 1, 7]

SELECT transform(ARRAY['x', 'abc', 'z'], x -> x || '0');
-- ['x0', 'abc0', 'z0']

SELECT transform(ARRAY[ARRAY[1, NULL, 2], ARRAY[3, NULL]],
                 a -> filter(a, x -> x IS NOT NULL));
-- [[1, 2], [3]]
```
:::

:::{function} euclidean_distance(array(double), array(double)) -> double
Calculates the euclidean distance:

```sql
SELECT euclidean_distance(ARRAY[1.0, 2.0], ARRAY[3.0, 4.0]);
-- 2.8284271247461903
```
:::

:::{function} dot_product(array(double), array(double)) -> double
Calculates the dot product:

```sql
SELECT dot_product(ARRAY[1.0, 2.0], ARRAY[3.0, 4.0]);
-- 11.0
```
:::

:::{function} zip(array1, array2[, ...]) -> array(row)
Merges the given arrays, element-wise, into a single array of rows. The M-th element of
the N-th argument will be the N-th field of the M-th output element.
If the arguments have an uneven length, missing values are filled with `NULL`.

```
SELECT zip(ARRAY[1, 2], ARRAY['1b', null, '3b']);
-- [ROW(1, '1b'), ROW(2, null), ROW(null, '3b')]
```
:::

:::{function} zip_with(array(T), array(U), function(T,U,R)) -> array(R)
Merges the two given arrays, element-wise, into a single array using `function`.
If one array is shorter, nulls are appended at the end to match the length of the
longer array, before applying `function`.

```
SELECT zip_with(ARRAY[1, 3, 5], ARRAY['a', 'b', 'c'],
                (x, y) -> (y, x));
-- [ROW('a', 1), ROW('b', 3), ROW('c', 5)]

SELECT zip_with(ARRAY[1, 2], ARRAY[3, 4],
                (x, y) -> x + y);
-- [4, 6]

SELECT zip_with(ARRAY['a', 'b', 'c'], ARRAY['d', 'e', 'f'],
                (x, y) -> concat(x, y));
-- ['ad', 'be', 'cf']

SELECT zip_with(ARRAY['a'], ARRAY['d', null, 'f'],
                (x, y) -> coalesce(x, y));
-- ['a', null, 'f']
```
:::
