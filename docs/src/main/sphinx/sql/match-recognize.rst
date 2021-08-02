===============
MATCH_RECOGNIZE
===============

Synopsis
--------

.. code-block:: text

    MATCH_RECOGNIZE (
      [ PARTITION BY column [, ...] ]
      [ ORDER BY column [, ...] ]
      [ MEASURES measure_definition [, ...] ]
      [ rows_per_match ]
      [ AFTER MATCH skip_to ]
      PATTERN ( row_pattern )
      [ SUBSET subset_definition [, ...] ]
      DEFINE variable_definition [, ...]
      )

Description
-----------

The ``MATCH_RECOGNIZE`` clause is an optional subclause of the ``FROM`` clause.
It is used to detect patterns in a set of rows. Patterns of interest are
specified using row pattern syntax based on regular expressions. The input to
pattern matching is a table, a view or a subquery. For each detected match, one
or more rows are returned. They contain requested information about the match.

Row pattern matching is a powerful tool when analyzing complex sequences of
events. The following examples show some of the typical use cases:

- in trade applications, tracking trends or identifying customers with specific
  behavioral patterns

- in shipping applications, tracking packages through all possible valid paths,

- in financial applications, detecting unusual incidents, which might signal
  fraud

Example
-------

In the following example, the pattern describes a V-shape over the
``totalprice`` column. A match is found whenever orders made by a customer
first decrease in price, and then increase past the starting point::

    SELECT * FROM orders MATCH_RECOGNIZE(
         PARTITION BY custkey
         ORDER BY orderdate
         MEASURES
                  A.totalprice AS starting_price,
                  LAST(B.totalprice) AS bottom_price,
                  LAST(U.totalprice) AS top_price
         ONE ROW PER MATCH
         AFTER MATCH SKIP PAST LAST ROW
         PATTERN (A B+ C+ D+)
         SUBSET U = (C, D)
         DEFINE
                  B AS totalprice < PREV(totalprice),
                  C AS totalprice > PREV(totalprice) AND totalprice <= A.totalprice,
                  D AS totalprice > PREV(totalprice)
         )

In the following sections, all subclauses of the ``MATCH_RECOGNIZE`` clause are
explained with this example query.

Partitioning and ordering
-------------------------

.. code-block:: sql

    PARTITION BY custkey

The ``PARTITION BY`` clause allows you to break up the input table into
separate sections, that are independently processed for pattern matching.
Without a partition declaration, the whole input table is used. This behavior
is analogous to the semantics of ``PARTITION BY`` clause in :ref:`window
specification<window_clause>`. In the example, the ``orders`` table is
partitioned by the ``custkey`` value, so that pattern matching is performed for
all orders of a specific customer independently from orders of other
customers.

.. code-block:: sql

    ORDER BY orderdate

The optional ``ORDER BY`` clause is generally useful to allow matching on an
ordered data set. For example, sorting the input by ``orderdate`` allows for
matching on a trend of changes over time.

.. _row_pattern_measures:

Row pattern measures
--------------------

The ``MEASURES`` clause allows to specify what information is retrieved from a
matched sequence of rows.

.. code-block:: text

    MEASURES measure_expression AS measure_name [, ...]

A measure expression is a scalar expression whose value is computed based on a
match. In the example, three row pattern measures are specified:

``A.totalprice AS starting_price`` returns the price in the first row of the
match, which is the only row associated with ``A`` according to the pattern.

``LAST(B.totalprice) AS bottom_price`` returns the lowest price (corresponding
to the bottom of the "V" in the pattern). It is the price in the last row
associated with ``B``, which is the last row of the descending section.

``LAST(U.totalprice) AS top_price`` returns the highest price in the match. It
is the price in the last row associated with ``C`` or ``D``, which is also the
final row of the match.

Measure expressions can refer to the columns of the input table. They also
allow special syntax to combine the input information with the details of the
match (see :ref:`pattern_recognition_expressions`).

Each measure defines an output column of the pattern recognition. The column
can be referenced with the ``measure_name``.

The ``MEASURES`` clause is optional. When no measures are specified, certain
input columns (depending on :ref:`ROWS PER MATCH<rows_per_match>` clause) are
the output of the pattern recognition.

.. _rows_per_match:

Rows per match
--------------

This clause can be used to specify the quantity of output rows. There are two
main options::

    ONE ROW PER MATCH

and

.. code-block:: sql

    ALL ROWS PER MATCH

``ONE ROW PER MATCH`` is the default option. For every match, a single row of
output is produced. Output consists of ``PARTITION BY`` columns and measures.
The output is also produced for empty matches, based on their starting rows.
Rows that are unmatched (that is, neither included in some non-empty match, nor
being the starting row of an empty match), are not included in the output.

For ``ALL ROWS PER MATCH``, every row of a match produces an output row, unless
it is excluded from the output by the :ref:`exclusion_syntax`. Output consists
of ``PARTITION BY`` columns, ``ORDER BY`` columns, measures and remaining
columns from the input table. By default, empty matches are shown and unmatched
rows are skipped, similarly as with the ``ONE ROW PER MATCH`` option. However,
this behavior can be changed by modifiers::

    ALL ROWS PER MATCH SHOW EMPTY MATCHES

shows empty matches and skips unmatched rows, like the default.

.. code-block:: sql

    ALL ROWS PER MATCH OMIT EMPTY MATCHES

excludes empty matches from the output.

.. code-block:: sql

    ALL ROWS PER MATCH WITH UNMATCHED ROWS

shows empty matches and produces additional output row for each unmatched row.

There are special rules for computing row pattern measures for empty matches
and unmatched rows. They are explained in
:ref:`empty_matches_and_unmatched_rows`.

Unmatched rows can only occur when the pattern does not allow an empty match.
Otherwise, they are considered as starting rows of empty matches. The option
``ALL ROWS PER MATCH WITH UNMATCHED ROWS`` is recommended when pattern
recognition is expected to pass all input rows, and it is not certain whether
the pattern allows an empty match.

.. _after_match_skip:

After match skip
----------------

The ``AFTER MATCH SKIP`` clause specifies where pattern matching resumes after
a non-empty match is found.

The default option is::

    AFTER MATCH SKIP PAST LAST ROW

With this option, pattern matching starts from the row after the last row of
the match. Overlapping matches are not detected.

With the following option, pattern matching starts from the second row of the
match::

    AFTER MATCH SKIP TO NEXT ROW

In the example, if a V-shape is detected, further overlapping matches are
found, starting from consecutive rows on the descending slope of the "V".
Skipping to the next row is the default behavior after detecting an empty match
or unmatched row.

The following ``AFTER MATCH SKIP`` options allow to resume pattern matching
based on the components of the pattern. Pattern matching starts from the last
(default) or first row matched to a certain row pattern variable. It can be
either a primary pattern variable (they are explained in
:ref:`row_pattern_syntax`) or a
:ref:`union variable<row_pattern_union_variables>`::

    AFTER MATCH SKIP TO [ FIRST | LAST ] pattern_variable

It is forbidden to skip to the first row of the current match, because it
results in an infinite loop. For example specifying ``AFTER MATCH SKIP TO A``
fails, because ``A`` is the first element of the pattern, and jumping back to
it creates an infinite loop. Similarly, skipping to a pattern variable which is
not present in the match causes failure.

All other options than the default ``AFTER MATCH SKIP PAST LAST ROW`` allow
detection of overlapping matches. The combination of ``ALL ROWS PER MATCH WITH
UNMATCHED ROWS`` with ``AFTER MATCH SKIP PAST LAST ROW`` is the only
configuration that guarantees exactly one output row for each input row.

.. _row_pattern_syntax:

Row pattern syntax
------------------

Row pattern is a form of a regular expression with some syntactical extensions
specific to row pattern recognition. It is specified in the ``PATTERN``
clause::

    PATTERN ( row_pattern )

The basic element of row pattern is a primary pattern variable. Like pattern
matching in character strings searches for characters, pattern matching in row
sequences searches for rows which can be "labeled" with certain primary pattern
variables. A primary pattern variable has a form of an identifier and is
:ref:`defined<row_pattern_variable_definitions>` by a boolean condition. This
condition determines whether a particular input row can be mapped to this
variable and take part in the match.

In the example ``PATTERN (A B+ C+ D+)``, there are four primary pattern
variables: ``A``, ``B``, ``C``, and ``D``.

Row pattern syntax includes the following usage:

concatenation
^^^^^^^^^^^^^

.. code-block:: text

    A B+ C+ D+

It is a sequence of components without operators between them. All components
are matched in the same order as they are specified.

alternation
^^^^^^^^^^^

.. code-block:: text

    A | B | C

It is a sequence of components separated by ``|``. Exactly one of the
components is matched. In case when multiple components can be matched, the
leftmost matching component is chosen.

permutation
^^^^^^^^^^^

.. code-block:: text

    PERMUTE(A, B, C)

It is equivalent to alternation of all permutations of its components. All
components are matched in some order. If multiple matches are possible for
different orderings of the components, the match is chosen based on the
lexicographical order established by the order of components in the ``PERMUTE``
list. In the above example, the most preferred option is ``A B C``, and the
least preferred option is ``C B A``.

grouping
^^^^^^^^

.. code-block:: text

    (A B C)

partition start anchor
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

    ^

partition end anchor
^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

    $

empty pattern
^^^^^^^^^^^^^

.. code-block:: text

    ()

.. _exclusion_syntax:

exclusion syntax
^^^^^^^^^^^^^^^^

.. code-block:: text

    {- row_pattern -}

Exclusion syntax is used to specify portions of the match to exclude from the
output. It is useful in combination with the ``ALL ROWS PER MATCH`` option,
when only certain sections of the match are interesting.

If you change the example to use ``ALL ROWS PER MATCH``, and the pattern is
modified to ``PATTERN (A {- B+ C+ -} D+)``, the result consists of the initial
matched row and the trailing section of rows.

Specifying pattern exclusions does not affect the computation of expressions in
``MEASURES`` and ``DEFINE`` clauses. Exclusions also do not affect pattern
matching. They have the same semantics as regular grouping with parentheses.

It is forbidden to specify pattern exclusions with the option ``ALL ROWS PER
MATCH WITH UNMATCHED ROWS``.

quantifiers
^^^^^^^^^^^

Pattern quantifiers allow to specify the desired number of repetitions of a
sub-pattern in a match. They are appended after the relevant pattern
component::

    (A | B)*

There are following row pattern quantifiers:

- zero or more repetitions:

.. code-block:: text

    *

- one or more repetitions:

.. code-block:: text

    +

- zero or one repetition:

.. code-block:: text

    ?

- exact number of repetitions, specified by a non-negative integer number:

.. code-block:: text

    {n}

- number of repetitions ranging between bounds, specified by non-negative
  integer numbers:

.. code-block:: text

    {m, n}

Specifying bounds is optional. If the left bound is omitted, it defaults to
``0``. So, ``{, 5}`` can be described as "between zero and five repetitions".
If the right bound is omitted, the number of accepted repetitions is unbounded.
So, ``{5, }`` can be described as "at least five repetitions". Also, ``{,}`` is
equivalent to ``*``.

Quantifiers are greedy by default. It means that higher number of repetitions
is preferred over lower number. This behavior can be changed to reluctant by
appending ``?`` immediately after the quantifier. With ``{3, 5}``, 3
repetitions is the least desired option and 5 repetitions -- the most desired.
With ``{3, 5}?``, 3 repetitions are most desired. Similarly, ``?`` prefers 1
repetition, while ``??`` prefers 0 repetitions.

.. _row_pattern_union_variables:

Row pattern union variables
---------------------------

As explained in :ref:`row_pattern_syntax`, primary pattern variables are the
basic elements of row pattern. In addition to primary pattern variables, you
can define union variables. They are introduced in the ``SUBSET`` clause::

    SUBSET U = (C, D), ...

In the preceding example, union variable ``U`` is defined as union of primary
variables ``C`` and ``D``. Union variables are useful in ``MEASURES``,
``DEFINE`` and ``AFTER MATCH SKIP`` clauses. They allow you to refer to set of
rows matched to either primary variable from a subset.

With the pattern: ``PATTERN((A | B){5} C+)`` it cannot be determined upfront if
the match contains any ``A`` or any ``B``. A union variable can be used to
access the last row matched to either ``A`` or ``B``. Define ``SUBSET U =
(A, B)``, and the expression ``LAST(U.totalprice)`` returns the value of the
``totalprice`` column from the last row mapped to either ``A`` or ``B``. Also,
``AFTER MATCH SKIP TO LAST A`` or ``AFTER MATCH SKIP TO LAST B`` can result in
failure if ``A`` or ``B`` is not present in the match. ``AFTER MATCH SKIP TO
LAST U`` does not fail.

.. _row_pattern_variable_definitions:

Row pattern variable definitions
--------------------------------

The ``DEFINE`` clause is where row pattern primary variables are defined. Each
variable is associated with a boolean condition::

    DEFINE B AS totalprice < PREV(totalprice), ...

During pattern matching, when a certain variable is considered for the next
step of the match, the boolean condition is evaluated in context of the current
match. If the result is ``true``, then the current row, "labeled" with the
variable, becomes part of the match.

In the preceding example, assume that the pattern allows to match ``B`` at some
point. There are some rows already matched to some pattern variables. Now,
variable ``B`` is being considered for the current row. Before the match is
made, the defining condition for ``B`` is evaluated. In this example, it is
only true if the value of the ``totalprice`` column in the current row is lower
than ``totalprice`` in the preceding row.

The mechanism of matching variables to rows shows the difference between
pattern matching in row sequences and regular expression matching in text. In
text, characters remain constantly in their positions. In row pattern matching,
a row can be mapped to different variables in different matches, depending on
the preceding part of the match, and even on the match number.

It is not required that every primary variable has a definition in the
``DEFINE`` clause. Variables not mentioned in the ``DEFINE`` clause are
implicitly associated with ``true`` condition, which means that they can be
matched to every row.

Boolean expressions in the ``DEFINE`` clause allow the same special syntax as
expressions in the ``MEASURES`` clause. Details are explained in
:ref:`pattern_recognition_expressions`.

.. _pattern_recognition_expressions:

Row pattern recognition expressions
-----------------------------------

Expressions in :ref:`MEASURES<row_pattern_measures>` and
:ref:`DEFINE<row_pattern_variable_definitions>` clauses are scalar expressions
evaluated over rows of the input table. They support special syntax, specific
to pattern recognition context. They can combine input information with the
information about the current match. Special syntax allows to access pattern
variables assigned to rows, browse rows based on how they are matched, and
refer to the sequential number of the match.

pattern variable references
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sql

    A.totalprice

    U.orderdate

    orderstatus

A column name prefixed with a pattern variable refers to values of this column
in all rows matched to this variable, or to any variable from the subset in
case of union variable. If a column name is not prefixed, it is considered as
prefixed with the ``universal pattern variable``, defined as union of all
primary pattern variables. In other words, a non-prefixed column name refers to
all rows of the current match.

It is forbidden to prefix a column name with a table name in the pattern
recognition context.

classifier function
^^^^^^^^^^^^^^^^^^^

.. code-block:: sql

    CLASSIFIER()

    CLASSIFIER(A)

    CLASSIFIER(U)

The ``classifier`` function returns the primary pattern variable associated
with the row. The return type is ``varchar``. The optional argument is a
pattern variable. It limits the rows of interest, the same way as with prefixed
column references. The ``classifier`` function is particularly useful with a
union variable as the argument. It allows you to determine which variable from
the subset actually matched.

match_number function
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sql

    MATCH_NUMBER()

The ``match_number`` function returns the sequential number of the match within
partition, starting from ``1``. Empty matches are assigned sequential numbers
as well as non-empty matches. The return type is ``bigint``.

logical navigation functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sql

    FIRST(A.totalprice, 2)

In the above example, the ``first`` function navigates to the first row matched
to pattern variable ``A``, and then searches forward until it finds two more
occurrences of variable ``A`` within the match. The result is the value of the
``totalprice`` column in that row.

.. code-block:: sql

    LAST(A.totalprice, 2)

In the above example, the ``last`` function navigates to the last row matched
to pattern variable ``A``, and then searches backwards until it finds two more
occurrences of variable ``A`` within the match. The result is the value of the
``totalprice`` column in that row.

With the ``first`` and ``last`` functions the result is ``null``, if the
searched row is not found in the mach.

The second argument is optional. The default value is ``0``, which means that
by default these functions navigate to the first or last row of interest. If
specified, the second argument must be a non-negative integer number.

physical navigation functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sql

    PREV(A.totalprice, 2)

In the above example, the ``prev`` function navigates to the last row matched
to pattern variable ``A``, and then searches two rows backward. The result is
the value of the ``totalprice`` column in that row.

.. code-block:: sql

    NEXT(A.totalprice, 2)

In the above example, the ``next`` function navigates to the last row matched
to pattern variable ``A``, and then searches two rows forward. The result is
the value of the ``totalprice`` column in that row.

With the ``prev`` and ``next`` functions, it is possible to navigate and
retrieve values outside the match. If the navigation goes beyond partition
bounds, the result is ``null``.

The second argument is optional. The default value is ``1``, which means that
by default these functions navigate to previous or next row. If specified, the
second argument must be a non-negative integer number.

nesting of navigation functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is possible to nest logical navigation functions within physical navigation
functions:

.. code-block:: sql

    PREV(FIRST(A.totalprice, 3), 2)

In case of nesting, first the logical navigation is performed. It establishes
the starting row for the physical navigation. When both navigation operations
succeed, the value is retrieved from the designated row.

Pattern navigation functions require at least one column reference or
``classifier`` function inside of their first argument. The following examples
are correct::

    LAST("pattern_variable_" || CLASSIFIER())

    NEXT(U.totalprice + 10)

This is incorrect::

    LAST(1)

It is also required that all column references and all ``classifier`` calls
inside a pattern navigation function are consistent in referred pattern
variables. They must all refer either to the same primary variable, the same
union variable, or to the implicit universal pattern variable. The following
examples are correct::

    LAST(CLASSIFIER() = 'A' OR totalprice > 10) /* universal pattern variable */

    LAST(CLASSIFIER(U) = 'A' OR U.totalprice > 10) /* pattern variable U */

This is incorrect::

    LAST(A.totalprice + B.totalprice)

``RUNNING`` and ``FINAL`` semantics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

During pattern matching in a sequence of rows, one row after another is
examined to determine if it fits the pattern. At any step, a partial match is
known, but it is not yet known what rows will be added in the future or what
pattern variables they will be mapped to. So, when evaluating a boolean
condition in the ``DEFINE`` clause for the current row, only the preceding part
of the match (plus the current row) is "visible". This is the ``running``
semantics.

When evaluating expressions in the ``MEASURES`` clause, the match is complete.
It is then possible to apply the ``final`` semantics. In the ``final``
semantics, the whole match is "visible" as from the position of the final row.

In the ``MEASURES`` clause, the ``running`` semantics can also be applied. When
outputting information row by row (as in ``ALL ROWS PER MATCH``), the
``running`` semantics evaluate expressions from the positions of consecutive
rows.

The ``running`` and ``final`` semantics are denoted by the keywords:
``RUNNING`` and ``FINAL``, preceding a logical navigation function ``first`` or
``last``::

    RUNNING LAST(A.totalprice)

    FINAL LAST(A.totalprice)

The ``running`` semantics is default in ``MEASURES`` and ``DEFINE`` clauses.
``FINAL`` can only be specified in the ``MEASURES`` clause.

With the option ``ONE ROW PER MATCH``, row pattern measures are evaluated from
the position of the final row in the match. Therefore, ``running`` and
``final`` semantics are the same.

.. _empty_matches_and_unmatched_rows:

Evaluating expressions in empty matches and unmatched rows
----------------------------------------------------------

An empty match occurs when the row pattern is successfully matched, but no
pattern variables are assigned. The following pattern produces an empty match
for every row::

    PATTERN(())

When evaluating row pattern measures for an empty match:

- all column references return ``null``

- all navigation operations return ``null``

- ``classifier`` function returns ``null``

- ``match_number`` function returns the sequential number of the match

Like every match, an empty match has its starting row. All input values which
are to be output along with the measures (as explained in
:ref:`rows_per_match`), are the values from the starting row.

An unmatched row is a row that is neither part of any non-empty match nor the
starting row of an empty match. With the option ``ALL ROWS PER MATCH WITH
UNMATCHED ROWS``, a single output row is produced. In that row, all row pattern
measures are ``null``. All input values which are to be output along with the
measures (as explained in :ref:`rows_per_match`), are the values from the
unmatched row. Using the ``match_number`` function as a measure can help
differentiate between an empty match and unmatched row.

Limitations
-----------

Standard SQL syntax allows you to use aggregate functions inside pattern
recognition expressions. Trino does not support it.
