=================================
Row pattern recognition in window
=================================

A window structure can be defined in two ways: in the ``WINDOW`` clause or in
the ``OVER`` clause of a window operation. In both cases, the window
specification can include row pattern recognition clauses. They are part of the
window frame. The syntax and semantics of row pattern recognition in window
are similar to those of the
:doc:`MATCH_RECOGNIZE</sql/match-recognize>` clause. This section explains the
details of pattern recognition in window, and highlights the similarities and
the differences between both pattern recognition mechanisms.

Window with row pattern recognition: synopsis
---------------------------------------------

.. code-block:: text

    window_specification:

        (
        [ existing_window_name ]
        [ PARTITION BY column [, ...] ]
        [ ORDER BY column [, ...] ]
        [ window_frame ]
        )

    window_frame:

        [ MEASURES measure_definition [, ...] ]
        frame_extent
        [ AFTER MATCH skip_to ]
        [ INITIAL | SEEK ]
        [ PATTERN ( row_pattern ) ]
        [ SUBSET subset_definition [, ...] ]
        [ DEFINE variable_definition [, ...] ]

Generally, a window frame specifies the ``frame_extent``, which defines the
"sliding window" of rows to be processed by a window function. It can be
defined in terms of ``ROWS``, ``RANGE`` or ``GROUPS``.

A window frame with row pattern recognition involves many other syntactical
components, mandatory or optional, and enforces certain limitations on the
``frame_extent``.

.. code-block:: text

    window frame with pattern recognition:

        [ MEASURES measure_definition [, ...] ]
        ROWS BETWEEN CURRENT ROW AND frame_end
        [ AFTER MATCH skip_to ]
        [ INITIAL | SEEK ]
        PATTERN ( row_pattern )
        [ SUBSET subset_definition [, ...] ]
        DEFINE variable_definition [, ...]

Description of the pattern recognition clauses
----------------------------------------------

The ``frame_extent`` with row pattern recognition must be defined in terms of
``ROWS``. The frame start must be at the ``CURRENT ROW``, which limits the
allowed frame extent values to the following::

    ROWS BETWEEN CURRENT ROW AND CURRENT ROW

    ROWS BETWEEN CURRENT ROW AND <expression> FOLLOWING

    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING

For every input row processed by the window, the portion of rows enclosed by
the ``frame_extent`` limits the search area for row pattern recognition. Unlike
in ``MATCH_RECOGNIZE``, where the pattern search can explore all rows until the
partition end, and all rows of the partition are available for computations, in
window the pattern matching can neither match rows nor retrieve input values
outside the frame.

Besides the ``frame_extent``, the two inevitable clauses for pattern matching
are ``PATTERN`` and ``DEFINE``.

The ``PATTERN`` clause specifies a row pattern which is a form of a regular
expression with some syntactical extensions. The row pattern syntax is similar
to the :ref:`row pattern syntax in MATCH_RECOGNIZE<row_pattern_syntax>`.
However, the anchor pattern: ``^``, ``$`` is not allowed in window.

The ``DEFINE`` clause is where row pattern primary variables are defined by
boolean conditions. It is similar to the
:ref:`DEFINE clause of MATCH_RECOGNIZE<row_pattern_variable_definitions>`.
The only difference is that the window syntax does not support the
``MATCH_NUMBER`` function.

The ``MEASURES`` clause is syntactically similar to the
:ref:`MEASURES clause of MATCH_RECOGNIZE<row_pattern_measures>`, the only
limitation being the usage of the ``MATCH_NUMBER`` function. However, the
semantics of this clause differs between ``MATCH_RECOGNIZE`` and window.
While in ``MATCH_RECOGNIZE`` every measure produces an output column, the
measures in window should be considered as **definitions** associated with the
window structure. They can be called over the window, in the same manner as
regular window functions::

    SELECT cust_key, value OVER w, label OVER w
        FROM orders
        WINDOW w AS (
                     PARTITION BY cust_key
                     ORDER BY order_date
                     MEASURES
                            RUNNING LAST(total_price) AS value,
                            CLASSIFIER() AS label
                     ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                     PATTERN (A B+ C+)
                     DEFINE
                            B AS B.value < PREV (B.value),
                            C AS C.value > PREV (C.value)
                    )

Measures defined in a window can be called in the ``SELECT`` clause and in the
``ORDER BY`` clause of the enclosing query. The ``RUNNING`` and ``FINAL``
keywords are allowed, however they have no effect, since the semantics of
the computations is effectively final.

The ``AFTER MATCH SKIP`` clause has the same syntax as the
:ref:`AFTER MATCH SKIP clause of MATCH_RECOGNIZE<after_match_skip>`.

The ``INITIAL`` or ``SEEK`` modifier is specific to row pattern recognition in
window. With ``INITIAL``, which is the default, the pattern match for an input
row can only be found starting from that row. With ``SEEK``, if there is no
match starting from the current row, the engine tries to find a match starting
from subsequent rows within the frame. As a result, it is possible to associate
an input row with a match which is detached from that row.

The ``SUBSET`` clause is used to define union variables as sets of primary
pattern variables. The syntax and semantics are the same as the
:ref:`SUBSET clause of MATCH_RECOGNIZE<row_pattern_union_variables>`

In window, unlike in ``MATCH_RECOGNIZE``, you cannot specify ``ONE ROW PER
MATCH`` or ``ALL ROWS PER MATCH``. This is because all calls over window,
whether they are regular window functions or measures, must comply with the
window semantics. A call over window is supposed to produce exactly one output
row for every input row. And so, the output mode of pattern recognition in
window is a combination of ``ONE ROW PER MATCH`` and ``WITH UNMATCHED ROWS``.

Processing input with row pattern recognition
---------------------------------------------

Pattern recognition in window processes input rows in two different cases:

* upon a row pattern measure call over the window::

    some_measure OVER w

* upon a window function call over the window::

    sum(total_price) OVER w

The output row produced for each input row, consists of:

* all values from the input row
* the value of the called measure or window function, computed with respect to
  the pattern match associated with the row

Processing the input can be described as the following sequence of steps:

.. code-block:: text

    * partition the input data accordingly to PARTITION BY
    * order each partition by the ORDER BY expressions
    * for every row of the ordered partition:
        If the row is 'skipped' by a match of some previous row,
            * for a measure, produce a one-row output as for an unmatched row,
            * for a window function, evaluate the function over an empty frame
              and produce a one-row output.
        Otherwise:
            * determine the frame extent
            * try match the row pattern starting from the current row within
              the frame extent
            * if no match is found, and SEEK is specified, try to find a match
              starting from subsequent rows within the frame extent
            If no match is found,
                * for a measure, produce a one-row output for an unmatched row,
                * for a window function, evaluate the function over an empty
                  frame and produce a one-row output.
            Otherwise:
                * for a measure, produce a one-row output for the match,
                * for a window function, evaluate the function over a frame
                  limited to the matched rows sequence and produce a one-row
                  output.
                * evaluate the AFTER MATCH SKIP clause, and mark the 'skipped'
                  rows

Empty matches and unmatched rows
--------------------------------

If no match can be associated with a particular input row, the row is
*unmatched*. It happens when no match can be found for the row, and also in the
case when no match is attempted for the row, because it is ``skipped`` by the
``AFTER MATCH SKIP`` clause of some preceding row. For an unmatched row, every
row pattern measure is ``null``. Every window function is evaluated over an
empty frame.

An *empty match* is a successful match which does not involve any pattern
variables. In other words, an empty match does not contain any rows. If an
empty match is associated with an input row, every row pattern measure for that
row is evaluated over an empty sequence of rows. All navigation operations and
``CLASSIFIER`` function return ``null``. Every window function is evaluated
over an empty frame.

In most cases, the results for empty matches and unmatched rows are the same.
A constant measure can be helpful to distinguish between them:

The call::

    matched OVER (
                  ...
                  MEASURES 'matched' AS matched
                  ...
                 )

returns ``'matched'`` for every matched row, including empty matches, and
``null`` for every unmatched row.

Limitations
-----------

Standard SQL syntax allows you to use aggregate functions inside pattern
recognition expressions. Trino does not support it.
