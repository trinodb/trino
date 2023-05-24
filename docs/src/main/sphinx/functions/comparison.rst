==================================
Comparison functions and operators
==================================

.. _comparison-operators:

Comparison operators
--------------------

======== ===========
Operator Description
======== ===========
``<``    Less than
``>``    Greater than
``<=``   Less than or equal to
``>=``   Greater than or equal to
``=``    Equal
``<>``   Not equal
``!=``   Not equal (non-standard but popular syntax)
======== ===========

.. _range-operator:

Range operator: BETWEEN
-----------------------

The ``BETWEEN`` operator tests if a value is within a specified range.
It uses the syntax ``value BETWEEN min AND max``::

    SELECT 3 BETWEEN 2 AND 6;

The statement shown above is equivalent to the following statement::

    SELECT 3 >= 2 AND 3 <= 6;

To test if a value does not fall within the specified range
use ``NOT BETWEEN``::

    SELECT 3 NOT BETWEEN 2 AND 6;

The statement shown above is equivalent to the following statement::

    SELECT 3 < 2 OR 3 > 6;

A ``NULL`` in a ``BETWEEN`` or ``NOT BETWEEN`` statement is evaluated
using the standard ``NULL`` evaluation rules applied to the equivalent
expression above::

    SELECT NULL BETWEEN 2 AND 4; -- null

    SELECT 2 BETWEEN NULL AND 6; -- null

    SELECT 2 BETWEEN 3 AND NULL; -- false

    SELECT 8 BETWEEN NULL AND 6; -- false

The ``BETWEEN`` and ``NOT BETWEEN`` operators can also be used to
evaluate any orderable type.  For example, a ``VARCHAR``::

    SELECT 'Paul' BETWEEN 'John' AND 'Ringo'; -- true

Note that the value, min, and max parameters to ``BETWEEN`` and ``NOT
BETWEEN`` must be the same type.  For example, Trino will produce an
error if you ask it if John is between 2.3 and 35.2.

.. _is-null-operator:

IS NULL and IS NOT NULL
-----------------------
The ``IS NULL`` and ``IS NOT NULL`` operators test whether a value
is null (undefined).  Both operators work for all data types.

Using ``NULL`` with ``IS NULL`` evaluates to true::

    select NULL IS NULL; -- true

But any other constant does not::

    SELECT 3.0 IS NULL; -- false

.. _is-distinct-operator:

IS DISTINCT FROM and IS NOT DISTINCT FROM
-----------------------------------------

In SQL a ``NULL`` value signifies an unknown value, so any comparison
involving a ``NULL`` will produce ``NULL``.  The  ``IS DISTINCT FROM``
and ``IS NOT DISTINCT FROM`` operators treat ``NULL`` as a known value
and both operators guarantee either a true or false outcome even in
the presence of ``NULL`` input::

    SELECT NULL IS DISTINCT FROM NULL; -- false

    SELECT NULL IS NOT DISTINCT FROM NULL; -- true

In the example shown above, a ``NULL`` value is not considered
distinct from ``NULL``.  When you are comparing values which may
include ``NULL`` use these operators to guarantee either a ``TRUE`` or
``FALSE`` result.

The following truth table demonstrate the handling of ``NULL`` in
``IS DISTINCT FROM`` and ``IS NOT DISTINCT FROM``:

======== ======== ========= ========= ============ ================
a        b        a = b     a <> b    a DISTINCT b a NOT DISTINCT b
======== ======== ========= ========= ============ ================
``1``    ``1``    ``TRUE``  ``FALSE`` ``FALSE``       ``TRUE``
``1``    ``2``    ``FALSE`` ``TRUE``  ``TRUE``        ``FALSE``
``1``    ``NULL`` ``NULL``  ``NULL``  ``TRUE``        ``FALSE``
``NULL`` ``NULL`` ``NULL``  ``NULL``  ``FALSE``       ``TRUE``
======== ======== ========= ========= ============ ================

GREATEST and LEAST
------------------

These functions are not in the SQL standard, but are a common extension.
Like most other functions in Trino, they return null if any argument is
null. Note that in some other databases, such as PostgreSQL, they only
return null if all arguments are null.

The following types are supported:
``DOUBLE``,
``BIGINT``,
``VARCHAR``,
``TIMESTAMP``,
``TIMESTAMP WITH TIME ZONE``,
``DATE``

.. function:: greatest(value1, value2, ..., valueN) -> [same as input]

    Returns the largest of the provided values.

.. function:: least(value1, value2, ..., valueN) -> [same as input]

    Returns the smallest of the provided values.

.. _quantified-comparison-predicates:

Quantified comparison predicates: ALL, ANY and SOME
---------------------------------------------------

The ``ALL``, ``ANY`` and ``SOME`` quantifiers can be used together with comparison operators in the
following way:

.. code-block:: text

    expression operator quantifier ( subquery )

For example::

    SELECT 'hello' = ANY (VALUES 'hello', 'world'); -- true

    SELECT 21 < ALL (VALUES 19, 20, 21); -- false

    SELECT 42 >= SOME (SELECT 41 UNION ALL SELECT 42 UNION ALL SELECT 43); -- true

Here are the meanings of some quantifier and comparison operator combinations:

====================    ===========
Expression              Meaning
====================    ===========
``A = ALL (...)``       Evaluates to ``true`` when ``A`` is equal to all values.
``A <> ALL (...)``      Evaluates to ``true`` when ``A`` doesn't match any value.
``A < ALL (...)``       Evaluates to ``true`` when ``A`` is smaller than the smallest value.
``A = ANY (...)``       Evaluates to ``true`` when ``A`` is equal to any of the values. This form is equivalent to ``A IN (...)``.
``A <> ANY (...)``      Evaluates to ``true`` when ``A`` doesn't match one or more values.
``A < ANY (...)``       Evaluates to ``true`` when ``A`` is smaller than the biggest value.
====================    ===========

``ANY`` and ``SOME`` have the same meaning and can be used interchangeably.

.. _like-operator:

Pattern comparison: LIKE
------------------------

The ``LIKE`` operator can be used to compare values with a pattern::

    ... column [NOT] LIKE 'pattern' ESCAPE 'character';

Matching characters is case sensitive, and the pattern supports two symbols for
matching:

- ``_`` matches any single character
- ``%`` matches zero or more characters

Typically it is often used as a condition in ``WHERE`` statements. An example is
a query to find all continents starting with ``E``, which returns ``Europe``::

    SELECT * FROM (VALUES 'America', 'Asia', 'Africa', 'Europe', 'Australia', 'Antarctica') AS t (continent)
    WHERE continent LIKE 'E%';

You can negate the result by adding ``NOT``, and get all other continents, all
not starting with ``E``::

    SELECT * FROM (VALUES 'America', 'Asia', 'Africa', 'Europe', 'Australia', 'Antarctica') AS t (continent)
    WHERE continent NOT LIKE 'E%';

If you only have one specific character to match, you can use the ``_`` symbol
for each character. The following query uses two underscores and produces only
``Asia`` as result::

    SELECT * FROM (VALUES 'America', 'Asia', 'Africa', 'Europe', 'Australia', 'Antarctica') AS t (continent)
    WHERE continent LIKE 'A__A';

The wildcard characters ``_`` and ``%`` must be escaped to allow you to match
them as literals. This can be achieved by specifying the ``ESCAPE`` character to
use::

    SELECT 'South_America' LIKE 'South\_America' ESCAPE '\';

The above query returns ``true`` since the escaped underscore symbol matches. If
you need to match the used escape character as well, you can escape it.

If you want to match for the chosen escape character, you simply escape itself.
For example, you can use ``\\`` to match for ''\''.
