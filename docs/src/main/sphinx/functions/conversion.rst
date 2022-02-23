====================
Conversion functions
====================

Trino will implicitly convert numeric and character values to the
correct type if such a conversion is possible. Trino will not convert
between character and numeric types. For example, a query that expects
a varchar will not automatically convert a bigint value to an
equivalent varchar.

When necessary, values can be explicitly cast to a particular type.

Conversion functions
--------------------

.. function:: cast(value AS type) -> type

    Explicitly cast a value as a type. This can be used to cast a
    varchar to a numeric value type and vice versa.

.. function:: try_cast(value AS type) -> type

    Like :func:`cast`, but returns null if the cast fails.

Formatting
----------

.. function:: format(format, args...) -> varchar

    Returns a formatted string using the specified `format string
    <https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Formatter.html#syntax>`_
    and arguments::

        SELECT format('%s%%', 123);
        -- '123%'

        SELECT format('%.5f', pi());
        -- '3.14159'

        SELECT format('%03d', 8);
        -- '008'

        SELECT format('%,.2f', 1234567.89);
        -- '1,234,567.89'

        SELECT format('%-7s,%7s', 'hello', 'world');
        -- 'hello  ,  world'

        SELECT format('%2$s %3$s %1$s', 'a', 'b', 'c');
        -- 'b c a'

        SELECT format('%1$tA, %1$tB %1$te, %1$tY', date '2006-07-04');
        -- 'Tuesday, July 4, 2006'

.. function:: format_number(number) -> varchar

    Returns a formatted string using a unit symbol::

        SELECT format_number(123456); -- '123K'
        SELECT format_number(1000000); -- '1M'

Data size
---------

The ``format_data_size`` and ``parse_data_size`` functions supports the following units:

======= ============= ==============
Unit    Description   Value
======= ============= ==============
``B``   Bytes         1
``kB``  Kilobytes     1024
``MB``  Megabytes     1024\ :sup:`2`
``GB``  Gigabytes     1024\ :sup:`3`
``TB``  Terabytes     1024\ :sup:`4`
``PB``  Petabytes     1024\ :sup:`5`
``EB``  Exabytes      1024\ :sup:`6`
``ZB``  Zettabytes    1024\ :sup:`7`
``YB``  Yottabytes    1024\ :sup:`8`
======= ============= ==============

The ``format_data_size_decimal`` and ``parse_data_size_decimal`` functions supports the following units:

======= ============= ==============
Unit    Description   Value
======= ============= ==============
``B``   Bytes         1
``kB``  Kilobytes     1000
``MB``  Megabytes     1000\ :sup:`2`
``GB``  Gigabytes     1000\ :sup:`3`
``TB``  Terabytes     1000\ :sup:`4`
``PB``  Petabytes     1000\ :sup:`5`
``EB``  Exabytes      1000\ :sup:`6`
``ZB``  Zettabytes    1000\ :sup:`7`
``YB``  Yottabytes    1000\ :sup:`8`
======= ============= ==============

.. function:: format_data_size(number) -> varchar

    Returns a formatted string using a unit symbol::

        SELECT format_data_size(123456); -- '121kB'
        SELECT format_data_size(1048576); -- '1MB'

.. function:: format_data_size_decimal(number) -> varchar

    Returns a formatted string using a unit symbol::

        SELECT format_data_size_decimal(123456); -- '123kB'
        SELECT format_data_size_decimal(1048576); -- '1.05MB'

.. function:: parse_data_size(string) -> decimal(38)

    Parses ``string`` of format ``value unit`` into a number, where
    ``value`` is the fractional number of ``unit`` values::

        SELECT parse_data_size('1B'); -- 1
        SELECT parse_data_size('1kB'); -- 1024
        SELECT parse_data_size('1MB'); -- 1048576
        SELECT parse_data_size('2.3MB'); -- 2411724

.. function:: parse_data_size_decimal(string) -> decimal(38)

    Parses ``string`` of format ``value unit`` into a number, where
    ``value`` is the fractional number of ``unit`` values::

        SELECT parse_data_size_decimal('1B'); -- 1
        SELECT parse_data_size_decimal('1kB'); -- 1000
        SELECT parse_data_size_decimal('1MB'); -- 1000000
        SELECT parse_data_size_decimal('2.3MB'); -- 2300000

Miscellaneous
-------------

.. function:: typeof(expr) -> varchar

    Returns the name of the type of the provided expression::

        SELECT typeof(123); -- integer
        SELECT typeof('cat'); -- varchar(3)
        SELECT typeof(cos(2) + 1.5); -- double
