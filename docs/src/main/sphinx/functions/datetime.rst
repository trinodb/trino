=====================================
Date and time functions and operators
=====================================

These functions and operators operate on :ref:`date and time data types <date-time-data-types>`.

Date and time operators
-----------------------

======== ===================================================== ===========================
Operator Example                                               Result
======== ===================================================== ===========================
``+``    ``date '2012-08-08' + interval '2' day``              ``2012-08-10``
``+``    ``time '01:00' + interval '3' hour``                  ``04:00:00.000``
``+``    ``timestamp '2012-08-08 01:00' + interval '29' hour`` ``2012-08-09 06:00:00.000``
``+``    ``timestamp '2012-10-31 01:00' + interval '1' month`` ``2012-11-30 01:00:00.000``
``+``    ``interval '2' day + interval '3' hour``              ``2 03:00:00.000``
``+``    ``interval '3' year + interval '5' month``            ``3-5``
``-``    ``date '2012-08-08' - interval '2' day``              ``2012-08-06``
``-``    ``time '01:00' - interval '3' hour``                  ``22:00:00.000``
``-``    ``timestamp '2012-08-08 01:00' - interval '29' hour`` ``2012-08-06 20:00:00.000``
``-``    ``timestamp '2012-10-31 01:00' - interval '1' month`` ``2012-09-30 01:00:00.000``
``-``    ``interval '2' day - interval '3' hour``              ``1 21:00:00.000``
``-``    ``interval '3' year - interval '5' month``            ``2-7``
======== ===================================================== ===========================

.. _at-time-zone-operator:

Time zone conversion
--------------------

The ``AT TIME ZONE`` operator sets the time zone of a timestamp::

    SELECT timestamp '2012-10-31 01:00 UTC';
    -- 2012-10-31 01:00:00.000 UTC

    SELECT timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles';
    -- 2012-10-30 18:00:00.000 America/Los_Angeles

Date and time functions
-----------------------

.. data:: current_date

    Returns the current date as of the start of the query.

.. data:: current_time

    Returns the current time with time zone as of the start of the query.

.. data:: current_timestamp

    Returns the current timestamp with time zone as of the start of the query,
    with ``3`` digits of subsecond precision,

.. data:: current_timestamp(p)
    :noindex:

    Returns the current :ref:`timestamp with time zone
    <timestamp-with-time-zone-data-type>` as of the start of the query, with
    ``p`` digits of subsecond precision::

        SELECT current_timestamp(6);
        -- 2020-06-24 08:25:31.759993 America/Los_Angeles

.. function:: current_timezone() -> varchar

    Returns the current time zone in the format defined by IANA
    (e.g., ``America/Los_Angeles``) or as fixed offset from UTC (e.g., ``+08:35``)

.. function:: date(x) -> date

    This is an alias for ``CAST(x AS date)``.

.. function:: last_day_of_month(x) -> date

    Returns the last day of the month.

.. function:: from_iso8601_timestamp(string) -> timestamp(3) with time zone

    Parses the ISO 8601 formatted date ``string``, optionally with time and time
    zone, into a ``timestamp(3) with time zone``. The time defaults to
    ``00:00:00.000``, and the time zone defaults to the session time zone::

        SELECT from_iso8601_timestamp('2020-05-11');
        -- 2020-05-11 00:00:00.000 America/Vancouver

        SELECT from_iso8601_timestamp('2020-05-11T11:15:05');
        -- 2020-05-11 11:15:05.000 America/Vancouver

        SELECT from_iso8601_timestamp('2020-05-11T11:15:05.055+01:00');
        -- 2020-05-11 11:15:05.055 +01:00

.. function:: from_iso8601_timestamp_nanos(string) -> timestamp(9) with time zone

    Parses the ISO 8601 formatted date and time ``string``. The time zone
    defaults to the session time zone::

        SELECT from_iso8601_timestamp_nanos('2020-05-11T11:15:05');
        -- 2020-05-11 11:15:05.000000000 America/Vancouver

        SELECT from_iso8601_timestamp_nanos('2020-05-11T11:15:05.123456789+01:00');
        -- 2020-05-11 11:15:05.123456789 +01:00

.. function:: from_iso8601_date(string) -> date

    Parses the ISO 8601 formatted date ``string`` into a ``date``. The date can
    be a calendar date, a week date using ISO week numbering, or year and day
    of year combined::

        SELECT from_iso8601_date('2020-05-11');
        -- 2020-05-11

        SELECT from_iso8601_date('2020-W10');
        -- 2020-03-02

        SELECT from_iso8601_date('2020-123');
        -- 2020-05-02

.. function:: at_timezone(timestamp(p), zone) -> timestamp(p) with time zone

    Returns the timestamp specified in ``timestamp`` with the time zone
    converted from the session time zone to the time zone specified in ``zone``
    with precision ``p``. In the following example, the session time zone is set
    to ``America/New_York``, which is three hours ahead of
    ``America/Los_Angeles``::

        SELECT current_timezone()
        -- America/New_York

        SELECT at_timezone(TIMESTAMP '2022-11-01 09:08:07.321', 'America/Los_Angeles')
        -- 2022-11-01 06:08:07.321 America/Los_Angeles

.. function:: with_timezone(timestamp(p), zone) -> timestamp(p) with time zone

    Returns the timestamp specified in ``timestamp`` with the time zone
    specified in ``zone`` with precision ``p``::

        SELECT current_timezone()
        -- America/New_York

        SELECT with_timezone(TIMESTAMP '2022-11-01 09:08:07.321', 'America/Los_Angeles')
        -- 2022-11-01 09:08:07.321 America/Los_Angeles

.. function:: from_unixtime(unixtime) -> timestamp(3) with time zone

    Returns the UNIX timestamp ``unixtime`` as a timestamp with time zone. ``unixtime`` is the
    number of seconds since ``1970-01-01 00:00:00 UTC``.

.. function:: from_unixtime(unixtime, zone) -> timestamp(3) with time zone
    :noindex:

    Returns the UNIX timestamp ``unixtime`` as a timestamp with time zone
    using ``zone`` for the time zone. ``unixtime`` is the number of seconds
    since ``1970-01-01 00:00:00 UTC``.

.. function:: from_unixtime(unixtime, hours, minutes) -> timestamp(3) with time zone
    :noindex:

    Returns the UNIX timestamp ``unixtime`` as a timestamp with time zone
    using ``hours`` and ``minutes`` for the time zone offset. ``unixtime`` is
    the number of seconds since ``1970-01-01 00:00:00`` in ``double`` data type.

.. function:: from_unixtime_nanos(unixtime) -> timestamp(9) with time zone

    Returns the UNIX timestamp ``unixtime`` as a timestamp with time zone. ``unixtime`` is the
    number of nanoseconds since ``1970-01-01 00:00:00.000000000 UTC``::

        SELECT from_unixtime_nanos(100);
        -- 1970-01-01 00:00:00.000000100 UTC

        SELECT from_unixtime_nanos(DECIMAL '1234');
        -- 1970-01-01 00:00:00.000001234 UTC

        SELECT from_unixtime_nanos(DECIMAL '1234.499');
        -- 1970-01-01 00:00:00.000001234 UTC

        SELECT from_unixtime_nanos(DECIMAL '-1234');
        -- 1969-12-31 23:59:59.999998766 UTC

.. data:: localtime

    Returns the current time as of the start of the query.

.. data:: localtimestamp

    Returns the current timestamp as of the start of the query, with ``3``
    digits of subsecond precision.

.. data:: localtimestamp(p)
    :noindex:

    Returns the current :ref:`timestamp <timestamp-data-type>` as of the start
    of the query, with ``p`` digits of subsecond precision::

        SELECT localtimestamp(6);
        -- 2020-06-10 15:55:23.383628

.. function:: now() -> timestamp(3) with time zone

    This is an alias for ``current_timestamp``.

.. function:: to_iso8601(x) -> varchar

    Formats ``x`` as an ISO 8601 string. ``x`` can be date, timestamp, or
    timestamp with time zone.

.. function:: to_milliseconds(interval) -> bigint

    Returns the day-to-second ``interval`` as milliseconds.

.. function:: to_unixtime(timestamp) -> double

    Returns ``timestamp`` as a UNIX timestamp.

.. note:: The following SQL-standard functions do not use parenthesis:

    - ``current_date``
    - ``current_time``
    - ``current_timestamp``
    - ``localtime``
    - ``localtimestamp``

Truncation function
-------------------

The ``date_trunc`` function supports the following units:

=========== ===========================
Unit        Example Truncated Value
=========== ===========================
``second``  ``2001-08-22 03:04:05.000``
``minute``  ``2001-08-22 03:04:00.000``
``hour``    ``2001-08-22 03:00:00.000``
``day``     ``2001-08-22 00:00:00.000``
``week``    ``2001-08-20 00:00:00.000``
``month``   ``2001-08-01 00:00:00.000``
``quarter`` ``2001-07-01 00:00:00.000``
``year``    ``2001-01-01 00:00:00.000``
=========== ===========================

The above examples use the timestamp ``2001-08-22 03:04:05.321`` as the input.

.. function:: date_trunc(unit, x) -> [same as input]

    Returns ``x`` truncated to ``unit``::

        SELECT date_trunc('day' , TIMESTAMP '2022-10-20 05:10:00');
        -- 2022-10-20 00:00:00.000

        SELECT date_trunc('month' , TIMESTAMP '2022-10-20 05:10:00');
        -- 2022-10-01 00:00:00.000

        SELECT date_trunc('year', TIMESTAMP '2022-10-20 05:10:00');
        -- 2022-01-01 00:00:00.000

.. _datetime-interval-functions:

Interval functions
------------------

The functions in this section support the following interval units:

================= ==================
Unit              Description
================= ==================
``millisecond``   Milliseconds
``second``        Seconds
``minute``        Minutes
``hour``          Hours
``day``           Days
``week``          Weeks
``month``         Months
``quarter``       Quarters of a year
``year``          Years
================= ==================

.. function:: date_add(unit, value, timestamp) -> [same as input]

    Adds an interval ``value`` of type ``unit`` to ``timestamp``.
    Subtraction can be performed by using a negative value::

        SELECT date_add('second', 86, TIMESTAMP '2020-03-01 00:00:00');
        -- 2020-03-01 00:01:26.000

        SELECT date_add('hour', 9, TIMESTAMP '2020-03-01 00:00:00');
        -- 2020-03-01 09:00:00.000

        SELECT date_add('day', -1, TIMESTAMP '2020-03-01 00:00:00 UTC');
        -- 2020-02-29 00:00:00.000 UTC

.. function:: date_diff(unit, timestamp1, timestamp2) -> bigint

    Returns ``timestamp2 - timestamp1`` expressed in terms of ``unit``::

        SELECT date_diff('second', TIMESTAMP '2020-03-01 00:00:00', TIMESTAMP '2020-03-02 00:00:00');
        -- 86400

        SELECT date_diff('hour', TIMESTAMP '2020-03-01 00:00:00 UTC', TIMESTAMP '2020-03-02 00:00:00 UTC');
        -- 24

        SELECT date_diff('day', DATE '2020-03-01', DATE '2020-03-02');
        -- 1

        SELECT date_diff('second', TIMESTAMP '2020-06-01 12:30:45.000000000', TIMESTAMP '2020-06-02 12:30:45.123456789');
        -- 86400

        SELECT date_diff('millisecond', TIMESTAMP '2020-06-01 12:30:45.000000000', TIMESTAMP '2020-06-02 12:30:45.123456789');
        -- 86400123

Duration function
-----------------

The ``parse_duration`` function supports the following units:

======= =============
Unit    Description
======= =============
``ns``  Nanoseconds
``us``  Microseconds
``ms``  Milliseconds
``s``   Seconds
``m``   Minutes
``h``   Hours
``d``   Days
======= =============

.. function:: parse_duration(string) -> interval

    Parses ``string`` of format ``value unit`` into an interval, where
    ``value`` is fractional number of ``unit`` values::

        SELECT parse_duration('42.8ms');
        -- 0 00:00:00.043

        SELECT parse_duration('3.81 d');
        -- 3 19:26:24.000

        SELECT parse_duration('5m');
        -- 0 00:05:00.000

.. function:: human_readable_seconds(double) -> varchar

    Formats the double value of ``seconds`` into a human readable string containing
    ``weeks``, ``days``, ``hours``, ``minutes``, and ``seconds``::

        SELECT human_readable_seconds(96);
        -- 1 minute, 36 seconds

        SELECT human_readable_seconds(3762);
        -- 1 hour, 2 minutes, 42 seconds

        SELECT human_readable_seconds(56363463);
        -- 93 weeks, 1 day, 8 hours, 31 minutes, 3 seconds

MySQL date functions
--------------------

The functions in this section use a format string that is compatible with
the MySQL ``date_parse`` and ``str_to_date`` functions. The following table,
based on the MySQL manual, describes the format specifiers:

========= ===========
Specifier Description
========= ===========
``%a``    Abbreviated weekday name (``Sun`` .. ``Sat``)
``%b``    Abbreviated month name (``Jan`` .. ``Dec``)
``%c``    Month, numeric (``1`` .. ``12``) [#z]_
``%D``    Day of the month with English suffix (``0th``, ``1st``, ``2nd``, ``3rd``, ...)
``%d``    Day of the month, numeric (``01`` .. ``31``) [#z]_
``%e``    Day of the month, numeric (``1`` .. ``31``) [#z]_
``%f``    Fraction of second (6 digits for printing: ``000000`` .. ``999000``; 1 - 9 digits for parsing: ``0`` .. ``999999999``) [#f]_
``%H``    Hour (``00`` .. ``23``)
``%h``    Hour (``01`` .. ``12``)
``%I``    Hour (``01`` .. ``12``)
``%i``    Minutes, numeric (``00`` .. ``59``)
``%j``    Day of year (``001`` .. ``366``)
``%k``    Hour (``0`` .. ``23``)
``%l``    Hour (``1`` .. ``12``)
``%M``    Month name (``January`` .. ``December``)
``%m``    Month, numeric (``01`` .. ``12``) [#z]_
``%p``    ``AM`` or ``PM``
``%r``    Time of day, 12-hour (equivalent to ``%h:%i:%s %p``)
``%S``    Seconds (``00`` .. ``59``)
``%s``    Seconds (``00`` .. ``59``)
``%T``    Time of day, 24-hour (equivalent to ``%H:%i:%s``)
``%U``    Week (``00`` .. ``53``), where Sunday is the first day of the week
``%u``    Week (``00`` .. ``53``), where Monday is the first day of the week
``%V``    Week (``01`` .. ``53``), where Sunday is the first day of the week; used with ``%X``
``%v``    Week (``01`` .. ``53``), where Monday is the first day of the week; used with ``%x``
``%W``    Weekday name (``Sunday`` .. ``Saturday``)
``%w``    Day of the week (``0`` .. ``6``), where Sunday is the first day of the week [#w]_
``%X``    Year for the week where Sunday is the first day of the week, numeric, four digits; used with ``%V``
``%x``    Year for the week, where Monday is the first day of the week, numeric, four digits; used with ``%v``
``%Y``    Year, numeric, four digits
``%y``    Year, numeric (two digits) [#y]_
``%%``    A literal ``%`` character
``%x``    ``x``, for any ``x`` not listed above
========= ===========

.. [#f] Timestamp is truncated to milliseconds.
.. [#y] When parsing, two-digit year format assumes range ``1970`` .. ``2069``, so "70" will result in year ``1970`` but "69" will produce ``2069``.
.. [#w] This specifier is not supported yet. Consider using :func:`day_of_week` (it uses ``1-7`` instead of ``0-6``).
.. [#z] This specifier does not support ``0`` as a month or day.

.. warning:: The following specifiers are not currently supported: ``%D %U %u %V %w %X``

.. function:: date_format(timestamp, format) -> varchar

    Formats ``timestamp`` as a string using ``format``::

        SELECT date_format(TIMESTAMP '2022-10-20 05:10:00', '%m-%d-%Y %H');
        -- 10-20-2022 05

.. function:: date_parse(string, format) -> timestamp(3)

    Parses ``string`` into a timestamp using ``format``::

        SELECT date_parse('2022/10/20/05', '%Y/%m/%d/%H');
        -- 2022-10-20 05:00:00.000

Java date functions
-------------------

The functions in this section use a format string that is compatible with
JodaTime's `DateTimeFormat`_ pattern format.

.. _DateTimeFormat: http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html

.. function:: format_datetime(timestamp, format) -> varchar

    Formats ``timestamp`` as a string using ``format``.

.. function:: parse_datetime(string, format) -> timestamp with time zone

    Parses ``string`` into a timestamp with time zone using ``format``.

Extraction function
-------------------

The ``extract`` function supports the following fields:

=================== ===========
Field               Description
=================== ===========
``YEAR``            :func:`year`
``QUARTER``         :func:`quarter`
``MONTH``           :func:`month`
``WEEK``            :func:`week`
``DAY``             :func:`day`
``DAY_OF_MONTH``    :func:`day`
``DAY_OF_WEEK``     :func:`day_of_week`
``DOW``             :func:`day_of_week`
``DAY_OF_YEAR``     :func:`day_of_year`
``DOY``             :func:`day_of_year`
``YEAR_OF_WEEK``    :func:`year_of_week`
``YOW``             :func:`year_of_week`
``HOUR``            :func:`hour`
``MINUTE``          :func:`minute`
``SECOND``          :func:`second`
``TIMEZONE_HOUR``   :func:`timezone_hour`
``TIMEZONE_MINUTE`` :func:`timezone_minute`
=================== ===========

The types supported by the ``extract`` function vary depending on the
field to be extracted. Most fields support all date and time types.

.. function:: extract(field FROM x) -> bigint

    Returns ``field`` from ``x``::

        SELECT extract(YEAR FROM TIMESTAMP '2022-10-20 05:10:00');
        -- 2022

    .. note:: This SQL-standard function uses special syntax for specifying the arguments.

Convenience extraction functions
--------------------------------

.. function:: day(x) -> bigint

    Returns the day of the month from ``x``.

.. function:: day_of_month(x) -> bigint

    This is an alias for :func:`day`.

.. function:: day_of_week(x) -> bigint

    Returns the ISO day of the week from ``x``.
    The value ranges from ``1`` (Monday) to ``7`` (Sunday).

.. function:: day_of_year(x) -> bigint

    Returns the day of the year from ``x``.
    The value ranges from ``1`` to ``366``.

.. function:: dow(x) -> bigint

    This is an alias for :func:`day_of_week`.

.. function:: doy(x) -> bigint

    This is an alias for :func:`day_of_year`.

.. function:: hour(x) -> bigint

    Returns the hour of the day from ``x``.
    The value ranges from ``0`` to ``23``.

.. function:: millisecond(x) -> bigint

    Returns the millisecond of the second from ``x``.

.. function:: minute(x) -> bigint

    Returns the minute of the hour from ``x``.

.. function:: month(x) -> bigint

    Returns the month of the year from ``x``.

.. function:: quarter(x) -> bigint

    Returns the quarter of the year from ``x``.
    The value ranges from ``1`` to ``4``.

.. function:: second(x) -> bigint

    Returns the second of the minute from ``x``.

.. function:: timezone_hour(timestamp) -> bigint

    Returns the hour of the time zone offset from ``timestamp``.

.. function:: timezone_minute(timestamp) -> bigint

    Returns the minute of the time zone offset from ``timestamp``.

.. function:: week(x) -> bigint

    Returns the `ISO week`_ of the year from ``x``.
    The value ranges from ``1`` to ``53``.

    .. _ISO week: https://wikipedia.org/wiki/ISO_week_date

.. function:: week_of_year(x) -> bigint

    This is an alias for :func:`week`.

.. function:: year(x) -> bigint

    Returns the year from ``x``.

.. function:: year_of_week(x) -> bigint

    Returns the year of the `ISO week`_ from ``x``.

.. function:: yow(x) -> bigint

    This is an alias for :func:`year_of_week`.
