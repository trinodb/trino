=========================================
List of functions and operators per topic
=========================================

Aggregate
---------

Details in :doc:`aggregate`.

- :func:`arbitrary`
- :func:`approx_distinct`
- :func:`approx_most_frequent`
- :func:`approx_percentile`
- :func:`approx_set`
- :func:`array_agg`
- :func:`avg`
- :func:`bitwise_and_agg`
- :func:`bitwise_or_agg`
- :func:`bool_and`
- :func:`bool_or`
- :func:`checksum`
- :func:`corr`
- :func:`count`
- :func:`count_if`
- :func:`covar_pop`
- :func:`covar_samp`
- :func:`every`
- :func:`geometric_mean`
- :func:`histogram`
- :func:`kurtosis`
- :func:`map_agg`
- :func:`map_union`
- :func:`max`
- :func:`max_by`
- :func:`merge`
- :func:`min`
- :func:`min_by`
- :func:`multimap_agg`
- :func:`numeric_histogram`
- :func:`qdigest_agg`
- :func:`regr_intercept`
- :func:`regr_slope`
- :func:`skewness`
- :func:`sum`
- :func:`stddev`
- :func:`stddev_pop`
- :func:`stddev_samp`
- :func:`tdigest`
- :func:`variance`
- :func:`var_pop`
- :func:`var_samp`

Array
-----

Details in :doc:`array`.

- :func:`all_match`
- :func:`any_match`
- :func:`array_distinct`
- :func:`array_intersect`
- :func:`array_union`
- :func:`array_except`
- :func:`array_join`
- :func:`array_max`
- :func:`array_min`
- :func:`array_position`
- :func:`array_remove`
- :func:`array_sort`
- :func:`arrays_overlap`
- :func:`cardinality`
- :func:`concat`
- :func:`combinations`
- :func:`contains`
- :func:`element_at`
- :func:`filter`
- :func:`flatten`
- :func:`ngrams`
- :func:`none_match`
- :func:`reduce`
- :func:`repeat`
- :func:`reverse`
- :func:`sequence`
- :func:`shuffle`
- :func:`slice`
- :func:`transform`
- :func:`zip`
- :func:`zip_with`

Subscript operator: []
----------------------

The ``[]`` operator is used to access an element of an array and is indexed
starting from one::

Concatenation operator: ||
--------------------------

The ``||`` operator is used to concatenate an array with an array or an element
of the same type::

Binary
------

Details in :doc:`binary`

- :func:`concat`
- :func:`crc32`
- :func:`hmac_md5`
- :func:`hmac_sha1`
- :func:`hmac_sha256`
- :func:`hmac_sha512`
- :func:`length`
- :func:`lpad`
- :func:`md5`
- :func:`murmur3`
- :func:`rpad`
- :func:`reverse`
- :func:`sha1`
- :func:`sha256`
- :func:`sha512`
- :func:`substr`
- :func:`spooky_hash_v2_32`
- :func:`spooky_hash_v2_64`
- :func:`from_base64`
- :func:`to_base64`
- :func:`from_base64url`
- :func:`to_base64url`
- :func:`from_big_endian_32`
- :func:`to_big_endian_32`
- :func:`from_big_endian_64`
- :func:`to_big_endian_64`
- :func:`from_ieee754_32`
- :func:`to_ieee754_32`
- :func:`from_ieee754_64`
- :func:`to_ieee754_64`
- :func:`from_hex`
- :func:`to_hex`
- :func:`xxhash64`

Binary operators
----------------

The ``||`` operator performs concatenation.

Bitwise
-------

Details in :doc:`bitwise`

- :func:`bit_count`
- :func:`bitwise_and`
- :func:`bitwise_not`
- :func:`bitwise_or`
- :func:`bitwise_xor`
- :func:`bitwise_left_shift`
- :func:`bitwise_right_shift`
- :func:`bitwise_right_shift_arithmetic`

Color
-----

Details in :doc:`color`

- :func:`bar`
- :func:`color`
- :func:`render`
- :func:`rgb`

Comparison
----------
Details in :doc:`comparison`

- :func:`greatest`
- :func:`least`

.. _comparison_operators:
.. _like_operator:
.. _is_distinct_operator:
.. _is_null_operator:
.. _range_operator:

Conditional
-----------
Details in :doc:`conditional`

- :func:`coalesce`
- :func:`if`
- :func:`nullif`
- :func:`try`

Conversion
----------

Details in :doc:`conversion`

- :func:`cast`
- :func:`format`
- :func:`try_cast`
- :func:`typeof`

Date and time
-------------

Details in :doc:`datetime`

- :func:`current_timezone`
- :func:`date`
- :func:`last_day_of_month`
- :func:`from_iso8601_timestamp`
- :func:`from_iso8601_date`
- :func:`at_timezone`
- :func:`with_timezone`
- :func:`from_unixtime`
- :func:`from_unixtime_nanos`
- :func:`now`
- :func:`to_iso8601`
- :func:`to_milliseconds`
- :func:`to_unixtime`
- :func:`date_trunc`
- :func:`date_add`
- :func:`date_diff`
- :func:`parse_duration`
- :func:`human_readable_seconds`
- :func:`date_format`
- :func:`date_parse`

Decimal
-------


Geospatial
----------

HyperLogLog
-----------


JSON
----

Lambda
------

Logical
-------

Machine learning
----------------

Map
---

Math
----

Quantile digest
---------------


Regular expression
------------------

Session
-------

String
------

System
------


T-Digest
--------

Teradata
--------

UUID
----

URL
---

Window
------




