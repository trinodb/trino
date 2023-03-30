=========================================
List of functions by topic
=========================================

Aggregate
---------

For more details, see :doc:`aggregate`

* :func:`approx_distinct`
* :func:`approx_most_frequent`
* :func:`approx_percentile`
* ``approx_set()``
* :func:`arbitrary`
* :func:`array_agg`
* :func:`avg`
* :func:`bitwise_and_agg`
* :func:`bitwise_or_agg`
* :func:`bool_and`
* :func:`bool_or`
* :func:`checksum`
* :func:`corr`
* :func:`count`
* :func:`count_if`
* :func:`covar_pop`
* :func:`covar_samp`
* :func:`every`
* :func:`geometric_mean`
* :func:`histogram`
* :func:`kurtosis`
* :func:`map_agg`
* :func:`map_union`
* :func:`max`
* :func:`max_by`
* ``merge()``
* :func:`min`
* :func:`min_by`
* :func:`multimap_agg`
* :func:`numeric_histogram`
* ``qdigest_agg()``
* :func:`regr_intercept`
* :func:`regr_slope`
* :func:`skewness`
* :func:`sum`
* :func:`stddev`
* :func:`stddev_pop`
* :func:`stddev_samp`
* ``tdigest_agg()``
* :func:`variance`
* :func:`var_pop`
* :func:`var_samp`

Array
-----

For more details, see :doc:`array`

* :func:`all_match`
* :func:`any_match`
* :func:`array_distinct`
* :func:`array_except`
* :func:`array_intersect`
* :func:`array_join`
* :func:`array_max`
* :func:`array_min`
* :func:`array_position`
* :func:`array_remove`
* :func:`array_sort`
* :func:`array_union`
* :func:`arrays_overlap`
* :func:`cardinality`
* :func:`combinations`
* ``concat()``
* :func:`contains`
* :func:`element_at`
* :func:`filter`
* :func:`flatten`
* :func:`ngrams`
* :func:`none_match`
* :func:`reduce`
* :func:`repeat`
* ``reverse()``
* :func:`sequence`
* :func:`shuffle`
* :func:`slice`
* :func:`transform`
* :func:`trim_array`
* :func:`zip`
* :func:`zip_with`

Binary
------

For more details, see :doc:`binary`

* ``concat()``
* :func:`crc32`
* :func:`from_base32`
* :func:`from_base64`
* :func:`from_base64url`
* :func:`from_big_endian_32`
* :func:`from_big_endian_64`
* :func:`from_hex`
* :func:`from_ieee754_32`
* :func:`from_ieee754_64`
* :func:`hmac_md5`
* :func:`hmac_sha1`
* :func:`hmac_sha256`
* :func:`hmac_sha512`
* ``length()``
* ``lpad()``
* :func:`md5`
* :func:`murmur3`
* ``reverse()``
* ``rpad()``
* :func:`sha1`
* :func:`sha256`
* :func:`sha512`
* :func:`spooky_hash_v2_32`
* :func:`spooky_hash_v2_64`
* ``substr()``
* :func:`to_base32`
* :func:`to_base64`
* :func:`to_base64url`
* :func:`to_big_endian_32`
* :func:`to_big_endian_64`
* :func:`to_hex`
* :func:`to_ieee754_32`
* :func:`to_ieee754_64`
* :func:`xxhash64`

Bitwise
-------

For more details, see :doc:`bitwise`

* :func:`bit_count`
* :func:`bitwise_and`
* :func:`bitwise_left_shift`
* :func:`bitwise_not`
* :func:`bitwise_or`
* :func:`bitwise_right_shift`
* :func:`bitwise_right_shift_arithmetic`
* :func:`bitwise_xor`

Color
-----

For more details, see :doc:`color`

* :func:`bar`
* :func:`color`
* :func:`render`
* :func:`rgb`

Comparison
----------

For more details, see :doc:`comparison`

* :func:`greatest`
* :func:`least`

Conditional
-----------

For more details, see :doc:`conditional`

* :ref:`coalesce <coalesce_function>`
* :ref:`if <if_function>`
* :ref:`nullif <nullif_function>`
* :ref:`try <try_function>`

Conversion
----------

For more details, see :doc:`conversion`

* :func:`cast`
* :func:`format`
* :func:`try_cast`
* :func:`typeof`

Date and time
-------------

For more details, see :doc:`datetime`

* :ref:`AT TIME ZONE <at_time_zone_operator>`
* :data:`current_date`
* :data:`current_time`
* :data:`current_timestamp`
* :data:`localtime`
* :data:`localtimestamp`
* :func:`current_timezone`
* :func:`date`
* :func:`date_add`
* :func:`date_diff`
* :func:`date_format`
* :func:`date_parse`
* :func:`date_trunc`
* :func:`format_datetime`
* :func:`from_iso8601_date`
* :func:`from_iso8601_timestamp`
* :func:`from_unixtime`
* :func:`from_unixtime_nanos`
* :func:`human_readable_seconds`
* :func:`last_day_of_month`
* :func:`now`
* :func:`parse_duration`
* :func:`to_iso8601`
* :func:`to_milliseconds`
* :func:`to_unixtime`
* :func:`with_timezone`

Geospatial
----------

For more details, see :doc:`geospatial`

* :func:`bing_tile`
* :func:`bing_tile_at`
* :func:`bing_tile_coordinates`
* :func:`bing_tile_polygon`
* :func:`bing_tile_quadkey`
* :func:`bing_tile_zoom_level`
* :func:`bing_tiles_around`
* :func:`convex_hull_agg`
* :func:`from_encoded_polyline`
* :func:`from_geojson_geometry`
* :func:`geometry_from_hadoop_shape`
* :func:`geometry_invalid_reason`
* :func:`geometry_nearest_points`
* :func:`geometry_to_bing_tiles`
* :func:`geometry_union`
* :func:`geometry_union_agg`
* :func:`great_circle_distance`
* :func:`line_interpolate_point`
* :func:`line_locate_point`
* :func:`simplify_geometry`
* :func:`ST_Area`
* :func:`ST_AsBinary`
* :func:`ST_AsText`
* :func:`ST_Boundary`
* :func:`ST_Buffer`
* :func:`ST_Centroid`
* :func:`ST_Contains`
* :func:`ST_ConvexHull`
* :func:`ST_CoordDim`
* :func:`ST_Crosses`
* :func:`ST_Difference`
* :func:`ST_Dimension`
* :func:`ST_Disjoint`
* :func:`ST_Distance`
* :func:`ST_EndPoint`
* :func:`ST_Envelope`
* :func:`ST_Equals`
* :func:`ST_ExteriorRing`
* :func:`ST_Geometries`
* :func:`ST_GeometryFromText`
* :func:`ST_GeometryN`
* :func:`ST_GeometryType`
* :func:`ST_GeomFromBinary`
* :func:`ST_InteriorRings`
* :func:`ST_InteriorRingN`
* :func:`ST_Intersects`
* :func:`ST_Intersection`
* :func:`ST_IsClosed`
* :func:`ST_IsEmpty`
* :func:`ST_IsSimple`
* :func:`ST_IsRing`
* :func:`ST_IsValid`
* :func:`ST_Length`
* :func:`ST_LineFromText`
* :func:`ST_LineString`
* :func:`ST_MultiPoint`
* :func:`ST_NumGeometries`
* :func:`ST_NumInteriorRing`
* :func:`ST_NumPoints`
* :func:`ST_Overlaps`
* :func:`ST_Point`
* :func:`ST_PointN`
* :func:`ST_Points`
* :func:`ST_Polygon`
* :func:`ST_Relate`
* :func:`ST_StartPoint`
* :func:`ST_SymDifference`
* :func:`ST_Touches`
* :func:`ST_Union`
* :func:`ST_Within`
* :func:`ST_X`
* :func:`ST_XMax`
* :func:`ST_XMin`
* :func:`ST_Y`
* :func:`ST_YMax`
* :func:`ST_YMin`
* :func:`to_encoded_polyline`
* :func:`to_geojson_geometry`
* :func:`to_geometry`
* :func:`to_spherical_geography`

HyperLogLog
-----------

For more details, see :doc:`hyperloglog`

* :func:`approx_set`
* ``cardinality()``
* :func:`empty_approx_set`
* :func:`merge`

JSON
----

For more details, see :doc:`json`

* :func:`is_json_scalar`
* :ref:`json_array() <json_array>`
* :func:`json_array_contains`
* :func:`json_array_get`
* :func:`json_array_length`
* :ref:`json_exists() <json_exists>`
* :func:`json_extract`
* :func:`json_extract_scalar`
* :func:`json_format`
* :func:`json_parse`
* :ref:`json_object() <json_object>`
* :ref:`json_query() <json_query>`
* :func:`json_size`
* :ref:`json_value() <json_value>`

Lambda
------

For more details, see :doc:`lambda`

* :func:`any_match`
* :func:`reduce_agg`
* :func:`regexp_replace`
* :func:`transform`

Machine learning
----------------

For more details, see :doc:`ml`

* :func:`classify`
* :func:`features`
* :func:`learn_classifier`
* :func:`learn_libsvm_classifier`
* :func:`learn_libsvm_regressor`
* :func:`learn_regressor`
* :func:`regress`

Map
---

For more details, see :doc:`map`

* :func:`cardinality`
* :func:`element_at`
* :func:`map`
* :func:`map_concat`
* :func:`map_entries`
* :func:`map_filter`
* :func:`map_from_entries`
* :func:`map_keys`
* :func:`map_values`
* :func:`map_zip_with`
* :func:`multimap_from_entries`
* :func:`transform_keys`
* :func:`transform_values`

Math
----

For more details, see :doc:`math`

* :func:`abs`
* :func:`acos`
* :func:`asin`
* :func:`atan`
* :func:`beta_cdf`
* :func:`cbrt`
* :func:`ceil`
* :func:`cos`
* :func:`cosh`
* :func:`cosine_similarity`
* :func:`degrees`
* :func:`e`
* :func:`exp`
* :func:`floor`
* :func:`from_base`
* :func:`infinity`
* :func:`inverse_beta_cdf`
* :func:`inverse_normal_cdf`
* :func:`is_finite`
* :func:`is_nan`
* :func:`ln`
* :func:`log`
* :func:`log2`
* :func:`log10`
* :func:`mod`
* :func:`nan`
* :func:`normal_cdf`
* :func:`pi`
* :func:`pow`
* :func:`power`
* :func:`radians`
* :func:`rand`
* :func:`random`
* :func:`round`
* :func:`sign`
* :func:`sin`
* :func:`sinh`
* :func:`sqrt`
* :func:`tan`
* :func:`tanh`
* :func:`to_base`
* :func:`truncate`
* :func:`width_bucket`
* :func:`wilson_interval_lower`
* :func:`wilson_interval_upper`

Quantile digest
---------------

For more details, see :doc:`qdigest`

* ``merge()``
* :func:`qdigest_agg`
* :func:`value_at_quantile`
* :func:`values_at_quantiles`

Regular expression
------------------

For more details, see :doc:`regexp`

* :func:`regexp_count`
* :func:`regexp_extract`
* :func:`regexp_extract_all`
* :func:`regexp_like`
* :func:`regexp_position`
* :func:`regexp_replace`
* :func:`regexp_split`

Session
-------

For more details, see :doc:`session`

* :data:`current_catalog`
* :func:`current_groups`
* :data:`current_schema`
* :data:`current_user`

Set Digest
----------

For more details, see :doc:`setdigest`

* :func:`make_set_digest`
* :func:`merge_set_digest`
* :ref:`cardinality() <setdigest-cardinality>`
* :func:`intersection_cardinality`
* :func:`jaccard_index`
* :func:`hash_counts`


String
------

For more details, see :doc:`string`

* :func:`chr`
* :func:`codepoint`
* :func:`concat`
* :func:`concat_ws`
* :func:`format`
* :func:`from_utf8`
* :func:`hamming_distance`
* :func:`length`
* :func:`levenshtein_distance`
* :func:`lower`
* :func:`lpad`
* :func:`ltrim`
* :func:`luhn_check`
* :func:`normalize`
* :func:`position`
* :func:`replace`
* :func:`reverse`
* :func:`rpad`
* :func:`rtrim`
* :func:`soundex`
* :func:`split`
* :func:`split_part`
* :func:`split_to_map`
* :func:`split_to_multimap`
* :func:`starts_with`
* :func:`strpos`
* :func:`substr`
* :func:`substring`
* :func:`to_utf8`
* :func:`translate`
* :func:`trim`
* :func:`upper`
* :func:`word_stem`

System
------

For more details, see :doc:`system`

* :func:`version`

T-Digest
--------

For more details, see :doc:`tdigest`

* ``merge()``
* :func:`tdigest_agg`
* ``value_at_quantile()``

Teradata
--------

For more details, see :doc:`teradata`

* :func:`char2hexint`
* :func:`index`
* :func:`to_char`
* :func:`to_timestamp`
* :func:`to_date`

URL
---

For more details, see :doc:`url`

* :func:`url_decode`
* :func:`url_encode`
* :func:`url_extract_fragment`
* :func:`url_extract_host`
* :func:`url_extract_parameter`
* :func:`url_extract_path`
* :func:`url_extract_port`
* :func:`url_extract_protocol`
* :func:`url_extract_query`

UUID
----

For more details, see :doc:`uuid`

* :func:`uuid`

Window
------

For more details, see :doc:`window`

* :func:`cume_dist`
* :func:`dense_rank`
* :func:`first_value`
* :func:`lag`
* :func:`last_value`
* :func:`lead`
* :func:`nth_value`
* :func:`ntile`
* :func:`percent_rank`
* :func:`rank`
* :func:`row_number`
