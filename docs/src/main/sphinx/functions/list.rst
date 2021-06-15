===============================
List of functions and operators
===============================

#
-

- :ref:`[] substring operator <subscript_operator>`
- :ref:`|| concatenation operator <concatenation_operator>`
- :ref:`\< comparison operator <comparison_operators>`
- :ref:`\> comparison operator <comparison_operators>`
- :ref:`<= comparison operator <comparison_operators>`
- :ref:`>= comparison operator <comparison_operators>`
- :ref:`= comparison operator <comparison_operators>`
- :ref:`<> comparison operator <comparison_operators>`
- :ref:`\!= comparison operator <comparison_operators>`
- :ref:`-> lambda expression <lambda_expressions>`
- :ref:`+ mathematical operator <mathematical_operators>`
- :ref:`- mathematical operator <mathematical_operators>`
- :ref:`* mathematical operator <mathematical_operators>`
- :ref:`/ mathematical operator <mathematical_operators>`
- :ref:`% mathematical operator <mathematical_operators>`

A
-

- :func:`abs`
- :func:`acos`
- :ref:`ALL <quantified_comparison_predicates>`
- :func:`all_match`
- :ref:`AND <logical_operators>`
- :ref:`ANY <quantified_comparison_predicates>`
- :func:`any_match`
- :func:`approx_distinct`
- :func:`approx_most_frequent`
- :func:`approx_percentile`
- :func:`approx_set`
- :func:`arbitrary`
- :func:`array_agg`
- :func:`array_distinct`
- :func:`array_except`
- :func:`array_intersect`
- :func:`array_join`
- :func:`array_max`
- :func:`array_min`
- :func:`array_position`
- :func:`array_remove`
- :func:`array_sort`
- :func:`array_union`
- :func:`arrays_overlap`
- :func:`asin`
- :ref:`AT TIME ZONE <at_time_zone_operator>`
- :func:`at_timezone`
- :func:`atan`
- :func:`atan2`
- :func:`avg`

B
-

- :func:`bar`
- :func:`beta_cdf`
- :ref:`BETWEEN <range_operator>`
- :func:`bing_tile`
- :func:`bing_tile_at`
- :func:`bing_tile_coordinates`
- :func:`bing_tile_polygon`
- :func:`bing_tile_quadkey`
- :func:`bing_tile_zoom_level`
- :func:`bing_tiles_around`
- :func:`bit_count`
- :func:`bitwise_and`
- :func:`bitwise_and_agg`
- :func:`bitwise_left_shift`
- :func:`bitwise_not`
- :func:`bitwise_or`
- :func:`bitwise_or_agg`
- :func:`bitwise_right_shift`
- :func:`bitwise_right_shift_arithmetic`
- :func:`bitwise_xor`
- :func:`bool_and`
- :func:`bool_or`

C
-

- :func:`cardinality`
- :ref:`CASE <case_expression>`
- :func:`cast`
- :func:`cbrt`
- :func:`ceil`
- :func:`ceiling`
- :func:`char2hexint`
- :func:`checksum`
- :func:`chr`
- :func:`classify`
- :ref:`coalesce <coalesce_function>`
- :func:`codepoint`
- :func:`color`
- :func:`combinations`
- :func:`concat`
- :func:`concat_ws`
- :func:`contains`
- :func:`contains_sequence`
- :func:`convex_hull_agg`
- :func:`corr`
- :func:`cos`
- :func:`cosh`
- :func:`cosine_similarity`
- :func:`count`
- :func:`count_if`
- :func:`covar_pop`
- :func:`covar_samp`
- :func:`crc32`
- :func:`cume_dist`
- :data:`current_date`
- :data:`current_time`
- :data:`current_timestamp`
- :func:`current_timezone`
- :data:`current_user`

D
-

- :func:`date`
- :func:`date_add`
- :func:`date_diff`
- :func:`date_format`
- :func:`date_parse`
- :func:`date_trunc`
- :func:`day`
- :func:`day_of_month`
- :func:`day_of_week`
- :func:`day_of_year`
- :ref:`DECIMAL <decimal_literal>`
- :func:`degrees`
- :func:`dense_rank`
- :func:`dow`
- :func:`doy`

E
-

- :func:`e`
- :func:`element_at`
- :func:`empty_approx_set`
- ``evaluate_classifier_predictions``
- :func:`every`
- :func:`extract`
- :func:`exp`

F
-

- :func:`features`
- :func:`filter`
- :func:`first_value`
- :func:`flatten`
- :func:`floor`
- :func:`format`
- ``format_datetime``
- :func:`format_number`
- :func:`from_base`
- :func:`from_base64`
- :func:`from_base64url`
- :func:`from_big_endian_32`
- :func:`from_big_endian_64`
- :func:`from_encoded_polyline`
- ``from_geojson_geometry``
- :func:`from_hex`
- :func:`from_ieee754_32`
- :func:`from_ieee754_64`
- :func:`from_iso8601_date`
- :func:`from_iso8601_timestamp`
- :func:`from_iso8601_timestamp_nanos`
- :func:`from_unixtime`
- :func:`from_unixtime_nanos`
- :func:`from_utf8`

G
-

- :func:`geometric_mean`
- :func:`geometry_from_hadoop_shape`
- :func:`geometry_invalid_reason`
- :func:`geometry_to_bing_tiles`
- :func:`geometry_union`
- :func:`geometry_union_agg`
- :func:`great_circle_distance`
- :func:`greatest`

H
-

- :func:`hamming_distance`
- ``hash_counts``
- :func:`histogram`
- :func:`hmac_md5`
- :func:`hmac_sha1`
- :func:`hmac_sha256`
- :func:`hmac_sha512`
- :func:`hour`
- :func:`human_readable_seconds`

I
-

- :ref:`if <if_function>`
- :func:`index`
- :func:`infinity`
- ``intersection_cardinality``
- :func:`inverse_beta_cdf`
- :func:`inverse_normal_cdf`
- :func:`is_finite`
- :func:`is_infinite`
- :func:`is_json_scalar`
- :func:`is_nan`
- :ref:`IS NOT DISTINCT <is_distinct_operator>`
- :ref:`IS NOT NULL <is_null_operator>`
- :ref:`IS DISTINCT <is_distinct_operator>`
- :ref:`IS NULL <is_null_operator>`

J
-

- ``jaccard_index``
- :func:`json_array_contains`
- :func:`json_array_get`
- :func:`json_array_length`
- :func:`json_extract`
- :func:`json_extract_scalar`
- :func:`json_format`
- :func:`json_parse`
- :func:`json_size`

K
-

- :func:`kurtosis`

L
-

- :func:`lag`
- :func:`last_day_of_month`
- :func:`last_value`
- :func:`lead`
- :func:`learn_classifier`
- :func:`learn_libsvm_classifier`
- :func:`learn_libsvm_regressor`
- :func:`learn_regressor`
- :func:`least`
- :func:`length`
- :func:`levenshtein_distance`
- :func:`line_interpolate_point`
- :func:`line_interpolate_points`
- :func:`ln`
- :data:`localtime`
- :data:`localtimestamp`
- :func:`log`
- :func:`log10`
- :func:`log2`
- :func:`lower`
- :func:`lpad`
- :func:`ltrim`
- :func:`luhn_check`

M
-

- ``make_set_digest``
- :func:`map`
- :func:`map_agg`
- :func:`map_concat`
- :func:`map_entries`
- :func:`map_filter`
- :func:`map_from_entries`
- :func:`map_keys`
- :func:`map_union`
- :func:`map_values`
- :func:`map_zip_with`
- :func:`max`
- :func:`max_by`
- :func:`md5`
- :func:`merge`
- ``merge_set_digest``
- :func:`millisecond`
- :func:`min`
- :func:`min_by`
- :func:`minute`
- :func:`mod`
- :func:`month`
- :func:`multimap_agg`
- :func:`multimap_from_entries`
- :func:`murmur3`

N
-

- :func:`nan`
- :func:`ngrams`
- :func:`none_match`
- :func:`normal_cdf`
- :func:`normalize`
- :ref:`NOT <logical_operators>`
- :ref:`NOT BETWEEN <range_operator>`
- :func:`now`
- :func:`nth_value`
- :func:`ntile`
- :ref:`nullif <nullif_function>`
- :func:`numeric_histogram`

O
-

- ``objectid``
- ``objectid_timestamp``
- :ref:`OR <logical_operators>`

P
-

- :func:`parse_datetime`
- :func:`parse_duration`
- :func:`parse_presto_data_size`
- :func:`percent_rank`
- :func:`pi`
- :func:`position`
- :func:`pow`
- :func:`power`

Q
-

- :func:`qdigest_agg`
- :func:`quarter`

R
-

- :func:`radians`
- :func:`rand`
- :func:`random`
- :func:`rank`
- :func:`reduce`
- :func:`reduce_agg`
- :func:`regexp_count`
- :func:`regexp_extract`
- :func:`regexp_extract_all`
- :func:`regexp_like`
- :func:`regexp_position`
- :func:`regexp_replace`
- :func:`regexp_split`
- :func:`regress`
- :func:`regr_intercept`
- :func:`regr_slope`
- :func:`render`
- :func:`repeat`
- :func:`replace`
- :func:`reverse`
- :func:`rgb`
- :func:`round`
- :func:`row_number`
- :func:`rpad`
- :func:`rtrim`

S
-

- :func:`second`
- :func:`sequence`
- :func:`sha1`
- :func:`sha256`
- :func:`sha512`
- :func:`shuffle`
- :func:`sign`
- :func:`simplify_geometry`
- :func:`sin`
- :func:`skewness`
- :func:`slice`
- :ref:`SOME <quantified_comparison_predicates>`
- :func:`soundex`
- ``spatial_partitioning``
- ``spatial_partitions``
- :func:`split`
- :func:`split_part`
- :func:`split_to_map`
- :func:`split_to_multimap`
- :func:`spooky_hash_v2_32`
- :func:`spooky_hash_v2_64`
- :func:`sqrt`
- :func:`ST_Area`
- :func:`ST_AsBinary`
- :func:`ST_AsText`
- :func:`ST_Boundary`
- :func:`ST_Buffer`
- :func:`ST_Centroid`
- :func:`ST_Contains`
- :func:`ST_ConvexHull`
- :func:`ST_CoordDim`
- :func:`ST_Crosses`
- :func:`ST_Difference`
- :func:`ST_Dimension`
- :func:`ST_Disjoint`
- :func:`ST_Distance`
- :func:`ST_EndPoint`
- :func:`ST_Envelope`
- :func:`ST_EnvelopeAsPts`
- :func:`ST_Equals`
- :func:`ST_ExteriorRing`
- :func:`ST_Geometries`
- :func:`ST_GeometryFromText`
- :func:`ST_GeometryN`
- :func:`ST_GeometryType`
- :func:`ST_GeomFromBinary`
- :func:`ST_InteriorRingN`
- :func:`ST_InteriorRings`
- :func:`ST_Intersection`
- :func:`ST_Intersects`
- :func:`ST_IsClosed`
- :func:`ST_IsEmpty`
- :func:`ST_IsRing`
- :func:`ST_IsSimple`
- :func:`ST_IsValid`
- :func:`ST_Length`
- :func:`ST_LineFromText`
- :func:`ST_LineString`
- :func:`ST_MultiPoint`
- :func:`ST_NumGeometries`
- ``ST_NumInteriorRing``
- :func:`ST_NumPoints`
- :func:`ST_Overlaps`
- :func:`ST_Point`
- :func:`ST_PointN`
- :func:`ST_Points`
- :func:`ST_Polygon`
- :func:`ST_Relate`
- :func:`ST_StartPoint`
- :func:`ST_SymDifference`
- :func:`ST_Touches`
- :func:`ST_Union`
- :func:`ST_Within`
- :func:`ST_X`
- :func:`ST_XMax`
- :func:`ST_XMin`
- :func:`ST_Y`
- :func:`ST_YMax`
- :func:`ST_YMin`
- :func:`starts_with`
- :func:`stddev`
- :func:`stddev_pop`
- :func:`stddev_samp`
- :func:`strpos`
- :func:`substr`
- :func:`substring`
- :func:`sum`

T
-

- :func:`tan`
- :func:`tanh`
- :func:`tdigest_agg`
- :func:`timezone_hour`
- :func:`timezone_minute`
- :func:`to_base`
- :func:`to_base64`
- :func:`to_base64url`
- :func:`to_big_endian_32`
- :func:`to_big_endian_64`
- :func:`to_char`
- :func:`to_date`
- :func:`to_encoded_polyline`
- ``to_geojson_geometry``
- :func:`to_geometry`
- :func:`to_hex`
- :func:`to_ieee754_32`
- :func:`to_ieee754_64`
- :func:`to_iso8601`
- :func:`to_milliseconds`
- :func:`to_spherical_geography`
- :func:`to_timestamp`
- :func:`to_unixtime`
- :func:`to_utf8`
- :func:`transform`
- :func:`transform_keys`
- :func:`transform_values`
- :func:`translate`
- :func:`trim`
- :func:`truncate`
- :ref:`try <try_function>`
- :func:`try_cast`
- :func:`typeof`

U
-

- :func:`upper`
- :func:`url_decode`
- :func:`url_encode`
- :func:`url_extract_fragment`
- :func:`url_extract_host`
- :func:`url_extract_parameter`
- :func:`url_extract_path`
- :func:`url_extract_protocol`
- :func:`url_extract_port`
- :func:`url_extract_query`
- :func:`uuid`

V
-

- :func:`value_at_quantile`
- :func:`values_at_quantiles`
- :func:`var_pop`
- :func:`var_samp`
- :func:`variance`
- :func:`version`

W
-

- :func:`week`
- :func:`week_of_year`
- :func:`width_bucket`
- :func:`wilson_interval_lower`
- :func:`wilson_interval_upper`
- :func:`with_timezone`
- :func:`word_stem`

X
-

- :func:`xxhash64`

Y
-

- :func:`year`
- :func:`year_of_week`
- :func:`yow`

Z
-

- :func:`zip`
- :func:`zip_with`

