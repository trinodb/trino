/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.phoenix5;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the Phoenix connector. Used when creating a table:
 * <p>
 * <pre>CREATE TABLE foo (a VARCHAR with (primary_key = true), b INT) WITH (SALT_BUCKETS=10, VERSIONS=5, COMPRESSION='lz');</pre>
 */
public final class PhoenixTableProperties
        implements TablePropertiesProvider
{
    public static final String ROWKEYS = "rowkeys";
    public static final String SALT_BUCKETS = "salt_buckets";
    public static final String SPLIT_ON = "split_on";
    public static final String DISABLE_WAL = "disable_wal";
    public static final String IMMUTABLE_ROWS = "immutable_rows";
    public static final String DEFAULT_COLUMN_FAMILY = "default_column_family";
    public static final String BLOOMFILTER = "bloomfilter";
    public static final String VERSIONS = "versions";
    public static final String MIN_VERSIONS = "min_versions";
    public static final String COMPRESSION = "compression";
    public static final String TTL = "ttl";
    public static final String DATA_BLOCK_ENCODING = "data_block_encoding";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public PhoenixTableProperties()
    {
        tableProperties = ImmutableList.of(
                stringProperty(
                        ROWKEYS,
                        "Comma-separated list of columns to be the primary key.",
                        null,
                        false),
                integerProperty(
                        SALT_BUCKETS,
                        "Number of salt buckets.  This causes an extra byte to be transparently prepended to every row key to ensure an evenly distributed read and write load across all region servers.",
                        null,
                        false),
                stringProperty(
                        SPLIT_ON,
                        "Comma-separated list of keys to split on during table creation.",
                        null,
                        false),
                booleanProperty(
                        DISABLE_WAL,
                        "If true, causes HBase not to write data to the write-ahead-log, thus making updates faster at the expense of potentially losing data in the event of a region server failure.",
                        null,
                        false),
                booleanProperty(
                        IMMUTABLE_ROWS,
                        "Set to true if the table has rows which are write-once, append-only.",
                        null,
                        false),
                stringProperty(
                        DEFAULT_COLUMN_FAMILY,
                        "The column family name to use by default.",
                        null,
                        false),
                enumProperty(
                        BLOOMFILTER,
                        "NONE, ROW or ROWCOL to enable blooms per column family.",
                        BloomType.class,
                        null,
                        false),
                integerProperty(
                        VERSIONS,
                        "The maximum number of row versions to store, configured per column family via HColumnDescriptor.",
                        null,
                        false),
                integerProperty(
                        MIN_VERSIONS,
                        "The minimum number of row versions to store, configured per column family via HColumnDescriptor.",
                        null,
                        false),
                enumProperty(
                        COMPRESSION,
                        "Compression algorithm to use for HBase blocks. Options are: SNAPPY, GZIP, LZ, and others.",
                        Compression.Algorithm.class,
                        null,
                        false),
                integerProperty(
                        TTL,
                        "Number of seconds for cell TTL.  HBase will automatically delete rows once the expiration time is reached.",
                        null,
                        false),
                enumProperty(
                        DATA_BLOCK_ENCODING,
                        "The block encoding algorithm to use for Cells in HBase blocks. Options are: NONE, PREFIX, DIFF, FAST_DIFF, ROW_INDEX_V1, and others.",
                        DataBlockEncoding.class,
                        null,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<Integer> getSaltBuckets(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        Integer value = (Integer) tableProperties.get(SALT_BUCKETS);
        return Optional.ofNullable(value);
    }

    public static Optional<String> getSplitOn(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        String value = (String) tableProperties.get(SPLIT_ON);
        return Optional.ofNullable(value);
    }

    public static Optional<List<String>> getRowkeys(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        String rowkeysCsv = (String) tableProperties.get(ROWKEYS);
        if (rowkeysCsv == null) {
            return Optional.empty();
        }

        return Optional.of(Arrays.stream(StringUtils.split(rowkeysCsv, ','))
                .map(String::trim)
                .collect(toImmutableList()));
    }

    public static Optional<Boolean> getDisableWal(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        Boolean value = (Boolean) tableProperties.get(DISABLE_WAL);
        return Optional.ofNullable(value);
    }

    public static Optional<Boolean> getImmutableRows(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        Boolean value = (Boolean) tableProperties.get(IMMUTABLE_ROWS);
        return Optional.ofNullable(value);
    }

    public static Optional<String> getDefaultColumnFamily(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        String value = (String) tableProperties.get(DEFAULT_COLUMN_FAMILY);
        return Optional.ofNullable(value);
    }

    public static Optional<BloomType> getBloomfilter(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        BloomType value = (BloomType) tableProperties.get(BLOOMFILTER);
        return Optional.ofNullable(value);
    }

    public static Optional<Integer> getVersions(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        Integer value = (Integer) tableProperties.get(VERSIONS);
        return Optional.ofNullable(value);
    }

    public static Optional<Integer> getMinVersions(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        Integer value = (Integer) tableProperties.get(MIN_VERSIONS);
        return Optional.ofNullable(value);
    }

    public static Optional<Compression.Algorithm> getCompression(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        Compression.Algorithm value = (Compression.Algorithm) tableProperties.get(COMPRESSION);
        return Optional.ofNullable(value);
    }

    public static Optional<DataBlockEncoding> getDataBlockEncoding(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        DataBlockEncoding value = (DataBlockEncoding) tableProperties.get(DATA_BLOCK_ENCODING);
        return Optional.ofNullable(value);
    }

    public static Optional<Integer> getTimeToLive(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        Integer value = (Integer) tableProperties.get(TTL);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(value);
    }
}
