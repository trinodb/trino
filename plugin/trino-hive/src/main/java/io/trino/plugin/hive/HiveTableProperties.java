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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.PARTITION_PROJECTION_ENABLED;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.PARTITION_PROJECTION_IGNORE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.PARTITION_PROJECTION_LOCATION_TEMPLATE;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V2;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class HiveTableProperties
{
    public static final String EXTERNAL_LOCATION_PROPERTY = "external_location";
    public static final String STORAGE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String BUCKETED_BY_PROPERTY = "bucketed_by";
    public static final String BUCKETING_VERSION = "bucketing_version";
    public static final String BUCKET_COUNT_PROPERTY = "bucket_count";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String ORC_BLOOM_FILTER_COLUMNS = "orc_bloom_filter_columns";
    public static final String ORC_BLOOM_FILTER_FPP = "orc_bloom_filter_fpp";
    public static final String AVRO_SCHEMA_URL = "avro_schema_url";
    public static final String AVRO_SCHEMA_LITERAL = "avro_schema_literal";
    public static final String TEXTFILE_FIELD_SEPARATOR = "textfile_field_separator";
    public static final String TEXTFILE_FIELD_SEPARATOR_ESCAPE = "textfile_field_separator_escape";
    public static final String NULL_FORMAT_PROPERTY = "null_format";
    public static final String SKIP_HEADER_LINE_COUNT = "skip_header_line_count";
    public static final String SKIP_FOOTER_LINE_COUNT = "skip_footer_line_count";
    public static final String CSV_SEPARATOR = "csv_separator";
    public static final String CSV_QUOTE = "csv_quote";
    public static final String CSV_ESCAPE = "csv_escape";
    public static final String REGEX_PATTERN = "regex";
    public static final String REGEX_CASE_INSENSITIVE = "regex_case_insensitive";
    public static final String TRANSACTIONAL = "transactional";
    public static final String AUTO_PURGE = "auto_purge";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public HiveTableProperties(
            HiveConfig config,
            OrcWriterConfig orcWriterConfig)
    {
        tableProperties = ImmutableList.of(
                stringProperty(
                        EXTERNAL_LOCATION_PROPERTY,
                        "File system location URI for external table",
                        null,
                        false),
                enumProperty(
                        STORAGE_FORMAT_PROPERTY,
                        "Hive storage format for the table",
                        HiveStorageFormat.class,
                        config.getHiveStorageFormat(),
                        false),
                new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value),
                new PropertyMetadata<>(
                        BUCKETED_BY_PROPERTY,
                        "Bucketing columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value),
                new PropertyMetadata<>(
                        SORTED_BY_PROPERTY,
                        "Bucket sorting columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(String.class::cast)
                                .map(HiveUtil::sortingColumnFromString)
                                .collect(toImmutableList()),
                        value -> ((List<?>) value).stream()
                                .map(SortingColumn.class::cast)
                                .map(HiveUtil::sortingColumnToString)
                                .collect(toImmutableList())),
                new PropertyMetadata<>(
                        ORC_BLOOM_FILTER_COLUMNS,
                        "ORC Bloom filter index columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(String.class::cast)
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value),
                doubleProperty(
                        ORC_BLOOM_FILTER_FPP,
                        "ORC Bloom filter false positive probability",
                        orcWriterConfig.getDefaultBloomFilterFpp(),
                        false),
                integerProperty(BUCKETING_VERSION, "Bucketing version", null, false),
                integerProperty(BUCKET_COUNT_PROPERTY, "Number of buckets", 0, false),
                stringProperty(AVRO_SCHEMA_URL, "URI pointing to Avro schema for the table", null, false),
                stringProperty(AVRO_SCHEMA_LITERAL, "JSON-encoded Avro schema for the table", null, false),
                integerProperty(SKIP_HEADER_LINE_COUNT, "Number of header lines", null, false),
                integerProperty(SKIP_FOOTER_LINE_COUNT, "Number of footer lines", null, false),
                stringProperty(TEXTFILE_FIELD_SEPARATOR, "TEXTFILE field separator character", null, false),
                stringProperty(TEXTFILE_FIELD_SEPARATOR_ESCAPE, "TEXTFILE field separator escape character", null, false),
                stringProperty(NULL_FORMAT_PROPERTY, "Serialization format for NULL value", null, false),
                stringProperty(CSV_SEPARATOR, "CSV separator character", null, false),
                stringProperty(CSV_QUOTE, "CSV quote character", null, false),
                stringProperty(CSV_ESCAPE, "CSV escape character", null, false),
                stringProperty(REGEX_PATTERN, "REGEX pattern", null, false),
                booleanProperty(REGEX_CASE_INSENSITIVE, "REGEX pattern is case insensitive", null, false),
                booleanProperty(TRANSACTIONAL, "Table is transactional", null, false),
                booleanProperty(AUTO_PURGE, "Skip trash when table or partition is deleted", config.isAutoPurge(), false),
                booleanProperty(
                        PARTITION_PROJECTION_IGNORE,
                        "Disable AWS Athena partition projection in Trino only",
                        null,
                        false),
                booleanProperty(
                        PARTITION_PROJECTION_ENABLED,
                        "Enable AWS Athena partition projection",
                        null,
                        false),
                stringProperty(
                        PARTITION_PROJECTION_LOCATION_TEMPLATE,
                        "Partition projection location template",
                        null,
                        false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static String getExternalLocation(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(EXTERNAL_LOCATION_PROPERTY);
    }

    public static String getAvroSchemaUrl(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(AVRO_SCHEMA_URL);
    }

    public static String getAvroSchemaLiteral(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(AVRO_SCHEMA_LITERAL);
    }

    public static Optional<Integer> getHeaderSkipCount(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Integer) tableProperties.get(SKIP_HEADER_LINE_COUNT));
    }

    public static Optional<Integer> getFooterSkipCount(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Integer) tableProperties.get(SKIP_FOOTER_LINE_COUNT));
    }

    public static Optional<String> getNullFormat(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(NULL_FORMAT_PROPERTY));
    }

    public static HiveStorageFormat getHiveStorageFormat(Map<String, Object> tableProperties)
    {
        return (HiveStorageFormat) tableProperties.get(STORAGE_FORMAT_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionedBy(Map<String, Object> tableProperties)
    {
        List<String> partitionedBy = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        return partitionedBy == null ? ImmutableList.of() : ImmutableList.copyOf(partitionedBy);
    }

    public static Optional<HiveBucketProperty> getBucketProperty(Map<String, Object> tableProperties)
    {
        List<String> bucketedBy = getBucketedBy(tableProperties);
        List<SortingColumn> sortedBy = getSortedBy(tableProperties);
        int bucketCount = (Integer) tableProperties.get(BUCKET_COUNT_PROPERTY);
        if ((bucketedBy.isEmpty()) && (bucketCount == 0)) {
            if (!sortedBy.isEmpty()) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, format("%s may be specified only when %s is specified", SORTED_BY_PROPERTY, BUCKETED_BY_PROPERTY));
            }
            return Optional.empty();
        }
        if (bucketCount < 0) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("%s must be greater than zero", BUCKET_COUNT_PROPERTY));
        }
        if (bucketedBy.isEmpty() || bucketCount == 0) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("%s and %s must be specified together", BUCKETED_BY_PROPERTY, BUCKET_COUNT_PROPERTY));
        }
        BucketingVersion bucketingVersion = getBucketingVersion(tableProperties);
        return Optional.of(new HiveBucketProperty(bucketedBy, bucketingVersion, bucketCount, sortedBy));
    }

    public static BucketingVersion getBucketingVersion(Map<String, Object> tableProperties)
    {
        Integer property = (Integer) tableProperties.get(BUCKETING_VERSION);
        if (property == null || property == 1) {
            return BUCKETING_V1;
        }
        if (property == 2) {
            return BUCKETING_V2;
        }
        throw new TrinoException(INVALID_TABLE_PROPERTY, format("%s must be between 1 and 2 (inclusive): %s", BUCKETING_VERSION, property));
    }

    @SuppressWarnings("unchecked")
    private static List<String> getBucketedBy(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(BUCKETED_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    private static List<SortingColumn> getSortedBy(Map<String, Object> tableProperties)
    {
        return (List<SortingColumn>) tableProperties.get(SORTED_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getOrcBloomFilterColumns(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(ORC_BLOOM_FILTER_COLUMNS);
    }

    public static Double getOrcBloomFilterFpp(Map<String, Object> tableProperties)
    {
        return (Double) tableProperties.get(ORC_BLOOM_FILTER_FPP);
    }

    public static Optional<Character> getSingleCharacterProperty(Map<String, Object> tableProperties, String key)
    {
        Object value = tableProperties.get(key);
        if (value == null) {
            return Optional.empty();
        }
        String stringValue = (String) value;
        if (stringValue.length() != 1) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("%s must be a single character string, but was: '%s'", key, stringValue));
        }
        return Optional.of(stringValue.charAt(0));
    }

    public static Optional<String> getRegexPattern(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(REGEX_PATTERN));
    }

    public static Optional<Boolean> isRegexCaseInsensitive(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Boolean) tableProperties.get(REGEX_CASE_INSENSITIVE));
    }

    public static Optional<Boolean> isTransactional(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Boolean) tableProperties.get(TRANSACTIONAL));
    }

    public static Optional<Boolean> isAutoPurge(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Boolean) tableProperties.get(AUTO_PURGE));
    }
}
