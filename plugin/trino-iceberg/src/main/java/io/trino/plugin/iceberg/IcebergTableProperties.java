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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergConfig.FORMAT_VERSION_SUPPORT_MAX;
import static io.trino.plugin.iceberg.IcebergConfig.FORMAT_VERSION_SUPPORT_MIN;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static org.apache.iceberg.TableProperties.ORC_BLOOM_FILTER_FPP;
import static org.apache.iceberg.TableProperties.RESERVED_PROPERTIES;

public class IcebergTableProperties
{
    public static final String FILE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONING_PROPERTY = "partitioning";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String LOCATION_PROPERTY = "location";
    public static final String FORMAT_VERSION_PROPERTY = "format_version";
    public static final String MAX_COMMIT_RETRY = "max_commit_retry";
    public static final String ORC_BLOOM_FILTER_COLUMNS_PROPERTY = "orc_bloom_filter_columns";
    public static final String ORC_BLOOM_FILTER_FPP_PROPERTY = "orc_bloom_filter_fpp";
    public static final String PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY = "parquet_bloom_filter_columns";
    public static final String OBJECT_STORE_LAYOUT_ENABLED_PROPERTY = "object_store_layout_enabled";
    public static final String DATA_LOCATION_PROPERTY = "data_location";
    public static final String EXTRA_PROPERTIES_PROPERTY = "extra_properties";

    public static final Set<String> SUPPORTED_PROPERTIES = ImmutableSet.<String>builder()
            .add(FILE_FORMAT_PROPERTY)
            .add(PARTITIONING_PROPERTY)
            .add(SORTED_BY_PROPERTY)
            .add(LOCATION_PROPERTY)
            .add(FORMAT_VERSION_PROPERTY)
            .add(MAX_COMMIT_RETRY)
            .add(ORC_BLOOM_FILTER_COLUMNS_PROPERTY)
            .add(ORC_BLOOM_FILTER_FPP_PROPERTY)
            .add(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)
            .add(DATA_LOCATION_PROPERTY)
            .add(EXTRA_PROPERTIES_PROPERTY)
            .add(PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY)
            .build();

    // These properties are used by Trino or Iceberg internally and cannot be set directly by users through extra_properties
    public static final Set<String> PROTECTED_ICEBERG_NATIVE_PROPERTIES = ImmutableSet.<String>builder()
            .addAll(RESERVED_PROPERTIES)
            .add(ORC_BLOOM_FILTER_COLUMNS)
            .add(ORC_BLOOM_FILTER_FPP)
            .add(DEFAULT_FILE_FORMAT)
            .add(FORMAT_VERSION)
            .build();

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public IcebergTableProperties(
            IcebergConfig icebergConfig,
            OrcWriterConfig orcWriterConfig,
            TypeManager typeManager)
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        FILE_FORMAT_PROPERTY,
                        "File format for the table",
                        IcebergFileFormat.class,
                        icebergConfig.getFileFormat(),
                        false))
                .add(new PropertyMetadata<>(
                        PARTITIONING_PROPERTY,
                        "Partition transforms",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(new PropertyMetadata<>(
                        SORTED_BY_PROPERTY,
                        "Sorted columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false))
                .add(integerProperty(
                        FORMAT_VERSION_PROPERTY,
                        "Iceberg table format version",
                        icebergConfig.getFormatVersion(),
                        IcebergTableProperties::validateFormatVersion,
                        false))
                .add(integerProperty(
                        MAX_COMMIT_RETRY,
                        "Number of times to retry a commit before failing",
                        icebergConfig.getMaxCommitRetry(),
                        value -> {
                            if (value < 0) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, "max_commit_retry must be greater than or equal to 0");
                            }
                        },
                        false))
                .add(new PropertyMetadata<>(
                        ORC_BLOOM_FILTER_COLUMNS_PROPERTY,
                        "ORC Bloom filter index columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(String.class::cast)
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                .add(doubleProperty(
                        ORC_BLOOM_FILTER_FPP_PROPERTY,
                        "ORC Bloom filter false positive probability",
                        orcWriterConfig.getDefaultBloomFilterFpp(),
                        IcebergTableProperties::validateOrcBloomFilterFpp,
                        false))
                .add(new PropertyMetadata<>(
                        PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY,
                        "Parquet Bloom filter index columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(String.class::cast)
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                .add(new PropertyMetadata<>(
                        EXTRA_PROPERTIES_PROPERTY,
                        "Extra table properties",
                        new MapType(VARCHAR, VARCHAR, typeManager.getTypeOperators()),
                        Map.class,
                        ImmutableMap.of(),
                        true, // currently not shown in SHOW CREATE TABLE
                        value -> {
                            Map<String, String> extraProperties = (Map<String, String>) value;
                            if (extraProperties.containsValue(null)) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, format("Extra table property value cannot be null '%s'", extraProperties));
                            }
                            if (extraProperties.containsKey(null)) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, format("Extra table property key cannot be null '%s'", extraProperties));
                            }

                            return extraProperties.entrySet().stream()
                                    .collect(toImmutableMap(entry -> entry.getKey().toLowerCase(ENGLISH), Map.Entry::getValue));
                        },
                        value -> value))
                .add(booleanProperty(
                        OBJECT_STORE_LAYOUT_ENABLED_PROPERTY,
                        "Set to true to enable Iceberg object store file layout",
                        icebergConfig.isObjectStoreLayoutEnabled(),
                        false))
                .add(stringProperty(
                        DATA_LOCATION_PROPERTY,
                        "File system location URI for the table's data files",
                        null,
                        false))
                .build();

        checkState(SUPPORTED_PROPERTIES.containsAll(tableProperties.stream()
                        .map(PropertyMetadata::getName)
                        .collect(toImmutableList())),
                "%s does not contain all supported properties", SUPPORTED_PROPERTIES);
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static IcebergFileFormat getFileFormat(Map<String, Object> tableProperties)
    {
        return (IcebergFileFormat) tableProperties.get(FILE_FORMAT_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitioning(Map<String, Object> tableProperties)
    {
        List<String> partitioning = (List<String>) tableProperties.get(PARTITIONING_PROPERTY);
        return partitioning == null ? ImmutableList.of() : ImmutableList.copyOf(partitioning);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getSortOrder(Map<String, Object> tableProperties)
    {
        List<String> sortedBy = (List<String>) tableProperties.get(SORTED_BY_PROPERTY);
        return sortedBy == null ? ImmutableList.of() : ImmutableList.copyOf(sortedBy);
    }

    public static Optional<String> getTableLocation(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(LOCATION_PROPERTY));
    }

    public static int getFormatVersion(Map<String, Object> tableProperties)
    {
        return (int) tableProperties.get(FORMAT_VERSION_PROPERTY);
    }

    private static void validateFormatVersion(int version)
    {
        if (version < FORMAT_VERSION_SUPPORT_MIN || version > FORMAT_VERSION_SUPPORT_MAX) {
            throw new TrinoException(INVALID_TABLE_PROPERTY,
                    format("format_version must be between %d and %d", FORMAT_VERSION_SUPPORT_MIN, FORMAT_VERSION_SUPPORT_MAX));
        }
    }

    public static int getMaxCommitRetry(Map<String, Object> tableProperties)
    {
        return (int) tableProperties.getOrDefault(MAX_COMMIT_RETRY, COMMIT_NUM_RETRIES_DEFAULT);
    }

    public static List<String> getOrcBloomFilterColumns(Map<String, Object> tableProperties)
    {
        List<String> orcBloomFilterColumns = (List<String>) tableProperties.get(ORC_BLOOM_FILTER_COLUMNS_PROPERTY);
        return orcBloomFilterColumns == null ? ImmutableList.of() : ImmutableList.copyOf(orcBloomFilterColumns);
    }

    public static Double getOrcBloomFilterFpp(Map<String, Object> tableProperties)
    {
        return (Double) tableProperties.get(ORC_BLOOM_FILTER_FPP_PROPERTY);
    }

    private static void validateOrcBloomFilterFpp(double fpp)
    {
        if (fpp < 0.0 || fpp > 1.0) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Bloom filter fpp value must be between 0.0 and 1.0");
        }
    }

    public static List<String> getParquetBloomFilterColumns(Map<String, Object> tableProperties)
    {
        List<String> parquetBloomFilterColumns = (List<String>) tableProperties.get(PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY);
        return parquetBloomFilterColumns == null ? ImmutableList.of() : ImmutableList.copyOf(parquetBloomFilterColumns);
    }

    public static boolean getObjectStoreLayoutEnabled(Map<String, Object> tableProperties)
    {
        return (boolean) tableProperties.getOrDefault(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY, false);
    }

    public static Optional<String> getDataLocation(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(DATA_LOCATION_PROPERTY));
    }

    public static Optional<Map<String, String>> getExtraProperties(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Map<String, String>) tableProperties.get(EXTRA_PROPERTIES_PROPERTY));
    }
}
