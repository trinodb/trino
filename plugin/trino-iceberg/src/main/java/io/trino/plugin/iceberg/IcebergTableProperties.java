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
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergConfig.FORMAT_VERSION_SUPPORT_MAX;
import static io.trino.plugin.iceberg.IcebergConfig.FORMAT_VERSION_SUPPORT_MIN;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class IcebergTableProperties
{
    public static final String FILE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONING_PROPERTY = "partitioning";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String LOCATION_PROPERTY = "location";
    public static final String FORMAT_VERSION_PROPERTY = "format_version";
    public static final String ORC_BLOOM_FILTER_COLUMNS = "orc_bloom_filter_columns";
    public static final String ORC_BLOOM_FILTER_FPP = "orc_bloom_filter_fpp";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public IcebergTableProperties(
            IcebergConfig icebergConfig,
            OrcWriterConfig orcWriterConfig)
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
                .add(new PropertyMetadata<>(
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
                        value -> value))
                .add(doubleProperty(
                        ORC_BLOOM_FILTER_FPP,
                        "ORC Bloom filter false positive probability",
                        orcWriterConfig.getDefaultBloomFilterFpp(),
                        IcebergTableProperties::validateOrcBloomFilterFpp,
                        false))
                .build();
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

    public static List<String> getOrcBloomFilterColumns(Map<String, Object> tableProperties)
    {
        List<String> orcBloomFilterColumns = (List<String>) tableProperties.get(ORC_BLOOM_FILTER_COLUMNS);
        return orcBloomFilterColumns == null ? ImmutableList.of() : ImmutableList.copyOf(orcBloomFilterColumns);
    }

    public static Double getOrcBloomFilterFpp(Map<String, Object> tableProperties)
    {
        return (Double) tableProperties.get(ORC_BLOOM_FILTER_FPP);
    }

    private static void validateOrcBloomFilterFpp(double fpp)
    {
        if (fpp < 0.0 || fpp > 1.0) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Bloom filter fpp value must be between 0.0 and 1.0");
        }
    }
}
