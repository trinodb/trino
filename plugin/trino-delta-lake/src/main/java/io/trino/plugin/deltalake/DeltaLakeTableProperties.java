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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class DeltaLakeTableProperties
{
    public static final String LOCATION_PROPERTY = "location";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String CHECKPOINT_INTERVAL_PROPERTY = "checkpoint_interval";
    public static final String CHANGE_DATA_FEED_ENABLED_PROPERTY = "change_data_feed_enabled";
    public static final String COLUMN_MAPPING_MODE_PROPERTY = "column_mapping_mode";
    public static final String DELETION_VECTORS_ENABLED_PROPERTY = "deletion_vectors_enabled";

    private final List<PropertyMetadata<?>> tableProperties;

    @SuppressWarnings("unchecked")
    @Inject
    public DeltaLakeTableProperties(DeltaLakeConfig config)
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false))
                .add(new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<String>) value).stream()
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                .add(longProperty(
                        CHECKPOINT_INTERVAL_PROPERTY,
                        "Checkpoint interval",
                        null,
                        false))
                .add(booleanProperty(
                        CHANGE_DATA_FEED_ENABLED_PROPERTY,
                        "Enables storing change data feed entries",
                        null,
                        false))
                .add(stringProperty(
                        COLUMN_MAPPING_MODE_PROPERTY,
                        "Column mapping mode. Possible values: [ID, NAME, NONE]",
                        // TODO: Consider using 'name' by default. 'none' column mapping doesn't support some statements
                        ColumnMappingMode.NONE.name(),
                        value -> {
                            EnumSet<ColumnMappingMode> allowed = EnumSet.of(ColumnMappingMode.ID, ColumnMappingMode.NAME, ColumnMappingMode.NONE);
                            if (allowed.stream().map(Enum::name).noneMatch(mode -> mode.equalsIgnoreCase(value))) {
                                throw new IllegalArgumentException(format("Invalid value [%s]. Valid values: [ID, NAME, NONE]", value));
                            }
                        },
                        false))
                .add(booleanProperty(
                        DELETION_VECTORS_ENABLED_PROPERTY,
                        "Enables deletion vectors",
                        config.isDeletionVectorsEnabled(),
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static String getLocation(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(LOCATION_PROPERTY);
    }

    public static List<String> getPartitionedBy(Map<String, Object> tableProperties)
    {
        List<String> partitionedBy = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        return partitionedBy == null ? ImmutableList.of() : ImmutableList.copyOf(partitionedBy);
    }

    public static Optional<Long> getCheckpointInterval(Map<String, Object> tableProperties)
    {
        Optional<Long> checkpointInterval = Optional.ofNullable((Long) tableProperties.get(CHECKPOINT_INTERVAL_PROPERTY));
        checkpointInterval.ifPresent(value -> {
            if (value <= 0) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, format("%s must be greater than 0", CHECKPOINT_INTERVAL_PROPERTY));
            }
        });

        return checkpointInterval;
    }

    public static Optional<Boolean> getChangeDataFeedEnabled(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Boolean) tableProperties.get(CHANGE_DATA_FEED_ENABLED_PROPERTY));
    }

    public static ColumnMappingMode getColumnMappingMode(Map<String, Object> tableProperties)
    {
        return ColumnMappingMode.valueOf(tableProperties.get(COLUMN_MAPPING_MODE_PROPERTY).toString().toUpperCase(ENGLISH));
    }

    public static boolean getDeletionVectorsEnabled(Map<String, Object> tableProperties)
    {
        return (boolean) tableProperties.getOrDefault(DELETION_VECTORS_ENABLED_PROPERTY, false);
    }
}
