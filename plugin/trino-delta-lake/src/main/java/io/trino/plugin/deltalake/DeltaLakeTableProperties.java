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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeManager;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DeltaLakeTableProperties
{
    public static final String LOCATION_PROPERTY = "location";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String CHECKPOINT_INTERVAL_PROPERTY = "checkpoint_interval";
    public static final String CHANGE_DATA_FEED_ENABLED_PROPERTY = "change_data_feed_enabled";
    public static final String COLUMN_MAPPING_MODE_PROPERTY = "column_mapping_mode";
    public static final String DELETION_VECTORS_ENABLED_PROPERTY = "deletion_vectors_enabled";
    public static final String EXTRA_PROPERTIES_PROPERTY = "extra_properties";

    private static final String TABLE_COMMENT_PROPERTY = "comment";
    private static final Set<String> SUPPORTED_PROPERTIES = ImmutableSet.of(
            LOCATION_PROPERTY,
            PARTITIONED_BY_PROPERTY,
            CHECKPOINT_INTERVAL_PROPERTY,
            CHANGE_DATA_FEED_ENABLED_PROPERTY,
            COLUMN_MAPPING_MODE_PROPERTY,
            DELETION_VECTORS_ENABLED_PROPERTY,
            EXTRA_PROPERTIES_PROPERTY);

    // These properties are connector-managed, require protocol actions, or have read/write semantics that Trino cannot enforce through extra_properties.
    private static final Set<String> PROTECTED_DELTA_PROPERTIES = ImmutableSet.of(
            "delta.appendOnly",
            "delta.checkpointInterval",
            "delta.checkpointPolicy",
            "delta.columnMapping.maxColumnId",
            "delta.columnMapping.mode",
            "delta.compatibility.symlinkFormatManifest.enabled",
            "delta.dataSkippingNumIndexedCols",
            "delta.dataSkippingStatsColumns",
            "delta.dataSkippingStringPrefixLength",
            "delta.enableChangeDataCapture",
            "delta.enableChangeDataFeed",
            "delta.enableDeletionVectors",
            "delta.enableIcebergCompatV1",
            "delta.enableIcebergCompatV2",
            "delta.enableIcebergCompatV3",
            "delta.enableIcebergWriterCompatV1",
            "delta.enableIcebergWriterCompatV3",
            "delta.enableInCommitTimestamps",
            "delta.enableInCommitTimestamps-preview",
            "delta.enableMaterializePartitionColumnsFeature",
            "delta.enableRowTracking",
            "delta.enableTypeWidening",
            "delta.enableVariantShredding",
            "delta.ignoreProtocolDefaults",
            "delta.inCommitTimestampEnablementTimestamp",
            "delta.inCommitTimestampEnablementVersion",
            "delta.isolationLevel",
            "delta.minReaderVersion",
            "delta.minWriterVersion",
            "delta.parquet.compression.codec",
            "delta.parquet.format.version",
            "delta.redirectReaderWriter-preview",
            "delta.redirectWriterOnly-preview",
            "delta.requireCheckpointProtectionBeforeVersion",
            "delta.randomPrefixLength",
            "delta.randomizeFilePrefixes",
            "delta.rowTracking.materializedRowCommitVersionColumnName",
            "delta.rowTracking.materializedRowIdColumnName",
            "delta.rowTrackingSuspended",
            "delta.universalFormat.enabledFormats",
            "delta.universalFormat.iceberg.atomicConversion.supported",
            "delta.writePartitionColumnsToParquet");
    private static final Set<String> PROTECTED_DELTA_PROPERTY_PREFIXES = ImmutableSet.of(
            "delta.columnMapping.",
            "delta.coordinatedCommits.",
            "delta.constraints.",
            "delta.feature.",
            "delta.identity.",
            "delta.rowTracking.");

    private final List<PropertyMetadata<?>> tableProperties;

    @SuppressWarnings("unchecked")
    @Inject
    public DeltaLakeTableProperties(DeltaLakeConfig config, TypeManager typeManager)
    {
        requireNonNull(config, "config is null");
        requireNonNull(typeManager, "typeManager is null");
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
                .add(new PropertyMetadata<>(
                        EXTRA_PROPERTIES_PROPERTY,
                        "Extra table properties",
                        new MapType(VARCHAR, VARCHAR, typeManager.getTypeOperators()),
                        Map.class,
                        null,
                        true,
                        value -> {
                            Map<String, String> extraProperties = (Map<String, String>) value;
                            if (extraProperties.containsValue(null)) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, "Extra table property value cannot be null '%s'".formatted(extraProperties));
                            }
                            if (extraProperties.containsKey(null)) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, "Extra table property key cannot be null '%s'".formatted(extraProperties));
                            }
                            return ImmutableMap.copyOf(extraProperties);
                        },
                        value -> value))
                .build();

        checkState(SUPPORTED_PROPERTIES.containsAll(tableProperties.stream()
                        .map(PropertyMetadata::getName)
                        .collect(toImmutableList())),
                "%s does not contain all supported properties",
                SUPPORTED_PROPERTIES);
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

    public static Optional<Map<String, String>> getExtraProperties(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Map<String, String>) tableProperties.get(EXTRA_PROPERTIES_PROPERTY));
    }

    public static void verifyExtraProperties(Set<String> basePropertyKeys, Map<String, String> extraProperties, Predicate<String> allowedExtraProperties)
    {
        Set<String> illegalExtraProperties = ImmutableSet.<String>builder()
                .addAll(intersection(
                        ImmutableSet.<String>builder()
                                .add(TABLE_COMMENT_PROPERTY)
                                .addAll(basePropertyKeys)
                                .addAll(SUPPORTED_PROPERTIES)
                                .build(),
                        extraProperties.keySet()))
                .addAll(extraProperties.keySet().stream()
                        .filter(DeltaLakeTableProperties::isProtectedDeltaProperty)
                        .collect(toImmutableSet()))
                .addAll(extraProperties.keySet().stream()
                        .filter(name -> !allowedExtraProperties.test(name))
                        .collect(toImmutableSet()))
                .build();

        if (!illegalExtraProperties.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Illegal keys in extra_properties: %s".formatted(illegalExtraProperties));
        }
    }

    private static boolean isProtectedDeltaProperty(String name)
    {
        return PROTECTED_DELTA_PROPERTIES.stream().anyMatch(name::equalsIgnoreCase) ||
                PROTECTED_DELTA_PROPERTY_PREFIXES.stream().anyMatch(prefix -> name.regionMatches(true, 0, prefix, 0, prefix.length()));
    }

    public static Map<String, String> addExtraProperties(Map<String, String> baseProperties, Map<String, String> extraProperties, Predicate<String> allowedExtraProperties)
    {
        verifyExtraProperties(baseProperties.keySet(), extraProperties, allowedExtraProperties);
        return ImmutableMap.<String, String>builder()
                .putAll(baseProperties)
                .putAll(extraProperties)
                .buildOrThrow();
    }
}
