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
package io.trino.plugin.hive.aws.athena;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.aws.athena.projection.Projection;
import io.trino.plugin.hive.aws.athena.projection.ProjectionFactory;
import io.trino.plugin.hive.aws.athena.projection.ProjectionType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.HiveTableProperties.getPartitionedBy;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_DIGITS;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_DIGITS_SUFFIX;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_FORMAT;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_FORMAT_SUFFIX;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL_SUFFIX;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL_UNIT;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_RANGE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_RANGE_SUFFIX;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_TYPE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_TYPE_SUFFIX;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_VALUES;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_VALUES_SUFFIX;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.METASTORE_PROPERTY_PROJECTION_ENABLED;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.METASTORE_PROPERTY_PROJECTION_IGNORE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.METASTORE_PROPERTY_PROJECTION_INTERVAL_UNIT_SUFFIX;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.PARTITION_PROJECTION_ENABLED;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.PARTITION_PROJECTION_IGNORE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.PARTITION_PROJECTION_LOCATION_TEMPLATE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.PROPERTY_KEY_PREFIX;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.getMetastoreProjectionPropertyKey;
import static io.trino.plugin.hive.aws.athena.projection.Projection.unsupportedProjectionColumnTypeException;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public final class PartitionProjectionService
{
    private final boolean partitionProjectionEnabled;
    private final Map<ProjectionType, ProjectionFactory> projectionFactories;
    private final TypeManager typeManager;

    @Inject
    public PartitionProjectionService(
            HiveConfig hiveConfig,
            Map<ProjectionType, ProjectionFactory> projectionFactories,
            TypeManager typeManager)
    {
        this.partitionProjectionEnabled = hiveConfig.isPartitionProjectionEnabled();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.projectionFactories = ImmutableMap.copyOf(requireNonNull(projectionFactories, "projectionFactories is null"));
    }

    public Map<String, Object> getPartitionProjectionTrinoTableProperties(Table table)
    {
        Map<String, String> metastoreTableProperties = table.getParameters();
        ImmutableMap.Builder<String, Object> trinoTablePropertiesBuilder = ImmutableMap.builder();
        rewriteProperty(metastoreTableProperties, trinoTablePropertiesBuilder, METASTORE_PROPERTY_PROJECTION_IGNORE, PARTITION_PROJECTION_IGNORE, Boolean::valueOf);
        rewriteProperty(metastoreTableProperties, trinoTablePropertiesBuilder, METASTORE_PROPERTY_PROJECTION_ENABLED, PARTITION_PROJECTION_ENABLED, Boolean::valueOf);
        rewriteProperty(metastoreTableProperties, trinoTablePropertiesBuilder, METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE, PARTITION_PROJECTION_LOCATION_TEMPLATE, String::valueOf);
        return trinoTablePropertiesBuilder.buildOrThrow();
    }

    public static Map<String, Object> getPartitionProjectionTrinoColumnProperties(Table table, String columnName)
    {
        Map<String, String> metastoreTableProperties = table.getParameters();
        return rewriteColumnProjectionProperties(metastoreTableProperties, columnName);
    }

    public Map<String, String> getPartitionProjectionHiveTableProperties(ConnectorTableMetadata tableMetadata)
    {
        // If partition projection is globally disabled we don't allow defining its properties
        if (!partitionProjectionEnabled && isAnyPartitionProjectionPropertyUsed(tableMetadata)) {
            throw columnProjectionException("Partition projection is disabled. Enable it in configuration by setting "
                    + HiveConfig.CONFIGURATION_HIVE_PARTITION_PROJECTION_ENABLED + "=true");
        }

        ImmutableMap.Builder<String, String> metastoreTablePropertiesBuilder = ImmutableMap.builder();
        // Handle Table Properties
        Map<String, Object> trinoTableProperties = tableMetadata.getProperties();
        rewriteProperty(
                trinoTableProperties,
                metastoreTablePropertiesBuilder,
                PARTITION_PROJECTION_IGNORE,
                METASTORE_PROPERTY_PROJECTION_IGNORE,
                value -> value.toString().toLowerCase(Locale.ENGLISH));
        rewriteProperty(
                trinoTableProperties,
                metastoreTablePropertiesBuilder,
                PARTITION_PROJECTION_ENABLED,
                METASTORE_PROPERTY_PROJECTION_ENABLED,
                value -> value.toString().toLowerCase(Locale.ENGLISH));
        rewriteProperty(
                trinoTableProperties,
                metastoreTablePropertiesBuilder,
                PARTITION_PROJECTION_LOCATION_TEMPLATE,
                METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE,
                Object::toString);

        // Handle Column Properties
        tableMetadata.getColumns().stream()
                .filter(columnMetadata -> !columnMetadata.getProperties().isEmpty())
                .forEach(columnMetadata -> {
                    Map<String, Object> columnProperties = columnMetadata.getProperties();
                    String columnName = columnMetadata.getName();
                    rewriteProperty(
                            columnProperties,
                            metastoreTablePropertiesBuilder,
                            COLUMN_PROJECTION_TYPE,
                            getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_TYPE_SUFFIX),
                            value -> ((ProjectionType) value).name().toLowerCase(Locale.ENGLISH));
                    rewriteProperty(
                            columnProperties,
                            metastoreTablePropertiesBuilder,
                            COLUMN_PROJECTION_VALUES,
                            getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_VALUES_SUFFIX),
                            value -> Joiner.on(",").join((List<String>) value));
                    rewriteProperty(
                            columnProperties,
                            metastoreTablePropertiesBuilder,
                            COLUMN_PROJECTION_RANGE,
                            getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_RANGE_SUFFIX),
                            value -> Joiner.on(",").join((List<String>) value));
                    rewriteProperty(
                            columnProperties,
                            metastoreTablePropertiesBuilder,
                            COLUMN_PROJECTION_INTERVAL,
                            getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_INTERVAL_SUFFIX),
                            value -> ((Integer) value).toString());
                    rewriteProperty(
                            columnProperties,
                            metastoreTablePropertiesBuilder,
                            COLUMN_PROJECTION_INTERVAL_UNIT,
                            getMetastoreProjectionPropertyKey(columnName, METASTORE_PROPERTY_PROJECTION_INTERVAL_UNIT_SUFFIX),
                            value -> ((ChronoUnit) value).name().toLowerCase(Locale.ENGLISH));
                    rewriteProperty(
                            columnProperties,
                            metastoreTablePropertiesBuilder,
                            COLUMN_PROJECTION_DIGITS,
                            getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_DIGITS_SUFFIX),
                            value -> ((Integer) value).toString());
                    rewriteProperty(
                            columnProperties,
                            metastoreTablePropertiesBuilder,
                            COLUMN_PROJECTION_FORMAT,
                            getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_FORMAT_SUFFIX),
                            String.class::cast);
                });

        // We initialize partition projection to validate properties.
        Map<String, String> metastoreTableProperties = metastoreTablePropertiesBuilder.buildOrThrow();
        List<String> partitionColumnNames = getPartitionedBy(tableMetadata.getProperties());
        createPartitionProjection(
                tableMetadata.getColumns()
                        .stream()
                        .map(ColumnMetadata::getName)
                        .collect(toImmutableList()),
                tableMetadata.getColumns().stream()
                        .filter(columnMetadata -> partitionColumnNames.contains(columnMetadata.getName()))
                        .collect(toImmutableMap(ColumnMetadata::getName, ColumnMetadata::getType)),
                metastoreTableProperties);

        return metastoreTableProperties;
    }

    private boolean isAnyPartitionProjectionPropertyUsed(ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getProperties().keySet().stream()
                .anyMatch(propertyKey -> propertyKey.startsWith(PROPERTY_KEY_PREFIX))) {
            return true;
        }
        return tableMetadata.getColumns().stream()
                .map(columnMetadata -> columnMetadata.getProperties().keySet())
                .flatMap(Set::stream)
                .anyMatch(propertyKey -> propertyKey.startsWith(PROPERTY_KEY_PREFIX));
    }

    public Optional<PartitionProjection> getPartitionProjectionFromTable(Table table)
    {
        if (!partitionProjectionEnabled) {
            return Optional.empty();
        }

        Map<String, String> tableProperties = table.getParameters();
        if (Optional.ofNullable(tableProperties.get(METASTORE_PROPERTY_PROJECTION_IGNORE))
                .map(Boolean::valueOf)
                .orElse(false)) {
            return Optional.empty();
        }

        return Optional.of(
                createPartitionProjection(
                        table.getDataColumns()
                                .stream()
                                .map(Column::getName)
                                .collect(toImmutableList()),
                        table.getPartitionColumns()
                                .stream().collect(toImmutableMap(
                                        Column::getName,
                                        column -> column.getType().getType(
                                                typeManager,
                                                DEFAULT_PRECISION))),
                        tableProperties));
    }

    private PartitionProjection createPartitionProjection(List<String> dataColumns, Map<String, Type> partitionColumns, Map<String, String> tableProperties)
    {
        Optional<Boolean> projectionEnabledProperty = Optional.ofNullable(tableProperties.get(METASTORE_PROPERTY_PROJECTION_ENABLED)).map(Boolean::valueOf);
        if (projectionEnabledProperty.orElse(false) && partitionColumns.size() < 1) {
            throw columnProjectionException("Partition projection can't be enabled when no partition columns are defined.");
        }

        Map<String, Projection> columnProjections = ImmutableSet.<String>builder()
                .addAll(partitionColumns.keySet())
                .addAll(dataColumns)
                .build()
                .stream()
                .collect(toImmutableMap(
                        identity(),
                        columnName -> rewriteColumnProjectionProperties(tableProperties, columnName)))
                .entrySet()
                .stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> {
                            String columnName = entry.getKey();
                            if (partitionColumns.containsKey(columnName)) {
                                return parseColumnProjection(columnName, partitionColumns.get(columnName), entry.getValue());
                            }
                            throw columnProjectionException("Partition projection can't be defined for non partition column: '" + columnName + "'");
                        }));

        Optional<String> storageLocationTemplate = Optional.ofNullable(tableProperties.get(METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE));
        if (projectionEnabledProperty.isPresent()) {
            for (String columnName : partitionColumns.keySet()) {
                if (!columnProjections.containsKey(columnName)) {
                    throw columnProjectionException("Partition projection definition for column: '" + columnName + "' missing");
                }
                if (storageLocationTemplate.isPresent()) {
                    String locationTemplate = storageLocationTemplate.get();
                    if (!locationTemplate.contains("${" + columnName + "}")) {
                        throw columnProjectionException(format("Partition projection location template: %s is missing partition column: '%s' placeholder", locationTemplate, columnName));
                    }
                }
            }
        }
        else if (!columnProjections.isEmpty()) {
            throw columnProjectionException(format(
                    "Columns %s projections are disallowed when partition projection property '%s' is missing",
                    columnProjections.keySet().stream().collect(Collectors.joining("', '", "['", "']")),
                    PARTITION_PROJECTION_ENABLED));
        }

        return new PartitionProjection(projectionEnabledProperty.orElse(false), storageLocationTemplate, columnProjections);
    }

    private static Map<String, Object> rewriteColumnProjectionProperties(Map<String, String> metastoreTableProperties, String columnName)
    {
        ImmutableMap.Builder<String, Object> trinoTablePropertiesBuilder = ImmutableMap.builder();
        rewriteProperty(
                metastoreTableProperties,
                trinoTablePropertiesBuilder,
                getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_TYPE_SUFFIX),
                COLUMN_PROJECTION_TYPE,
                value -> ProjectionType.valueOf(value.toUpperCase(Locale.ENGLISH)));
        rewriteProperty(
                metastoreTableProperties,
                trinoTablePropertiesBuilder,
                getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_VALUES_SUFFIX),
                COLUMN_PROJECTION_VALUES,
                PartitionProjectionService::splitCommaSeparatedString);
        rewriteProperty(
                metastoreTableProperties,
                trinoTablePropertiesBuilder,
                getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_RANGE_SUFFIX),
                COLUMN_PROJECTION_RANGE,
                PartitionProjectionService::splitCommaSeparatedString);
        rewriteProperty(
                metastoreTableProperties,
                trinoTablePropertiesBuilder,
                getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_INTERVAL_SUFFIX),
                COLUMN_PROJECTION_INTERVAL,
                Integer::valueOf);
        rewriteProperty(
                metastoreTableProperties,
                trinoTablePropertiesBuilder,
                getMetastoreProjectionPropertyKey(columnName, METASTORE_PROPERTY_PROJECTION_INTERVAL_UNIT_SUFFIX),
                COLUMN_PROJECTION_INTERVAL_UNIT,
                value -> ChronoUnit.valueOf(value.toUpperCase(Locale.ENGLISH)));
        rewriteProperty(
                metastoreTableProperties,
                trinoTablePropertiesBuilder,
                getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_DIGITS_SUFFIX),
                COLUMN_PROJECTION_DIGITS,
                Integer::valueOf);
        rewriteProperty(
                metastoreTableProperties,
                trinoTablePropertiesBuilder,
                getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_FORMAT_SUFFIX),
                COLUMN_PROJECTION_FORMAT,
                value -> value);
        return trinoTablePropertiesBuilder.buildOrThrow();
    }

    private Projection parseColumnProjection(String columnName, Type columnType, Map<String, Object> columnProperties)
    {
        ProjectionType projectionType = (ProjectionType) columnProperties.get(COLUMN_PROJECTION_TYPE);
        if (Objects.isNull(projectionType)) {
            throw columnProjectionException("Projection type property missing for column: '" + columnName + "'");
        }
        ProjectionFactory projectionFactory = Optional.ofNullable(projectionFactories.get(projectionType))
                .orElseThrow(() -> columnProjectionException(format("Partition projection type %s for column: '%s' not supported", projectionType, columnName)));
        if (!projectionFactory.isSupportedColumnType(columnType)) {
            throw unsupportedProjectionColumnTypeException(columnName, columnType);
        }
        return projectionFactory.create(columnName, columnType, columnProperties);
    }

    private static <I, V> void rewriteProperty(
            Map<String, I> sourceProperties,
            ImmutableMap.Builder<String, V> targetPropertiesBuilder,
            String sourcePropertyKey,
            String targetPropertyKey,
            Function<I, V> valueMapper)
    {
        Optional.ofNullable(sourceProperties.get(sourcePropertyKey))
                .ifPresent(value -> targetPropertiesBuilder.put(targetPropertyKey, valueMapper.apply(value)));
    }

    private TrinoException columnProjectionException(String message)
    {
        return new TrinoException(INVALID_COLUMN_PROPERTY, message);
    }

    private static List<String> splitCommaSeparatedString(String value)
    {
        return Splitter.on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(value);
    }
}
