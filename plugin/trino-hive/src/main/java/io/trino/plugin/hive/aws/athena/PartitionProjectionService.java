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
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import static io.trino.plugin.hive.aws.athena.ProjectionType.DATE;
import static io.trino.plugin.hive.aws.athena.ProjectionType.ENUM;
import static io.trino.plugin.hive.aws.athena.ProjectionType.INJECTED;
import static io.trino.plugin.hive.aws.athena.ProjectionType.INTEGER;
import static java.lang.String.format;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public final class PartitionProjectionService
{
    private final boolean partitionProjectionEnabled;
    private final Map<ProjectionType, ProjectionFactory> projectionFactories;
    private final TypeManager typeManager;

    @Inject
    public PartitionProjectionService(HiveConfig hiveConfig, TypeManager typeManager)
    {
        this.partitionProjectionEnabled = hiveConfig.isPartitionProjectionEnabled();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.projectionFactories = ImmutableMap.<ProjectionType, ProjectionFactory>builder()
                .put(ENUM, new EnumProjectionFactory())
                .put(INTEGER, new IntegerProjectionFactory())
                .put(DATE, new DateProjectionFactory())
                .put(INJECTED, new InjectedProjectionFactory())
                .buildOrThrow();
    }

    public static Map<String, Object> getPartitionProjectionTrinoTableProperties(Table table)
    {
        Map<String, String> metastoreTableProperties = table.getParameters();
        ImmutableMap.Builder<String, Object> trinoTablePropertiesBuilder = ImmutableMap.builder();

        String ignore = metastoreTableProperties.get(METASTORE_PROPERTY_PROJECTION_IGNORE);
        if (ignore != null) {
            trinoTablePropertiesBuilder.put(PARTITION_PROJECTION_IGNORE, Boolean.valueOf(ignore));
        }

        String enabled = metastoreTableProperties.get(METASTORE_PROPERTY_PROJECTION_ENABLED);
        if (enabled != null) {
            trinoTablePropertiesBuilder.put(PARTITION_PROJECTION_ENABLED, Boolean.valueOf(enabled));
        }

        String locationTemplate = metastoreTableProperties.get(METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE);
        if (locationTemplate != null) {
            trinoTablePropertiesBuilder.put(PARTITION_PROJECTION_LOCATION_TEMPLATE, locationTemplate);
        }

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
            throw new InvalidProjectionException("Partition projection is disabled. Enable it in configuration by setting "
                    + HiveConfig.CONFIGURATION_HIVE_PARTITION_PROJECTION_ENABLED + "=true");
        }

        ImmutableMap.Builder<String, String> metastoreTablePropertiesBuilder = ImmutableMap.builder();
        // Handle Table Properties
        Map<String, Object> trinoTableProperties = tableMetadata.getProperties();

        Object ignore = trinoTableProperties.get(PARTITION_PROJECTION_IGNORE);
        if (ignore != null) {
            metastoreTablePropertiesBuilder.put(METASTORE_PROPERTY_PROJECTION_IGNORE, ignore.toString().toLowerCase(ROOT));
        }

        Object enabled = trinoTableProperties.get(PARTITION_PROJECTION_ENABLED);
        if (enabled != null) {
            metastoreTablePropertiesBuilder.put(METASTORE_PROPERTY_PROJECTION_ENABLED, enabled.toString().toLowerCase(ROOT));
        }

        Object locationTemplate = trinoTableProperties.get(PARTITION_PROJECTION_LOCATION_TEMPLATE);
        if (locationTemplate != null) {
            metastoreTablePropertiesBuilder.put(METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE, locationTemplate.toString());
        }

        // Handle Column Properties
        tableMetadata.getColumns().stream()
                .filter(columnMetadata -> !columnMetadata.getProperties().isEmpty())
                .forEach(columnMetadata -> {
                    Map<String, Object> columnProperties = columnMetadata.getProperties();
                    String columnName = columnMetadata.getName();

                    if (columnProperties.get(COLUMN_PROJECTION_TYPE) instanceof ProjectionType projectionType) {
                        metastoreTablePropertiesBuilder.put(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_TYPE_SUFFIX), projectionType.name().toLowerCase(ROOT));
                    }

                    if (columnProperties.get(COLUMN_PROJECTION_VALUES) instanceof List<?> values) {
                        metastoreTablePropertiesBuilder.put(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_VALUES_SUFFIX), Joiner.on(",").join(values));
                    }

                    if (columnProperties.get(COLUMN_PROJECTION_RANGE) instanceof List<?> range) {
                        metastoreTablePropertiesBuilder.put(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_RANGE_SUFFIX), Joiner.on(",").join(range));
                    }

                    if (columnProperties.get(COLUMN_PROJECTION_INTERVAL) instanceof Integer interval) {
                        metastoreTablePropertiesBuilder.put(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_INTERVAL_SUFFIX), interval.toString());
                    }

                    if (columnProperties.get(COLUMN_PROJECTION_INTERVAL_UNIT) instanceof ChronoUnit intervalUnit) {
                        metastoreTablePropertiesBuilder.put(getMetastoreProjectionPropertyKey(columnName, METASTORE_PROPERTY_PROJECTION_INTERVAL_UNIT_SUFFIX), intervalUnit.name().toLowerCase(ROOT));
                    }

                    if (columnProperties.get(COLUMN_PROJECTION_DIGITS) instanceof Integer digits) {
                        metastoreTablePropertiesBuilder.put(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_DIGITS_SUFFIX), digits.toString());
                    }

                    if (columnProperties.get(COLUMN_PROJECTION_FORMAT) instanceof String format) {
                        metastoreTablePropertiesBuilder.put(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_FORMAT_SUFFIX), format);
                    }
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

    private static boolean isAnyPartitionProjectionPropertyUsed(ConnectorTableMetadata tableMetadata)
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

    Optional<PartitionProjection> getPartitionProjectionFromTable(Table table)
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
        if (projectionEnabledProperty.orElse(false) && partitionColumns.isEmpty()) {
            throw new InvalidProjectionException("Partition projection can't be enabled when no partition columns are defined.");
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
                            throw new InvalidProjectionException("Partition projection can't be defined for non partition column: '" + columnName + "'");
                        }));

        Optional<String> storageLocationTemplate = Optional.ofNullable(tableProperties.get(METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE));
        if (projectionEnabledProperty.isPresent()) {
            for (String columnName : partitionColumns.keySet()) {
                if (!columnProjections.containsKey(columnName)) {
                    throw new InvalidProjectionException("Partition projection definition for column: '" + columnName + "' missing");
                }
                if (storageLocationTemplate.isPresent()) {
                    String locationTemplate = storageLocationTemplate.get();
                    if (!locationTemplate.contains("${" + columnName + "}")) {
                        throw new InvalidProjectionException(format("Partition projection location template: %s is missing partition column: '%s' placeholder", locationTemplate, columnName));
                    }
                }
            }
        }
        else if (!columnProjections.isEmpty()) {
            throw new InvalidProjectionException(format(
                    "Columns %s projections are disallowed when partition projection property '%s' is missing",
                    columnProjections.keySet().stream().collect(Collectors.joining("', '", "['", "']")),
                    PARTITION_PROJECTION_ENABLED));
        }

        return new PartitionProjection(projectionEnabledProperty.orElse(false), storageLocationTemplate, columnProjections);
    }

    private static Map<String, Object> rewriteColumnProjectionProperties(Map<String, String> metastoreTableProperties, String columnName)
    {
        ImmutableMap.Builder<String, Object> trinoTablePropertiesBuilder = ImmutableMap.builder();

        String type = metastoreTableProperties.get(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_TYPE_SUFFIX));
        if (type != null) {
            trinoTablePropertiesBuilder.put(COLUMN_PROJECTION_TYPE, ProjectionType.valueOf(type.toUpperCase(ROOT)));
        }

        String values = metastoreTableProperties.get(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_VALUES_SUFFIX));
        if (values != null) {
            trinoTablePropertiesBuilder.put(COLUMN_PROJECTION_VALUES, splitCommaSeparatedString(values));
        }

        String range = metastoreTableProperties.get(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_RANGE_SUFFIX));
        if (range != null) {
            trinoTablePropertiesBuilder.put(COLUMN_PROJECTION_RANGE, splitCommaSeparatedString(range));
        }

        String interval = metastoreTableProperties.get(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_INTERVAL_SUFFIX));
        if (interval != null) {
            trinoTablePropertiesBuilder.put(COLUMN_PROJECTION_INTERVAL, Integer.valueOf(interval));
        }

        String intervalUnit = metastoreTableProperties.get(getMetastoreProjectionPropertyKey(columnName, METASTORE_PROPERTY_PROJECTION_INTERVAL_UNIT_SUFFIX));
        if (intervalUnit != null) {
            trinoTablePropertiesBuilder.put(COLUMN_PROJECTION_INTERVAL_UNIT, ChronoUnit.valueOf(intervalUnit.toUpperCase(ROOT)));
        }

        String digits = metastoreTableProperties.get(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_DIGITS_SUFFIX));
        if (digits != null) {
            trinoTablePropertiesBuilder.put(COLUMN_PROJECTION_DIGITS, Integer.valueOf(digits));
        }

        String format = metastoreTableProperties.get(getMetastoreProjectionPropertyKey(columnName, COLUMN_PROJECTION_FORMAT_SUFFIX));
        if (format != null) {
            trinoTablePropertiesBuilder.put(COLUMN_PROJECTION_FORMAT, format);
        }

        return trinoTablePropertiesBuilder.buildOrThrow();
    }

    private Projection parseColumnProjection(String columnName, Type columnType, Map<String, Object> columnProperties)
    {
        ProjectionType projectionType = (ProjectionType) columnProperties.get(COLUMN_PROJECTION_TYPE);
        if (Objects.isNull(projectionType)) {
            throw new InvalidProjectionException(columnName, "Projection type property missing for column: '" + columnName + "'");
        }
        ProjectionFactory projectionFactory = Optional.ofNullable(projectionFactories.get(projectionType))
                .orElseThrow(() -> new InvalidProjectionException(columnName, format("Partition projection type %s for column: '%s' not supported", projectionType, columnName)));
        if (!projectionFactory.isSupportedColumnType(columnType)) {
            throw new InvalidProjectionException(columnName, columnType);
        }
        return projectionFactory.create(columnName, columnType, columnProperties);
    }

    private static List<String> splitCommaSeparatedString(String value)
    {
        return Splitter.on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(value);
    }
}
