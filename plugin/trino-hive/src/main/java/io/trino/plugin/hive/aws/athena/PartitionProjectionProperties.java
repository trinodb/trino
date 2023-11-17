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
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.HiveTableProperties.getPartitionedBy;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.util.Locale.ROOT;

public final class PartitionProjectionProperties
{
    private static final String COLUMN_PROJECTION_TYPE_SUFFIX = "type";
    private static final String COLUMN_PROJECTION_VALUES_SUFFIX = "values";
    private static final String COLUMN_PROJECTION_RANGE_SUFFIX = "range";
    private static final String COLUMN_PROJECTION_INTERVAL_SUFFIX = "interval";
    private static final String COLUMN_PROJECTION_DIGITS_SUFFIX = "digits";
    private static final String COLUMN_PROJECTION_FORMAT_SUFFIX = "format";
    private static final String METASTORE_PROPERTY_PROJECTION_INTERVAL_UNIT_SUFFIX = "interval.unit";
    private static final String METASTORE_PROPERTY_PROJECTION_ENABLED = "projection.enabled";
    private static final String METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE = "storage.location.template";
    private static final String METASTORE_PROPERTY_PROJECTION_IGNORE = "trino.partition_projection.ignore";
    private static final String PROPERTY_KEY_PREFIX = "partition_projection_";

    public static final String COLUMN_PROJECTION_FORMAT = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_FORMAT_SUFFIX;
    public static final String COLUMN_PROJECTION_DIGITS = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_DIGITS_SUFFIX;
    public static final String COLUMN_PROJECTION_INTERVAL_UNIT = PROPERTY_KEY_PREFIX + "interval_unit";
    public static final String COLUMN_PROJECTION_INTERVAL = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_INTERVAL_SUFFIX;
    public static final String COLUMN_PROJECTION_RANGE = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_RANGE_SUFFIX;
    public static final String COLUMN_PROJECTION_VALUES = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_VALUES_SUFFIX;
    public static final String COLUMN_PROJECTION_TYPE = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_TYPE_SUFFIX;
    public static final String PARTITION_PROJECTION_IGNORE = PROPERTY_KEY_PREFIX + "ignore";
    public static final String PARTITION_PROJECTION_LOCATION_TEMPLATE = PROPERTY_KEY_PREFIX + "location_template";
    public static final String PARTITION_PROJECTION_ENABLED = PROPERTY_KEY_PREFIX + "enabled";

    private PartitionProjectionProperties() {}

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

    public static Map<String, String> getPartitionProjectionHiveTableProperties(ConnectorTableMetadata tableMetadata)
    {
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
        Set<String> partitionColumnNames = ImmutableSet.copyOf(getPartitionedBy(tableMetadata.getProperties()));
        createPartitionProjection(
                tableMetadata.getColumns()
                        .stream()
                        .map(ColumnMetadata::getName)
                        .filter(name -> !partitionColumnNames.contains(name))
                        .collect(toImmutableList()),
                tableMetadata.getColumns().stream()
                        .filter(columnMetadata -> partitionColumnNames.contains(columnMetadata.getName()))
                        .collect(toImmutableMap(ColumnMetadata::getName, ColumnMetadata::getType)),
                metastoreTableProperties);

        return metastoreTableProperties;
    }

    public static boolean arePartitionProjectionPropertiesSet(ConnectorTableMetadata tableMetadata)
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

    static Optional<PartitionProjection> getPartitionProjectionFromTable(Table table, TypeManager typeManager)
    {
        Map<String, String> tableProperties = table.getParameters();
        if (parseBoolean(tableProperties.get(METASTORE_PROPERTY_PROJECTION_IGNORE)) ||
                !parseBoolean(tableProperties.get(METASTORE_PROPERTY_PROJECTION_ENABLED))) {
            return Optional.empty();
        }

        Set<String> partitionColumnNames = table.getPartitionColumns().stream().map(Column::getName).collect(Collectors.toSet());
        return createPartitionProjection(
                table.getDataColumns().stream()
                        .map(Column::getName)
                        .filter(partitionColumnNames::contains)
                        .collect(toImmutableList()),
                table.getPartitionColumns().stream()
                        .collect(toImmutableMap(Column::getName, column -> column.getType().getType(typeManager, DEFAULT_PRECISION))),
                tableProperties);
    }

    private static Optional<PartitionProjection> createPartitionProjection(List<String> dataColumns, Map<String, Type> partitionColumns, Map<String, String> tableProperties)
    {
        // This method is used during table creation to validate the properties. The validation is performed even if the projection is disabled.
        boolean enabled = parseBoolean(tableProperties.get(METASTORE_PROPERTY_PROJECTION_ENABLED));

        if (!tableProperties.containsKey(METASTORE_PROPERTY_PROJECTION_ENABLED) &&
                partitionColumns.keySet().stream().anyMatch(columnName -> !rewriteColumnProjectionProperties(tableProperties, columnName).isEmpty())) {
            throw new InvalidProjectionException("Columns partition projection properties cannot be set when '%s' is not set".formatted(PARTITION_PROJECTION_ENABLED));
        }

        if (enabled && partitionColumns.isEmpty()) {
            throw new InvalidProjectionException("Partition projection cannot be enabled on a table that is not partitioned");
        }

        for (String columnName : dataColumns) {
            if (!rewriteColumnProjectionProperties(tableProperties, columnName).isEmpty()) {
                throw new InvalidProjectionException("Partition projection cannot be defined for non-partition column: '" + columnName + "'");
            }
        }

        Map<String, Projection> columnProjections = new HashMap<>();
        partitionColumns.forEach((columnName, type) -> {
            Map<String, Object> columnProperties = rewriteColumnProjectionProperties(tableProperties, columnName);
            if (enabled) {
                columnProjections.put(columnName, parseColumnProjection(columnName, type, columnProperties));
            }
        });

        Optional<String> storageLocationTemplate = Optional.ofNullable(tableProperties.get(METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE));
        storageLocationTemplate.ifPresent(locationTemplate -> {
            for (String columnName : partitionColumns.keySet()) {
                if (!locationTemplate.contains("${" + columnName + "}")) {
                    throw new InvalidProjectionException(format("Partition projection location template: %s is missing partition column: '%s' placeholder", locationTemplate, columnName));
                }
            }
        });

        if (!enabled) {
            return Optional.empty();
        }
        return Optional.of(new PartitionProjection(storageLocationTemplate, columnProjections));
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

    private static Projection parseColumnProjection(String columnName, Type columnType, Map<String, Object> columnProperties)
    {
        ProjectionType projectionType = (ProjectionType) columnProperties.get(COLUMN_PROJECTION_TYPE);
        if (projectionType == null) {
            throw new InvalidProjectionException(columnName, "Projection type property missing");
        }
        return switch (projectionType) {
            case ENUM -> new EnumProjection(columnName, columnType, columnProperties);
            case INTEGER -> new IntegerProjection(columnName, columnType, columnProperties);
            case DATE -> new DateProjection(columnName, columnType, columnProperties);
            case INJECTED -> new InjectedProjection(columnName, columnType);
        };
    }

    private static List<String> splitCommaSeparatedString(String value)
    {
        return Splitter.on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(value);
    }

    private static String getMetastoreProjectionPropertyKey(String columnName, String propertyKeySuffix)
    {
        return "projection" + "." + columnName + "." + propertyKeySuffix;
    }

    static <T, I> T getProjectionPropertyRequiredValue(
            String columnName,
            Map<String, I> columnProjectionProperties,
            String propertyKey,
            Function<I, T> decoder)
    {
        return getProjectionPropertyValue(columnProjectionProperties, propertyKey, decoder)
                .orElseThrow(() -> new InvalidProjectionException(columnName, format("Missing required property: '%s'", propertyKey)));
    }

    static <T, I> Optional<T> getProjectionPropertyValue(
            Map<String, I> columnProjectionProperties,
            String propertyKey,
            Function<I, T> decoder)
    {
        return Optional.ofNullable(
                        columnProjectionProperties.get(propertyKey))
                .map(decoder);
    }
}
