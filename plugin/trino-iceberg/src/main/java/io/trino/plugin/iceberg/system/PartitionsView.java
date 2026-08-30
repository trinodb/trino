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
package io.trino.plugin.iceberg.system;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types.NestedField;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getPartitionColumnType;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public final class PartitionsView
{
    private static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();

    private PartitionsView() {}

    public static ConnectorViewDefinition create(TypeManager typeManager, Table icebergTable, String catalogName, String schemaName, String tableName)
    {
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(icebergTable, "icebergTable is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");

        ImmutableList.Builder<ViewColumn> viewColumns = ImmutableList.builder();
        Optional<IcebergPartitionColumn> partitionType = getPartitionColumnType(typeManager, getAllPartitionFields(icebergTable), icebergTable.schema());

        boolean hasPartitionColumn;
        if (partitionType.isPresent()) {
            hasPartitionColumn = true;
            viewColumns.add(new ViewColumn("partition", partitionType.get().rowType().getTypeId(), Optional.empty()));
        }
        else {
            hasPartitionColumn = false;
        }

        Stream.of(
                        "record_count",
                        "file_count",
                        "total_size",
                        "position_delete_record_count",
                        "position_delete_file_count",
                        "equality_delete_record_count",
                        "equality_delete_file_count")
                .forEach(column -> viewColumns.add(new ViewColumn(column, BIGINT.getTypeId(), Optional.empty())));

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toImmutableSet());

        List<NestedField> nonPartitionPrimitiveColumns = icebergTable.schema().columns().stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        Optional<RowType> dataColumnType = getMetricsColumnType(typeManager, nonPartitionPrimitiveColumns);

        // The $files table has one row per content file, so every aggregate is restricted to the content it reports on
        String dataFileFilter = contentFilter(FileContent.DATA);
        String positionDeleteFilter = contentFilter(FileContent.POSITION_DELETES);
        String equalityDeleteFilter = contentFilter(FileContent.EQUALITY_DELETES);

        boolean hasDataColumn;
        String dataAggregationSql;
        if (dataColumnType.isPresent()) {
            hasDataColumn = true;
            viewColumns.add(new ViewColumn("data", dataColumnType.get().getTypeId(), Optional.empty()));
            dataAggregationSql = buildDataAggregation(typeManager, nonPartitionPrimitiveColumns, dataFileFilter);
        }
        else {
            hasDataColumn = false;
            dataAggregationSql = "";
        }

        String viewSql =
                """
                SELECT %1$s
                    COALESCE(SUM(record_count) %2$s, 0) AS record_count,
                    COUNT(*) %2$s AS file_count,
                    COALESCE(SUM(file_size_in_bytes) %2$s, 0) AS total_size,
                    COALESCE(SUM(record_count) %3$s, 0) AS position_delete_record_count,
                    COUNT(*) %3$s AS position_delete_file_count,
                    COALESCE(SUM(record_count) %4$s, 0) AS equality_delete_record_count,
                    COUNT(*) %4$s AS equality_delete_file_count%5$s
                FROM %6$s.%7$s.%8$s
                %9$s
                """.formatted(
                        hasPartitionColumn ? "partition," : "",
                        dataFileFilter,
                        positionDeleteFilter,
                        equalityDeleteFilter,
                        hasDataColumn ? ", " + dataAggregationSql : "",
                        quoted(catalogName),
                        quoted(schemaName),
                        quoted(tableName + "$files"),
                        hasPartitionColumn ? "GROUP BY 1" : "");

        return new ConnectorViewDefinition(
                viewSql,
                Optional.of(catalogName),
                Optional.of(schemaName),
                viewColumns.build(),
                Optional.empty(),
                Optional.empty(),
                true,
                ImmutableList.of());
    }

    private static String contentFilter(FileContent content)
    {
        return "FILTER (WHERE content = %d)".formatted(content.id());
    }

    private static String buildDataAggregation(TypeManager typeManager, List<NestedField> nonPartitionColumns, String dataFileFilter)
    {
        ImmutableList.Builder<String> rowValues = ImmutableList.builder();
        ImmutableList.Builder<String> rowTypes = ImmutableList.builder();

        for (NestedField column : nonPartitionColumns) {
            String trinoTypeDisplayName = toTrinoType(column.type(), typeManager).getDisplayName();
            rowValues.add(buildColumnAggregation(column.fieldId(), dataFileFilter));
            rowTypes.add(buildColumnRowType(column.name(), trinoTypeDisplayName));
        }

        return "CAST(ROW(%s) AS ROW(%s)) AS data".formatted(COMMA_JOINER.join(rowValues.build()), COMMA_JOINER.join(rowTypes.build()));
    }

    private static String buildColumnAggregation(int fieldId, String dataFileFilter)
    {
        String min = "MIN(lower_bounds.\"%1$d\") %2$s".formatted(fieldId, dataFileFilter);
        String max = "MAX(upper_bounds.\"%1$d\") %2$s".formatted(fieldId, dataFileFilter);
        String nullCount = "SUM(element_at(null_value_counts, %d)) %s".formatted(fieldId, dataFileFilter);
        String nanCount = "SUM(element_at(nan_value_counts, %d)) %s".formatted(fieldId, dataFileFilter);

        // we need this case to ensure that it is compatible with the current $partitions implementation
        return """
               CASE
                   WHEN %1$s IS NULL AND %2$s IS NULL AND %3$s IS NULL THEN NULL
                   ELSE ROW(%1$s, %2$s, %3$s, %4$s)
               END
               """
                .formatted(min, max, nullCount, nanCount);
    }

    private static String buildColumnRowType(String columnName, String trinoTypeDisplayName)
    {
        return "%s ROW(min %2$s, max %2$s, null_count BIGINT, nan_count BIGINT)"
                .formatted(quoted(columnName), trinoTypeDisplayName);
    }

    // identifiers embedded in the generated view SQL are always quoted, since a bare reserved word (e.g. a column named "group") would not parse
    private static String quoted(String name)
    {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    private static Optional<RowType> getMetricsColumnType(TypeManager typeManager, List<NestedField> columns)
    {
        List<RowType.Field> metricColumns = columns.stream()
                .map(column -> {
                    Type trinoType = toTrinoType(column.type(), typeManager);
                    return RowType.field(
                            column.name(),
                            RowType.from(ImmutableList.of(
                                    new RowType.Field(Optional.of("min"), trinoType),
                                    new RowType.Field(Optional.of("max"), trinoType),
                                    new RowType.Field(Optional.of("null_count"), BIGINT),
                                    new RowType.Field(Optional.of("nan_count"), BIGINT))));
                })
                .collect(toImmutableList());

        if (metricColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(RowType.from(metricColumns));
    }
}
