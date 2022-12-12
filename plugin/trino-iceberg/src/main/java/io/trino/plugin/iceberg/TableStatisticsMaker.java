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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isExtendedStatisticsEnabled;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class TableStatisticsMaker
{
    public static final String TRINO_STATS_PREFIX = "trino.stats.ndv.";
    public static final String TRINO_STATS_NDV_FORMAT = TRINO_STATS_PREFIX + "%d.ndv";
    public static final Pattern TRINO_STATS_COLUMN_ID_PATTERN = Pattern.compile(Pattern.quote(TRINO_STATS_PREFIX) + "(?<columnId>\\d+)\\..*");
    public static final Pattern TRINO_STATS_NDV_PATTERN = Pattern.compile(Pattern.quote(TRINO_STATS_PREFIX) + "(?<columnId>\\d+)\\.ndv");

    private final TypeManager typeManager;
    private final ConnectorSession session;
    private final Table icebergTable;

    private TableStatisticsMaker(TypeManager typeManager, ConnectorSession session, Table icebergTable)
    {
        this.typeManager = typeManager;
        this.session = session;
        this.icebergTable = icebergTable;
    }

    public static TableStatistics getTableStatistics(TypeManager typeManager, ConnectorSession session, IcebergTableHandle tableHandle, Table icebergTable)
    {
        return new TableStatisticsMaker(typeManager, session, icebergTable).makeTableStatistics(tableHandle);
    }

    private TableStatistics makeTableStatistics(IcebergTableHandle tableHandle)
    {
        if (tableHandle.getSnapshotId().isEmpty()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        TupleDomain<IcebergColumnHandle> enforcedPredicate = tableHandle.getEnforcedPredicate();

        if (enforcedPredicate.isNone()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        Schema icebergTableSchema = icebergTable.schema();
        List<Types.NestedField> columns = icebergTableSchema.columns();

        List<IcebergColumnHandle> columnHandles = getColumns(icebergTableSchema, typeManager);
        Map<Integer, IcebergColumnHandle> idToColumnHandle = columnHandles.stream()
                .collect(toUnmodifiableMap(IcebergColumnHandle::getId, identity()));
        Map<Integer, org.apache.iceberg.types.Type> idToType = columns.stream()
                .map(column -> Maps.immutableEntry(column.fieldId(), column.type()))
                .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(enforcedPredicate))
                .useSnapshot(tableHandle.getSnapshotId().get())
                .includeColumnStats();

        IcebergStatistics.Builder icebergStatisticsBuilder = new IcebergStatistics.Builder(columns, typeManager);
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            fileScanTasks.forEach(fileScanTask -> icebergStatisticsBuilder.acceptDataFile(fileScanTask.file(), fileScanTask.spec()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        IcebergStatistics summary = icebergStatisticsBuilder.build();

        if (summary.getFileCount() == 0) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        Map<Integer, Long> ndvs = readNdvs();

        ImmutableMap.Builder<ColumnHandle, ColumnStatistics> columnHandleBuilder = ImmutableMap.builder();
        double recordCount = summary.getRecordCount();
        for (Map.Entry<Integer, IcebergColumnHandle> columnHandleTuple : idToColumnHandle.entrySet()) {
            IcebergColumnHandle columnHandle = columnHandleTuple.getValue();
            int fieldId = columnHandle.getId();
            ColumnStatistics.Builder columnBuilder = new ColumnStatistics.Builder();
            Long nullCount = summary.getNullCounts().get(fieldId);
            if (nullCount != null) {
                columnBuilder.setNullsFraction(Estimate.of(nullCount / recordCount));
            }
            if (idToType.get(columnHandleTuple.getKey()).typeId() == Type.TypeID.FIXED) {
                Types.FixedType fixedType = (Types.FixedType) idToType.get(columnHandleTuple.getKey());
                long columnSize = fixedType.length();
                columnBuilder.setDataSize(Estimate.of(columnSize));
            }
            else if (summary.getColumnSizes() != null) {
                Long columnSize = summary.getColumnSizes().get(fieldId);
                if (columnSize != null) {
                    // columnSize is the size on disk and Trino column stats is size in memory.
                    // The relation between the two is type and data dependent.
                    // However, Trino currently does not use data size statistics for fixed-width types
                    // (it's not needed for them), so do not report it at all, to avoid reporting some bogus value.
                    if (!(columnHandle.getBaseType() instanceof FixedWidthType)) {
                        if (columnHandle.getBaseType() == VARCHAR) {
                            // Tested using item table from TPCDS benchmark
                            // compared column size of item_desc column stored inside files
                            // with length of values in that column reported by trino
                            columnSize = (long) (columnSize * 2.7);
                        }
                        else if (columnHandle.getBaseType() == VARBINARY) {
                            // Tested using VARBINARY columns with random, both in length and content, data
                            // compared column size stored inside parquet files with length of bytes written to it
                            // Data used for testing came from alpha numeric characters so it was not very real life like scenario
                            // In the future better heuristic could be used here
                            columnSize = (long) (columnSize * 1.4);
                        }
                        columnBuilder.setDataSize(Estimate.of(columnSize));
                    }
                }
            }
            Object min = summary.getMinValues().get(fieldId);
            Object max = summary.getMaxValues().get(fieldId);
            if (min != null && max != null) {
                columnBuilder.setRange(DoubleRange.from(columnHandle.getType(), min, max));
            }
            columnBuilder.setDistinctValuesCount(
                    Optional.ofNullable(ndvs.get(fieldId))
                            .map(Estimate::of)
                            .orElseGet(Estimate::unknown));
            columnHandleBuilder.put(columnHandle, columnBuilder.build());
        }
        return new TableStatistics(Estimate.of(recordCount), columnHandleBuilder.buildOrThrow());
    }

    private Map<Integer, Long> readNdvs()
    {
        if (!isExtendedStatisticsEnabled(session)) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<Integer, Long> ndvByColumnId = ImmutableMap.builder();
        icebergTable.properties().forEach((key, value) -> {
            if (key.startsWith(TRINO_STATS_PREFIX)) {
                Matcher matcher = TRINO_STATS_NDV_PATTERN.matcher(key);
                if (matcher.matches()) {
                    int columnId = Integer.parseInt(matcher.group("columnId"));
                    long ndv = Long.parseLong(value);
                    ndvByColumnId.put(columnId, ndv);
                }
            }
        });
        return ndvByColumnId.buildOrThrow();
    }
}
