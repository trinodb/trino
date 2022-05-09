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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.stats.BlobMetadata;
import org.apache.iceberg.stats.FileMetadata;
import org.apache.iceberg.stats.StatsReader;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.Stats.NDV_STATS;
import static io.trino.plugin.iceberg.Stats.bytesLittleEndianToLong;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class TableStatisticsMaker
{
    private final TypeManager typeManager;
    private final FileIoProvider fileIoProvider;

    @Inject
    public TableStatisticsMaker(TypeManager typeManager, FileIoProvider fileIoProvider)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");
    }

    public TableStatistics getTableStatistics(ConnectorSession session, IcebergTableHandle tableHandle, Table icebergTable)
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

        Map<Integer, Long> ndvs = readNdvs(session, icebergTable);

        Schema icebergTableSchema = icebergTable.schema();
        List<Types.NestedField> columns = icebergTableSchema.columns();

        List<IcebergColumnHandle> columnHandles = getColumns(icebergTableSchema, typeManager);
        Map<Integer, IcebergColumnHandle> idToColumnHandle = columnHandles.stream()
                .collect(toUnmodifiableMap(IcebergColumnHandle::getId, identity()));

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

        ImmutableMap.Builder<ColumnHandle, ColumnStatistics> columnHandleBuilder = ImmutableMap.builder();
        double recordCount = summary.getRecordCount();
        for (IcebergColumnHandle columnHandle : idToColumnHandle.values()) {
            int fieldId = columnHandle.getId();
            ColumnStatistics.Builder columnBuilder = new ColumnStatistics.Builder();
            Long nullCount = summary.getNullCounts().get(fieldId);
            if (nullCount != null) {
                columnBuilder.setNullsFraction(Estimate.of(nullCount / recordCount));
            }
            if (summary.getColumnSizes() != null) {
                Long columnSize = summary.getColumnSizes().get(fieldId);
                if (columnSize != null) {
                    columnBuilder.setDataSize(Estimate.of(columnSize));
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

    private Map<Integer, Long> readNdvs(ConnectorSession session, Table icebergTable)
    {
        Map<Integer, Long> ndvByColumnId = new HashMap<>();
        try (FileIO fileIo = fileIoProvider.createFileIo(new HdfsContext(session), session.getQueryId())) {
            getStatisticsFiles(icebergTable)
                    .filter(statisticsFile -> statisticsFile.statistics().containsKey(NDV_STATS))
                    // newest first
                    .sorted(comparing(StatisticsFile::sequenceNumber).reversed())
                    .forEach(statisticsFile -> {
                        Set<List<Integer>> columnSets = statisticsFile.statistics().get(NDV_STATS);
                        Set<Integer> columnIds = columnSets.stream()
                                .filter(columnSet -> columnSet.size() == 1)
                                .map(Iterables::getOnlyElement)
                                .collect(toImmutableSet());
                        Set<Integer> newColumnIds = Sets.difference(columnIds, ndvByColumnId.keySet());
                        if (newColumnIds.isEmpty()) {
                            // All columns covered by the stats file already have known NDV.
                            return;
                        }

                        try (StatsReader statsReader = new StatsReader(
                                fileIo.newInputFile(statisticsFile.location()),
                                statisticsFile.fileSizeInBytes(),
                                statisticsFile.fileFooterSizeInBytes())) {
                            FileMetadata fileMetadata = statsReader.fileMetadata();
                            ImmutableList<BlobMetadata> blobsToRead = fileMetadata.blobs().stream()
                                    .filter(blob -> blob.type().equals(NDV_STATS))
                                    .filter(blob -> blob.columns().size() == 1 && newColumnIds.contains(getOnlyElement(blob.columns())))
                                    .collect(toImmutableList());
                            statsReader.readAll(blobsToRead).forEach(entry -> {
                                int columnId = getOnlyElement(entry.first().columns());
                                long ndv = bytesLittleEndianToLong(entry.second());
                                Long previous = ndvByColumnId.put(columnId, ndv);
                                verify(previous == null, "(%s, %s) entry already exists when putting (%s, %s) ", columnId, previous, columnId, ndv);
                            });
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
        return ndvByColumnId;
    }

    private static Stream<StatisticsFile> getStatisticsFiles(Table icebergTable)
    {
        if (icebergTable.currentSnapshot() == null || icebergTable.currentSnapshot().statisticsFiles() == null) {
            return Stream.of();
        }
        return stream(icebergTable.currentSnapshot().statisticsFiles());
    }
}
