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
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class FilesTable
        implements SystemTable
{
    private static final int SCAN_FILE_BATCH_SIZE = 10000;

    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;

    public FilesTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, Optional<Long> snapshotId)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");

        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("content", INTEGER))
                        .add(new ColumnMetadata("file_path", VARCHAR))
                        .add(new ColumnMetadata("file_format", VARCHAR))
                        .add(new ColumnMetadata("record_count", BIGINT))
                        .add(new ColumnMetadata("file_size_in_bytes", BIGINT))
                        .add(new ColumnMetadata("column_sizes", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("null_value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("nan_value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("lower_bounds", typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .add(new ColumnMetadata("upper_bounds", typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .add(new ColumnMetadata("key_metadata", VARBINARY))
                        .add(new ColumnMetadata("split_offsets", new ArrayType(BIGINT)))
                        .add(new ColumnMetadata("equality_ids", new ArrayType(INTEGER)))
                        .build());
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        if (snapshotId.isEmpty()) {
            return new FixedPageSource(ImmutableList.of());
        }
        return buildIncrementalPageSource(tableMetadata, icebergTable, snapshotId.get());
    }

    private static ConnectorPageSource buildIncrementalPageSource(ConnectorTableMetadata tableMetadata, Table icebergTable, long snapshotId)
    {
        Map<Integer, Type> idToTypeMapping = getIcebergIdToTypeMapping(icebergTable.schema());

        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(snapshotId)
                .includeColumnStats();

        PlanFilesIterable planFilesIterable = new PlanFilesIterable(tableScan.planFiles(), idToTypeMapping, getType(tableMetadata));
        try {
            return planFilesIterable.initializePageSource();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize page source", e);
        }
    }

    private static class PlanFilesIterable
            implements Iterable<Page>
    {
        private CloseableIterator<FileScanTask> planFilesIterator;
        private final Map<Integer, Type> idToTypeMapping;
        private PageBuilder pageBuilder;
        private final int columnSize;
        private Page page;
        private boolean closed;
        private boolean firstBatchProcessed;

        public PlanFilesIterable(CloseableIterable<FileScanTask> planFiles, Map<Integer, Type> idToTypeMapping, List<io.trino.spi.type.Type> types)
        {
            requireNonNull(planFiles, "planFiles is null");
            requireNonNull(types, "types is null");
            this.planFilesIterator = planFiles.iterator();
            this.idToTypeMapping = requireNonNull(idToTypeMapping, "idToTypeMapping is null");
            this.pageBuilder = new PageBuilder(types);
            this.columnSize = types.size();
        }

        public ConnectorPageSource initializePageSource()
                throws IOException
        {
            page = buildPage();
            return new IncrementalPopulatingPageSource(CloseableIterable.withNoopClose(this), page.getRetainedSizeInBytes());
        }

        @Override
        public CloseableIterator<Page> iterator()
        {
            return new CloseableIterator<>()
            {
                @Override
                public boolean hasNext()
                {
                    return !closed && (planFilesIterator.hasNext() || !firstBatchProcessed);
                }

                @Override
                public Page next()
                {
                    if (firstBatchProcessed) {
                        page = buildPage();
                    }
                    else {
                        firstBatchProcessed = true;
                    }
                    return page;
                }

                @Override
                public void close()
                        throws IOException
                {
                    if (!closed) {
                        planFilesIterator.close();
                        planFilesIterator = null;
                        pageBuilder = null;
                        page = null;
                        closed = true;
                    }
                }
            };
        }

        private Page buildPage()
        {
            pageBuilder.reset();
            int i = 0;
            while (!pageBuilder.isFull() && i < SCAN_FILE_BATCH_SIZE && planFilesIterator.hasNext()) {
                addFileRowInPage(pageBuilder, planFilesIterator.next(), idToTypeMapping, columnSize);
                i++;
            }
            return pageBuilder.build();
        }
    }

    private static Map<Integer, Type> getIcebergIdToTypeMapping(Schema schema)
    {
        ImmutableMap.Builder<Integer, Type> icebergIdToTypeMapping = ImmutableMap.builder();
        for (Types.NestedField field : schema.columns()) {
            populateIcebergIdToTypeMapping(field, icebergIdToTypeMapping);
        }
        return icebergIdToTypeMapping.buildOrThrow();
    }

    private static void populateIcebergIdToTypeMapping(Types.NestedField field, ImmutableMap.Builder<Integer, Type> icebergIdToTypeMapping)
    {
        Type type = field.type();
        icebergIdToTypeMapping.put(field.fieldId(), type);
        if (type instanceof Type.NestedType) {
            type.asNestedType().fields().forEach(child -> populateIcebergIdToTypeMapping(child, icebergIdToTypeMapping));
        }
    }

    private static void addFileRowInPage(PageBuilder pageBuilder, FileScanTask fileScanTask, Map<Integer, Type> idToTypeMapping, int columnSize)
    {
        DataFile dataFile = fileScanTask.file();
        int columnIndex = -1;

        INTEGER.writeLong(pageBuilder.getBlockBuilder(++columnIndex), dataFile.content().id());
        VARCHAR.writeString(pageBuilder.getBlockBuilder(++columnIndex), dataFile.path().toString());
        VARCHAR.writeString(pageBuilder.getBlockBuilder(++columnIndex), dataFile.format().name());
        BIGINT.writeLong(pageBuilder.getBlockBuilder(++columnIndex), dataFile.recordCount());
        BIGINT.writeLong(pageBuilder.getBlockBuilder(++columnIndex), dataFile.fileSizeInBytes());

        BlockBuilder column = pageBuilder.getBlockBuilder(++columnIndex);
        if (checkNonNull(dataFile.columnSizes(), column)) {
            appendIntegerBigintMap(dataFile.columnSizes(), column);
        }

        column = pageBuilder.getBlockBuilder(++columnIndex);
        if (checkNonNull(dataFile.valueCounts(), column)) {
            appendIntegerBigintMap(dataFile.valueCounts(), column);
        }

        column = pageBuilder.getBlockBuilder(++columnIndex);
        if (checkNonNull(dataFile.nullValueCounts(), column)) {
            appendIntegerBigintMap(dataFile.nullValueCounts(), column);
        }

        column = pageBuilder.getBlockBuilder(++columnIndex);
        if (checkNonNull(dataFile.nanValueCounts(), column)) {
            appendIntegerBigintMap(dataFile.nanValueCounts(), column);
        }

        column = pageBuilder.getBlockBuilder(++columnIndex);
        if (checkNonNull(dataFile.lowerBounds(), column)) {
            Map<Integer, String> values = dataFile.lowerBounds().entrySet().stream()
                    .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                    .collect(toImmutableMap(
                            Map.Entry<Integer, ByteBuffer>::getKey,
                            entry -> Transforms.identity().toHumanString(
                                    idToTypeMapping.get(entry.getKey()), Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue()))));
            appendIntegerVarcharMap(values, column);
        }

        column = pageBuilder.getBlockBuilder(++columnIndex);
        if (checkNonNull(dataFile.upperBounds(), column)) {
            Map<Integer, String> values = dataFile.upperBounds().entrySet().stream()
                    .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                    .collect(toImmutableMap(
                            Map.Entry<Integer, ByteBuffer>::getKey,
                            entry -> Transforms.identity().toHumanString(
                                    idToTypeMapping.get(entry.getKey()), Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue()))));
            appendIntegerVarcharMap(values, column);
        }

        column = pageBuilder.getBlockBuilder(++columnIndex);
        if (checkNonNull(dataFile.keyMetadata(), column)) {
            VARBINARY.writeSlice(column, Slices.wrappedBuffer(dataFile.keyMetadata()));
        }

        column = pageBuilder.getBlockBuilder(++columnIndex);
        if (checkNonNull(dataFile.splitOffsets(), column)) {
            appendBigintArray(dataFile.splitOffsets(), column);
        }

        column = pageBuilder.getBlockBuilder(++columnIndex);
        if (checkNonNull(dataFile.equalityFieldIds(), column)) {
            appendIntegerArray(dataFile.equalityFieldIds(), column);
        }
        checkArgument(columnIndex + 1 == columnSize, "columnIndex + 1 should equal to columnSize");
        pageBuilder.declarePosition();
    }

    private static boolean checkNonNull(Object object, BlockBuilder blockBuilder)
    {
        if (object == null) {
            blockBuilder.appendNull();
            return false;
        }
        return true;
    }

    private static void appendIntegerBigintMap(Map<Integer, Long> values, BlockBuilder column)
    {
        BlockBuilder map = column.beginBlockEntry();
        values.forEach((key, value) -> {
            INTEGER.writeLong(map, key);
            BIGINT.writeLong(map, value);
        });
        column.closeEntry();
    }

    private static void appendBigintArray(Iterable<Long> values, BlockBuilder column)
    {
        BlockBuilder array = column.beginBlockEntry();
        for (Long value : values) {
            BIGINT.writeLong(array, value);
        }
        column.closeEntry();
    }

    private static void appendIntegerArray(Iterable<Integer> values, BlockBuilder column)
    {
        BlockBuilder array = column.beginBlockEntry();
        for (Integer value : values) {
            INTEGER.writeLong(array, value);
        }
        column.closeEntry();
    }

    public static void appendIntegerVarcharMap(Map<Integer, String> values, BlockBuilder column)
    {
        BlockBuilder map = column.beginBlockEntry();
        values.forEach((key, value) -> {
            INTEGER.writeLong(map, key);
            VARCHAR.writeString(map, value);
        });
        column.closeEntry();
    }

    private static List<io.trino.spi.type.Type> getType(ConnectorTableMetadata table)
    {
        return table.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
    }
}
