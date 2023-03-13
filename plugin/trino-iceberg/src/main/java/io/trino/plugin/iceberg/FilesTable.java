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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
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
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
    private final ConnectorTableMetadata tableMetadata;
    private final TypeManager typeManager;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;

    public FilesTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, Optional<Long> snapshotId)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

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
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        List<io.trino.spi.type.Type> types = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        if (snapshotId.isEmpty()) {
            return InMemoryRecordSet.builder(types).build().cursor();
        }

        Map<Integer, Type> idToTypeMapping = getIcebergIdToTypeMapping(icebergTable.schema());
        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(snapshotId.get())
                .includeColumnStats();

        PlanFilesIterable planFilesIterable = new PlanFilesIterable(tableScan.planFiles(), idToTypeMapping, types, typeManager);
        return planFilesIterable.cursor();
    }

    private static class PlanFilesIterable
            extends CloseableGroup
            implements Iterable<List<Object>>
    {
        private final CloseableIterable<FileScanTask> planFiles;
        private final Map<Integer, Type> idToTypeMapping;
        private final List<io.trino.spi.type.Type> types;
        private boolean closed;
        private final io.trino.spi.type.Type integerToBigintMapType;
        private final io.trino.spi.type.Type integerToVarcharMapType;

        public PlanFilesIterable(CloseableIterable<FileScanTask> planFiles, Map<Integer, Type> idToTypeMapping, List<io.trino.spi.type.Type> types, TypeManager typeManager)
        {
            this.planFiles = requireNonNull(planFiles, "planFiles is null");
            this.idToTypeMapping = ImmutableMap.copyOf(requireNonNull(idToTypeMapping, "idToTypeMapping is null"));
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.integerToBigintMapType = typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()));
            this.integerToVarcharMapType = typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()));
            addCloseable(planFiles);
        }

        public RecordCursor cursor()
        {
            CloseableIterator<List<Object>> iterator = this.iterator();
            return new InMemoryRecordSet.InMemoryRecordCursor(types, iterator) {
                @Override
                public void close()
                {
                    try (iterator) {
                        super.close();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("Failed to close cursor", e);
                    }
                }
            };
        }

        @Override
        public CloseableIterator<List<Object>> iterator()
        {
            final CloseableIterator<FileScanTask> planFilesIterator = planFiles.iterator();
            addCloseable(planFilesIterator);

            return new CloseableIterator<>() {
                @Override
                public boolean hasNext()
                {
                    return !closed && planFilesIterator.hasNext();
                }

                @Override
                public List<Object> next()
                {
                    return getRecord(planFilesIterator.next().file());
                }

                @Override
                public void close()
                        throws IOException
                {
                    PlanFilesIterable.super.close();
                    closed = true;
                }
            };
        }

        private List<Object> getRecord(DataFile dataFile)
        {
            List<Object> columns = new ArrayList<>();
            columns.add(dataFile.content().id());
            columns.add(dataFile.path().toString());
            columns.add(dataFile.format().name());
            columns.add(dataFile.recordCount());
            columns.add(dataFile.fileSizeInBytes());
            columns.add(getIntegerBigintMapBlock(dataFile.columnSizes()));
            columns.add(getIntegerBigintMapBlock(dataFile.valueCounts()));
            columns.add(getIntegerBigintMapBlock(dataFile.nullValueCounts()));
            columns.add(getIntegerBigintMapBlock(dataFile.nanValueCounts()));
            columns.add(getIntegerVarcharMapBlock(dataFile.lowerBounds()));
            columns.add(getIntegerVarcharMapBlock(dataFile.upperBounds()));
            columns.add(toVarbinarySlice(dataFile.keyMetadata()));
            columns.add(toBigintArrayBlock(dataFile.splitOffsets()));
            columns.add(toIntegerArrayBlock(dataFile.equalityFieldIds()));
            checkArgument(columns.size() == types.size(), "Expected %s types in row, but got %s values", types.size(), columns.size());
            return columns;
        }

        private Object getIntegerBigintMapBlock(Map<Integer, Long> value)
        {
            if (value == null) {
                return null;
            }
            return toIntegerBigintMapBlock(value);
        }

        private Object getIntegerVarcharMapBlock(Map<Integer, ByteBuffer> value)
        {
            if (value == null) {
                return null;
            }
            return toIntegerVarcharMapBlock(
                    value.entrySet().stream()
                            .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                            .collect(toImmutableMap(
                                    Map.Entry<Integer, ByteBuffer>::getKey,
                                    entry -> Transforms.identity().toHumanString(
                                            idToTypeMapping.get(entry.getKey()), Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue())))));
        }

        private Object toIntegerBigintMapBlock(Map<Integer, Long> values)
        {
            BlockBuilder blockBuilder = integerToBigintMapType.createBlockBuilder(null, 1);
            BlockBuilder singleMapBlockBuilder = blockBuilder.beginBlockEntry();
            values.forEach((key, value) -> {
                INTEGER.writeLong(singleMapBlockBuilder, key);
                BIGINT.writeLong(singleMapBlockBuilder, value);
            });
            blockBuilder.closeEntry();
            return integerToBigintMapType.getObject(blockBuilder, 0);
        }

        private Object toIntegerVarcharMapBlock(Map<Integer, String> values)
        {
            BlockBuilder blockBuilder = integerToVarcharMapType.createBlockBuilder(null, 1);
            BlockBuilder singleMapBlockBuilder = blockBuilder.beginBlockEntry();
            values.forEach((key, value) -> {
                INTEGER.writeLong(singleMapBlockBuilder, key);
                VARCHAR.writeString(singleMapBlockBuilder, value);
            });
            blockBuilder.closeEntry();
            return integerToVarcharMapType.getObject(blockBuilder, 0);
        }

        @Nullable
        private static Block toIntegerArrayBlock(List<Integer> values)
        {
            if (values == null) {
                return null;
            }
            BlockBuilder builder = INTEGER.createBlockBuilder(null, values.size());
            values.forEach(value -> INTEGER.writeLong(builder, value));
            return builder.build();
        }

        @Nullable
        private static Block toBigintArrayBlock(List<Long> values)
        {
            if (values == null) {
                return null;
            }
            BlockBuilder builder = BIGINT.createBlockBuilder(null, values.size());
            values.forEach(value -> BIGINT.writeLong(builder, value));
            return builder.build();
        }

        @Nullable
        private static Slice toVarbinarySlice(ByteBuffer value)
        {
            if (value == null) {
                return null;
            }
            return Slices.wrappedBuffer(value);
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
}
