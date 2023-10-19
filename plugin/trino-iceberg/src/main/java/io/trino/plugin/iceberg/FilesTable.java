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
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
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
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionColumnType;
import static io.trino.plugin.iceberg.IcebergUtil.partitionTypes;
import static io.trino.plugin.iceberg.PartitionTable.getAllPartitionFields;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
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
    private final Optional<IcebergPartitionColumn> partitionColumnType;
    private final List<PartitionField> partitionFields;
    private final Map<Integer, Type.PrimitiveType> idToPrimitiveTypeMapping;

    public FilesTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, Optional<Long> snapshotId)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.partitionFields = getAllPartitionFields(icebergTable);

        this.partitionColumnType = getPartitionColumnType(partitionFields, icebergTable.schema(), typeManager);

        List<ColumnMetadata> connectorMetadata = Lists.newArrayList(
                new ColumnMetadata("content", INTEGER),
                new ColumnMetadata("file_path", VARCHAR),
                new ColumnMetadata("file_format", VARCHAR),
                new ColumnMetadata("record_count", BIGINT),
                new ColumnMetadata("file_size_in_bytes", BIGINT),
                new ColumnMetadata("column_sizes", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))),
                new ColumnMetadata("value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))),
                new ColumnMetadata("null_value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))),
                new ColumnMetadata("nan_value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))),
                new ColumnMetadata("lower_bounds", typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))),
                new ColumnMetadata("upper_bounds", typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))),
                new ColumnMetadata("key_metadata", VARBINARY),
                new ColumnMetadata("split_offsets", new ArrayType(BIGINT)),
                new ColumnMetadata("equality_ids", new ArrayType(INTEGER)));
        partitionColumnType.ifPresent(icebergPartitionColumn ->
                connectorMetadata.add(new ColumnMetadata("partition", icebergPartitionColumn.rowType())));

        idToPrimitiveTypeMapping = IcebergUtil.primitiveFieldTypes(icebergTable.schema());
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), connectorMetadata);
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

        PlanFilesIterable planFilesIterable = new
                PlanFilesIterable(tableScan.planFiles(), idToTypeMapping, types, typeManager, partitionColumnType, partitionFields, idToPrimitiveTypeMapping);
        return planFilesIterable.cursor();
    }

    private static class PlanFilesIterable
            extends CloseableGroup
            implements Iterable<List<Object>>
    {
        private final CloseableIterable<FileScanTask> planFiles;
        private final Map<Integer, Type> idToTypeMapping;
        private final Map<Integer, Type.PrimitiveType> idToPrimitiveTypeMapping;
        private final List<io.trino.spi.type.Type> types;
        private boolean closed;
        private final MapType integerToBigintMapType;
        private final MapType integerToVarcharMapType;
        Optional<IcebergPartitionColumn> partitionColumnType;
        private final List<PartitionField> partitionFields;

        public PlanFilesIterable(CloseableIterable<FileScanTask> planFiles,
                                 Map<Integer, Type> idToTypeMapping,
                                 List<io.trino.spi.type.Type> types,
                                 TypeManager typeManager,
                                 Optional<IcebergPartitionColumn> partitionColumnType,
                                 List<PartitionField> partitionFields,
                                 Map<Integer, Type.PrimitiveType> idToPrimitiveTypeMapping)
        {
            this.planFiles = requireNonNull(planFiles, "planFiles is null");
            this.idToTypeMapping = ImmutableMap.copyOf(requireNonNull(idToTypeMapping, "idToTypeMapping is null"));
            this.idToPrimitiveTypeMapping = idToPrimitiveTypeMapping;
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.integerToBigintMapType = new MapType(INTEGER, BIGINT, typeManager.getTypeOperators());
            this.integerToVarcharMapType = new MapType(INTEGER, VARCHAR, typeManager.getTypeOperators());
            this.partitionColumnType = partitionColumnType;
            this.partitionFields = partitionFields;
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
                    FileScanTask fileScanTask = planFilesIterator.next();
                    return getRecord(fileScanTask.file(), fileScanTask.spec());
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

        private List<Object> getRecord(DataFile dataFile,
                                       PartitionSpec spec)
        {
            List<Object> columns = new ArrayList<>();
            columns.add(dataFile.content().id());
            columns.add(dataFile.path().toString());
            columns.add(dataFile.format().name());
            columns.add(dataFile.recordCount());
            columns.add(dataFile.fileSizeInBytes());
            columns.add(getIntegerBigintSqlMap(dataFile.columnSizes()));
            columns.add(getIntegerBigintSqlMap(dataFile.valueCounts()));
            columns.add(getIntegerBigintSqlMap(dataFile.nullValueCounts()));
            columns.add(getIntegerBigintSqlMap(dataFile.nanValueCounts()));
            columns.add(getIntegerVarcharSqlMap(dataFile.lowerBounds()));
            columns.add(getIntegerVarcharSqlMap(dataFile.upperBounds()));
            columns.add(toVarbinarySlice(dataFile.keyMetadata()));
            columns.add(toBigintArrayBlock(dataFile.splitOffsets()));
            columns.add(toIntegerArrayBlock(dataFile.equalityFieldIds()));

            Types.StructType structType = spec.partitionType();
            StructLike partitionStruct = dataFile.partition();
            StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(structType).set(partitionStruct);
            StructLikeWrapperWithFieldIdToIndex structLikeWrapperWithFieldIdToIndex = new StructLikeWrapperWithFieldIdToIndex(partitionWrapper, structType);
            List<Type> partitionTypes = partitionTypes(partitionFields, idToPrimitiveTypeMapping);
            List<? extends Class<?>> partitionColumnClass = partitionTypes.stream()
                    .map(type -> type.typeId().javaClass())
                    .collect(toImmutableList());

            // add data for partition columns
            IcebergUtil.addPartitionColumn(partitionColumnType, partitionTypes, partitionColumnClass, structLikeWrapperWithFieldIdToIndex, columns);
            return columns;
        }

        private SqlMap getIntegerBigintSqlMap(Map<Integer, Long> value)
        {
            if (value == null) {
                return null;
            }
            return toIntegerBigintSqlMap(value);
        }

        private SqlMap getIntegerVarcharSqlMap(Map<Integer, ByteBuffer> value)
        {
            if (value == null) {
                return null;
            }
            return toIntegerVarcharSqlMap(
                    value.entrySet().stream()
                            .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                            .collect(toImmutableMap(
                                    Map.Entry<Integer, ByteBuffer>::getKey,
                                    entry -> Transforms.identity().toHumanString(
                                            idToTypeMapping.get(entry.getKey()), Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue())))));
        }

        private SqlMap toIntegerBigintSqlMap(Map<Integer, Long> values)
        {
            return buildMapValue(
                    integerToBigintMapType,
                    values.size(),
                    (keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
                        INTEGER.writeLong(keyBuilder, key);
                        BIGINT.writeLong(valueBuilder, value);
                    }));
        }

        private SqlMap toIntegerVarcharSqlMap(Map<Integer, String> values)
        {
            return buildMapValue(
                    integerToVarcharMapType,
                    values.size(),
                    (keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
                        INTEGER.writeLong(keyBuilder, key);
                        VARCHAR.writeString(valueBuilder, value);
                    }));
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
            return Slices.wrappedHeapBuffer(value);
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
