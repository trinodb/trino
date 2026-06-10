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
package io.trino.plugin.iceberg.system.positiondeletes;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex;
import io.trino.plugin.iceberg.delete.PositionDeleteReader;
import io.trino.plugin.iceberg.system.IcebergPartitionColumn;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Type.PrimitiveType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex.createStructLikeWrapper;
import static io.trino.plugin.iceberg.system.PositionDeletesTable.DELETE_FILE_PATH;
import static io.trino.plugin.iceberg.system.PositionDeletesTable.FILE_PATH;
import static io.trino.plugin.iceberg.system.PositionDeletesTable.PARTITION;
import static io.trino.plugin.iceberg.system.PositionDeletesTable.POS;
import static io.trino.plugin.iceberg.system.PositionDeletesTable.SPEC_ID;
import static io.trino.plugin.iceberg.util.SystemTableUtil.partitionTypes;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class PositionDeletesTablePageSource
        implements ConnectorPageSource
{
    private final PositionDeletesTableSplit split;
    private final PageBuilder pageBuilder;
    private final List<String> columns;
    private final List<DeleteRow> deleteRows = new ArrayList<>();
    private final Map<Integer, PartitionSpec> idToPartitionSpecMapping;
    private final Map<Integer, PrimitiveType> idToTypeMapping;
    private final Optional<IcebergPartitionColumn> partitionColumnType;
    private int currentPosition;
    private long completedPositions;
    private long readTimeNanos;
    private boolean closed;

    private record DeleteRow(String filePath, long pos) {}

    public PositionDeletesTablePageSource(
            ConnectorPageSource pageSource,
            PositionDeletesTableSplit split,
            List<String> columns)
    {
        requireNonNull(pageSource, "pageSource is null");
        this.split = requireNonNull(split, "split is null");
        this.columns = ImmutableList.copyOf(columns);

        Schema schema = SchemaParser.fromJson(split.schemaJson());
        idToPartitionSpecMapping = split.partitionSpecsByIdJson().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> PartitionSpecParser.fromJson(schema, entry.getValue())));
        idToTypeMapping = primitiveFieldTypes(schema);
        partitionColumnType = split.partitionColumn();

        List<Type> types = columns.stream().map(this::columnType).collect(toImmutableList());
        pageBuilder = new PageBuilder(types);

        PositionDeleteReader.readMultiFilePositionDeletes(
                pageSource,
                (filePath, pos) -> deleteRows.add(new DeleteRow(filePath, pos)));
    }

    private Type columnType(String columnName)
    {
        return switch (columnName) {
            case FILE_PATH, DELETE_FILE_PATH -> VARCHAR;
            case POS -> BIGINT;
            case PARTITION -> (RowType) split.partitionColumnType().orElseThrow();
            case SPEC_ID -> INTEGER;
            default -> throw new IllegalArgumentException("Unknown column: " + columnName);
        };
    }

    @Override
    public long getCompletedBytes()
    {
        return split.deleteFileLength();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return closed || currentPosition >= deleteRows.size();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (isFinished()) {
            return null;
        }

        long start = System.nanoTime();
        while (currentPosition < deleteRows.size() && !pageBuilder.isFull()) {
            pageBuilder.declarePosition();
            DeleteRow deleteRow = deleteRows.get(currentPosition++);

            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);

                switch (columnName) {
                    case FILE_PATH -> VARCHAR.writeString(blockBuilder, deleteRow.filePath());
                    case POS -> BIGINT.writeLong(blockBuilder, deleteRow.pos());
                    case PARTITION -> writePartitionColumn(blockBuilder);
                    case SPEC_ID -> INTEGER.writeInt(blockBuilder, split.specId());
                    case DELETE_FILE_PATH -> VARCHAR.writeString(blockBuilder, split.deleteFilePath());
                    default -> throw new IllegalArgumentException("Unknown column: " + columnName);
                }
            }
        }
        readTimeNanos += System.nanoTime() - start;

        if (!pageBuilder.isEmpty()) {
            Page page = pageBuilder.build();
            completedPositions += page.getPositionCount();
            pageBuilder.reset();
            return SourcePage.create(page);
        }

        close();
        return null;
    }

    private void writePartitionColumn(BlockBuilder blockBuilder)
    {
        if (partitionColumnType.isEmpty()) {
            return;
        }

        PartitionSpec partitionSpec = idToPartitionSpecMapping.get(split.specId());
        PartitionData partitionData = PartitionData.fromJson(split.partitionDataJson(), partitionSpec);
        StructLikeWrapperWithFieldIdToIndex partitionStruct = createStructLikeWrapper(partitionSpec, partitionData);
        List<org.apache.iceberg.types.Type> partitionTypes = partitionTypes(partitionSpec.fields(), idToTypeMapping);
        List<? extends Class<?>> partitionColumnClass = partitionTypes.stream()
                .map(type -> type.typeId().javaClass())
                .collect(toImmutableList());
        List<Type> partitionColumnTypes = partitionColumnType.orElseThrow().rowType().getFields().stream()
                .map(RowType.Field::getType)
                .collect(toImmutableList());

        if (blockBuilder instanceof RowBlockBuilder rowBlockBuilder) {
            rowBlockBuilder.buildEntry(fields -> {
                for (int i = 0; i < partitionColumnTypes.size(); i++) {
                    Type trinoType = partitionColumnTypes.get(i);
                    Object value = null;
                    Integer fieldId = partitionColumnType.get().fieldIds().get(i);
                    if (partitionStruct.getFieldIdToIndex().containsKey(fieldId)) {
                        value = convertIcebergValueToTrino(
                                partitionTypes.get(i),
                                partitionStruct.getStructLikeWrapper().get().get(partitionStruct.getFieldIdToIndex().get(fieldId), partitionColumnClass.get(i)));
                    }
                    writeNativeValue(trinoType, fields.get(i), value);
                }
            });
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
