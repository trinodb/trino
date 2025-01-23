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
import io.trino.plugin.iceberg.util.PageListBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import jakarta.annotation.Nullable;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetricsUtil.ReadableMetricsStruct;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedHeapBuffer;
import static io.trino.plugin.iceberg.FilesTable.getIcebergIdToTypeMapping;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionColumnType;
import static io.trino.plugin.iceberg.IcebergUtil.partitionTypes;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.PartitionsTable.getAllPartitionFields;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.apache.iceberg.MetadataTableType.ALL_ENTRIES;
import static org.apache.iceberg.MetadataTableType.ENTRIES;

// https://iceberg.apache.org/docs/latest/spark-queries/#all-entries
// https://iceberg.apache.org/docs/latest/spark-queries/#entries
public class EntriesTable
        extends BaseSystemTable
{
    private final Map<Integer, Type> idToTypeMapping;
    private final List<Types.NestedField> primitiveFields;
    private final Optional<IcebergPartitionColumn> partitionColumn;
    private final List<Type> partitionTypes;

    public EntriesTable(TypeManager typeManager, SchemaTableName tableName, Table icebergTable, MetadataTableType metadataTableType, ExecutorService executor)
    {
        super(
                requireNonNull(icebergTable, "icebergTable is null"),
                new ConnectorTableMetadata(
                        requireNonNull(tableName, "tableName is null"),
                        columns(requireNonNull(typeManager, "typeManager is null"), icebergTable)),
                metadataTableType,
                executor);
        checkArgument(metadataTableType == ALL_ENTRIES || metadataTableType == ENTRIES, "Unexpected metadata table type: %s", metadataTableType);
        idToTypeMapping = getIcebergIdToTypeMapping(icebergTable.schema());
        primitiveFields = IcebergUtil.primitiveFields(icebergTable.schema()).stream()
                .sorted(Comparator.comparing(Types.NestedField::name))
                .collect(toImmutableList());
        List<PartitionField> partitionFields = getAllPartitionFields(icebergTable);
        partitionColumn = getPartitionColumnType(partitionFields, icebergTable.schema(), typeManager);
        partitionTypes = partitionTypes(partitionFields, primitiveFieldTypes(icebergTable.schema()));
    }

    private static List<ColumnMetadata> columns(TypeManager typeManager, Table icebergTable)
    {
        return ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("status", INTEGER))
                .add(new ColumnMetadata("snapshot_id", BIGINT))
                .add(new ColumnMetadata("sequence_number", BIGINT))
                .add(new ColumnMetadata("file_sequence_number", BIGINT))
                .add(new ColumnMetadata("data_file", RowType.from(dataFileFieldMetadata(typeManager, icebergTable))))
                .add(new ColumnMetadata("readable_metrics", typeManager.getType(new TypeSignature(JSON))))
                .build();
    }

    private static List<RowType.Field> dataFileFieldMetadata(TypeManager typeManager, Table icebergTable)
    {
        List<PartitionField> partitionFields = getAllPartitionFields(icebergTable);
        Optional<IcebergPartitionColumn> partitionColumnType = getPartitionColumnType(partitionFields, icebergTable.schema(), typeManager);

        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        fields.add(new RowType.Field(Optional.of("content"), INTEGER));
        fields.add(new RowType.Field(Optional.of("file_path"), VARCHAR));
        fields.add(new RowType.Field(Optional.of("file_format"), VARCHAR));
        fields.add(new RowType.Field(Optional.of("spec_id"), INTEGER));
        partitionColumnType.ifPresent(type -> fields.add(new RowType.Field(Optional.of("partition"), type.rowType())));
        fields.add(new RowType.Field(Optional.of("record_count"), BIGINT));
        fields.add(new RowType.Field(Optional.of("file_size_in_bytes"), BIGINT));
        fields.add(new RowType.Field(Optional.of("column_sizes"), typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        fields.add(new RowType.Field(Optional.of("value_counts"), typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        fields.add(new RowType.Field(Optional.of("null_value_counts"), typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        fields.add(new RowType.Field(Optional.of("nan_value_counts"), typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        fields.add(new RowType.Field(Optional.of("lower_bounds"), typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))));
        fields.add(new RowType.Field(Optional.of("upper_bounds"), typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))));
        fields.add(new RowType.Field(Optional.of("key_metadata"), VARBINARY));
        fields.add(new RowType.Field(Optional.of("split_offsets"), new ArrayType(BIGINT)));
        fields.add(new RowType.Field(Optional.of("equality_ids"), new ArrayType(INTEGER)));
        fields.add(new RowType.Field(Optional.of("sort_order_id"), INTEGER));
        return fields.build();
    }

    @Override
    protected void addRow(PageListBuilder pagesBuilder, Row row, TimeZoneKey timeZoneKey)
    {
        pagesBuilder.beginRow();
        pagesBuilder.appendInteger(row.get("status", Integer.class));
        pagesBuilder.appendBigint(row.get("snapshot_id", Long.class));
        pagesBuilder.appendBigint(row.get("sequence_number", Long.class));
        pagesBuilder.appendBigint(row.get("file_sequence_number", Long.class));
        StructProjection dataFile = row.get("data_file", StructProjection.class);
        appendDataFile((RowBlockBuilder) pagesBuilder.nextColumn(), dataFile);
        ReadableMetricsStruct readableMetrics = row.get("readable_metrics", ReadableMetricsStruct.class);
        String readableMetricsJson = FilesTable.toJson(readableMetrics, primitiveFields);
        pagesBuilder.appendVarchar(readableMetricsJson);
        pagesBuilder.endRow();
    }

    private void appendDataFile(RowBlockBuilder blockBuilder, StructProjection dataFile)
    {
        blockBuilder.buildEntry(fieldBuilders -> {
            Integer content = dataFile.get(0, Integer.class);
            INTEGER.writeLong(fieldBuilders.get(0), content);

            String filePath = dataFile.get(1, String.class);
            VARCHAR.writeString(fieldBuilders.get(1), filePath);

            String fileFormat = dataFile.get(2, String.class);
            VARCHAR.writeString(fieldBuilders.get(2), fileFormat);

            Integer specId = dataFile.get(3, Integer.class);
            INTEGER.writeLong(fieldBuilders.get(3), Long.valueOf(specId));

            partitionColumn.ifPresent(type -> {
                StructProjection partition = dataFile.get(4, StructProjection.class);
                RowBlockBuilder partitionBlockBuilder = (RowBlockBuilder) fieldBuilders.get(4);
                partitionBlockBuilder.buildEntry(partitionBuilder -> {
                    for (int i = 0; i < type.rowType().getFields().size(); i++) {
                        Type icebergType = partitionTypes.get(i);
                        io.trino.spi.type.Type trinoType = type.rowType().getFields().get(i).getType();
                        Object value = null;
                        Integer fieldId = type.fieldIds().get(i);
                        if (fieldId != null) {
                            value = convertIcebergValueToTrino(icebergType, partition.get(i, icebergType.typeId().javaClass()));
                        }
                        writeNativeValue(trinoType, partitionBuilder.get(i), value);
                    }
                });
            });

            int position = partitionColumn.isEmpty() ? 4 : 5;
            Long recordCount = dataFile.get(position, Long.class);
            BIGINT.writeLong(fieldBuilders.get(position), recordCount);

            Long fileSizeInBytes = dataFile.get(++position, Long.class);
            BIGINT.writeLong(fieldBuilders.get(position), fileSizeInBytes);

            //noinspection unchecked
            Map<Integer, Long> columnSizes = dataFile.get(++position, Map.class);
            appendIntegerBigintMap((MapBlockBuilder) fieldBuilders.get(position), columnSizes);

            //noinspection unchecked
            Map<Integer, Long> valueCounts = dataFile.get(++position, Map.class);
            appendIntegerBigintMap((MapBlockBuilder) fieldBuilders.get(position), valueCounts);

            //noinspection unchecked
            Map<Integer, Long> nullValueCounts = dataFile.get(++position, Map.class);
            appendIntegerBigintMap((MapBlockBuilder) fieldBuilders.get(position), nullValueCounts);

            //noinspection unchecked
            Map<Integer, Long> nanValueCounts = dataFile.get(++position, Map.class);
            appendIntegerBigintMap((MapBlockBuilder) fieldBuilders.get(position), nanValueCounts);

            switch (ContentType.of(content)) {
                case DATA, EQUALITY_DELETE -> {
                    //noinspection unchecked
                    Map<Integer, ByteBuffer> lowerBounds = dataFile.get(++position, Map.class);
                    appendIntegerVarcharMap((MapBlockBuilder) fieldBuilders.get(position), lowerBounds);

                    //noinspection unchecked
                    Map<Integer, ByteBuffer> upperBounds = dataFile.get(++position, Map.class);
                    appendIntegerVarcharMap((MapBlockBuilder) fieldBuilders.get(position), upperBounds);
                }
                case POSITION_DELETE -> {
                    //noinspection unchecked
                    Map<Integer, ByteBuffer> lowerBounds = dataFile.get(++position, Map.class);
                    appendBoundsForPositionDelete((MapBlockBuilder) fieldBuilders.get(position), lowerBounds);

                    //noinspection unchecked
                    Map<Integer, ByteBuffer> upperBounds = dataFile.get(++position, Map.class);
                    appendBoundsForPositionDelete((MapBlockBuilder) fieldBuilders.get(position), upperBounds);
                }
            }

            ByteBuffer keyMetadata = dataFile.get(++position, ByteBuffer.class);
            if (keyMetadata == null) {
                fieldBuilders.get(position).appendNull();
            }
            else {
                VARBINARY.writeSlice(fieldBuilders.get(position), wrappedHeapBuffer(keyMetadata));
            }

            //noinspection unchecked
            List<Long> splitOffsets = dataFile.get(++position, List.class);
            appendBigintArray((ArrayBlockBuilder) fieldBuilders.get(position), splitOffsets);

            switch (ContentType.of(content)) {
                case DATA -> {
                    // data files don't have equality ids
                    fieldBuilders.get(++position).appendNull();

                    Integer sortOrderId = dataFile.get(++position, Integer.class);
                    INTEGER.writeLong(fieldBuilders.get(position), Long.valueOf(sortOrderId));
                }
                case POSITION_DELETE -> {
                    // position delete files don't have equality ids
                    fieldBuilders.get(++position).appendNull();

                    // position delete files don't have sort order id
                    fieldBuilders.get(++position).appendNull();
                }
                case EQUALITY_DELETE -> {
                    //noinspection unchecked
                    List<Integer> equalityIds = dataFile.get(++position, List.class);
                    appendIntegerArray((ArrayBlockBuilder) fieldBuilders.get(position), equalityIds);

                    Integer sortOrderId = dataFile.get(++position, Integer.class);
                    INTEGER.writeLong(fieldBuilders.get(position), Long.valueOf(sortOrderId));
                }
            }
        });
    }

    public static void appendBigintArray(ArrayBlockBuilder blockBuilder, @Nullable List<Long> values)
    {
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }
        blockBuilder.buildEntry(elementBuilder -> {
            for (Long value : values) {
                BIGINT.writeLong(elementBuilder, value);
            }
        });
    }

    public static void appendIntegerArray(ArrayBlockBuilder blockBuilder, @Nullable List<Integer> values)
    {
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }
        blockBuilder.buildEntry(elementBuilder -> {
            for (Integer value : values) {
                INTEGER.writeLong(elementBuilder, value);
            }
        });
    }

    private static void appendIntegerBigintMap(MapBlockBuilder blockBuilder, @Nullable Map<Integer, Long> values)
    {
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
            INTEGER.writeLong(keyBuilder, key);
            BIGINT.writeLong(valueBuilder, value);
        }));
    }

    private void appendIntegerVarcharMap(MapBlockBuilder blockBuilder, @Nullable Map<Integer, ByteBuffer> values)
    {
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
            Type type = idToTypeMapping.get(key);
            INTEGER.writeLong(keyBuilder, key);
            VARCHAR.writeString(valueBuilder, Transforms.identity().toHumanString(type, Conversions.fromByteBuffer(type, value)));
        }));
    }

    private static void appendBoundsForPositionDelete(MapBlockBuilder blockBuilder, @Nullable Map<Integer, ByteBuffer> values)
    {
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }

        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            INTEGER.writeLong(keyBuilder, DELETE_FILE_POS.fieldId());
            ByteBuffer pos = values.get(DELETE_FILE_POS.fieldId());
            checkArgument(pos != null, "delete file pos is null");
            VARCHAR.writeString(valueBuilder, Transforms.identity().toHumanString(Types.LongType.get(), Conversions.fromByteBuffer(Types.LongType.get(), pos)));

            INTEGER.writeLong(keyBuilder, DELETE_FILE_PATH.fieldId());
            ByteBuffer path = values.get(DELETE_FILE_PATH.fieldId());
            checkArgument(path != null, "delete file path is null");
            VARCHAR.writeString(valueBuilder, Transforms.identity().toHumanString(Types.StringType.get(), Conversions.fromByteBuffer(Types.StringType.get(), path)));
        });
    }

    private enum ContentType
    {
        DATA,
        POSITION_DELETE,
        EQUALITY_DELETE;

        static ContentType of(int content)
        {
            checkArgument(content >= 0 && content <= 2, "Unexpected content type: %s", content);
            if (content == 0) {
                return DATA;
            }
            if (content == 1) {
                return POSITION_DELETE;
            }
            return EQUALITY_DELETE;
        }
    }
}
