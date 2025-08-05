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
package io.trino.plugin.iceberg.system.files;

import com.google.common.io.Closer;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.plugin.iceberg.system.FilesTable;
import io.trino.plugin.iceberg.system.IcebergPartitionColumn;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.IcebergUtil.readerForManifest;
import static io.trino.plugin.iceberg.system.FilesTable.COLUMN_SIZES_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.CONTENT_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.EQUALITY_IDS_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.FILE_FORMAT_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.FILE_PATH_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.FILE_SIZE_IN_BYTES_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.KEY_METADATA_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.LOWER_BOUNDS_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.NAN_VALUE_COUNTS_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.NULL_VALUE_COUNTS_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.PARTITION_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.READABLE_METRICS_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.RECORD_COUNT_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.SORT_ORDER_ID_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.SPEC_ID_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.SPLIT_OFFSETS_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.UPPER_BOUNDS_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.VALUE_COUNTS_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.FilesTable.getColumnType;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getPartitionColumnType;
import static io.trino.plugin.iceberg.util.SystemTableUtil.partitionTypes;
import static io.trino.plugin.iceberg.util.SystemTableUtil.readableMetricsToJson;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetricsUtil.readableMetricsStruct;

public final class FilesTablePageSource
        implements ConnectorPageSource
{
    private final Closer closer;
    private final Schema schema;
    private final Schema metadataSchema;
    private final Map<Integer, PrimitiveType> idToTypeMapping;
    private final List<PartitionField> partitionFields;
    private final Optional<IcebergPartitionColumn> partitionColumnType;
    private final List<Types.NestedField> primitiveFields;
    private final Iterator<? extends ContentFile<?>> contentIterator;
    private final Map<String, Integer> columnNameToIndex;
    private final PageBuilder pageBuilder;
    private final long completedBytes;
    private long completedPositions;
    private long readTimeNanos;
    private boolean closed;

    public FilesTablePageSource(
            TypeManager typeManager,
            TrinoFileSystem trinoFileSystem,
            ForwardingFileIoFactory fileIoFactory,
            List<String> requiredColumns,
            FilesTableSplit split)
    {
        this.closer = Closer.create();
        this.schema = SchemaParser.fromJson(requireNonNull(split.schemaJson(), "schema is null"));
        this.metadataSchema = SchemaParser.fromJson(requireNonNull(split.metadataTableJson(), "metadataSchema is null"));
        this.idToTypeMapping = primitiveFieldTypes(schema);
        Map<Integer, PartitionSpec> specs = split.partitionSpecsByIdJson().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> PartitionSpecParser.fromJson(SchemaParser.fromJson(split.schemaJson()), entry.getValue())));
        this.partitionFields = getAllPartitionFields(schema, specs);
        this.partitionColumnType = getPartitionColumnType(typeManager, partitionFields, schema);
        this.primitiveFields = IcebergUtil.primitiveFields(schema).stream()
                .sorted(Comparator.comparing(Types.NestedField::name))
                .collect(toImmutableList());
        ManifestReader<? extends ContentFile<?>> manifestReader = closer.register(readerForManifest(split.manifestFile(), fileIoFactory.create(trinoFileSystem), specs));
        // TODO figure out why selecting the specific column causes null to be returned for offset_splits
        this.contentIterator = closer.register(requireNonNull(manifestReader, "manifestReader is null").iterator());
        this.pageBuilder = new PageBuilder(requiredColumns.stream().map(column -> {
            if (column.equals(PARTITION_COLUMN_NAME)) {
                return split.partitionColumnType().orElseThrow();
            }
            return getColumnType(column, typeManager);
        }).collect(toImmutableList()));
        this.columnNameToIndex = mapWithIndex(requiredColumns.stream(),
                (columnName, position) -> immutableEntry(columnName, Long.valueOf(position).intValue()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        this.completedBytes = split.manifestFile().length();
        this.completedPositions = 0L;
        this.readTimeNanos = 0L;
        this.closed = false;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
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
        return closed;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (closed) {
            return null;
        }

        while (contentIterator.hasNext() && !pageBuilder.isFull()) {
            pageBuilder.declarePosition();
            long start = System.nanoTime();
            ContentFile<?> contentFile = contentIterator.next();

            // content
            writeValueOrNull(pageBuilder, CONTENT_COLUMN_NAME, () -> contentFile.content().id(), INTEGER::writeInt);
            // file_path
            writeValueOrNull(pageBuilder, FILE_PATH_COLUMN_NAME, contentFile::location, VARCHAR::writeString);
            // file_format
            writeValueOrNull(pageBuilder, FILE_FORMAT_COLUMN_NAME, () -> contentFile.format().toString(), VARCHAR::writeString);
            // spec_id
            writeValueOrNull(pageBuilder, SPEC_ID_COLUMN_NAME, contentFile::specId, INTEGER::writeInt);
            // partitions
            if (partitionColumnType.isPresent() && columnNameToIndex.containsKey(FilesTable.PARTITION_COLUMN_NAME)) {
                List<Type> partitionTypes = partitionTypes(partitionFields, idToTypeMapping);
                List<io.trino.spi.type.Type> partitionColumnTypes = partitionColumnType.orElseThrow().rowType().getFields().stream()
                        .map(RowType.Field::getType)
                        .collect(toImmutableList());

                if (pageBuilder.getBlockBuilder(columnNameToIndex.get(FilesTable.PARTITION_COLUMN_NAME)) instanceof RowBlockBuilder rowBlockBuilder) {
                    rowBlockBuilder.buildEntry(fields -> {
                        for (int i = 0; i < partitionColumnTypes.size(); i++) {
                            Type type = partitionTypes.get(i);
                            io.trino.spi.type.Type trinoType = partitionColumnType.get().rowType().getFields().get(i).getType();
                            Object value = null;
                            Integer fieldId = partitionColumnType.get().fieldIds().get(i);
                            if (fieldId != null) {
                                value = convertIcebergValueToTrino(type, contentFile.partition().get(i, type.typeId().javaClass()));
                            }
                            writeNativeValue(trinoType, fields.get(i), value);
                        }
                    });
                }
            }
            // record_count
            writeValueOrNull(pageBuilder, RECORD_COUNT_COLUMN_NAME, contentFile::recordCount, BIGINT::writeLong);
            // file_size_in_bytes
            writeValueOrNull(pageBuilder, FILE_SIZE_IN_BYTES_COLUMN_NAME, contentFile::fileSizeInBytes, BIGINT::writeLong);
            // column_sizes
            writeValueOrNull(pageBuilder, COLUMN_SIZES_COLUMN_NAME, contentFile::columnSizes,
                    FilesTablePageSource::writeIntegerBigintInMap);
            // value_counts
            writeValueOrNull(pageBuilder, VALUE_COUNTS_COLUMN_NAME, contentFile::valueCounts,
                    FilesTablePageSource::writeIntegerBigintInMap);
            // null_value_counts
            writeValueOrNull(pageBuilder, NULL_VALUE_COUNTS_COLUMN_NAME, contentFile::nullValueCounts,
                    FilesTablePageSource::writeIntegerBigintInMap);
            // nan_value_counts
            writeValueOrNull(pageBuilder, NAN_VALUE_COUNTS_COLUMN_NAME, contentFile::nanValueCounts,
                    FilesTablePageSource::writeIntegerBigintInMap);
            // lower_bounds
            writeValueOrNull(pageBuilder, LOWER_BOUNDS_COLUMN_NAME, contentFile::lowerBounds,
                    this::writeIntegerVarcharInMap);
            // upper_bounds
            writeValueOrNull(pageBuilder, UPPER_BOUNDS_COLUMN_NAME, contentFile::upperBounds,
                    this::writeIntegerVarcharInMap);
            // key_metadata
            writeValueOrNull(pageBuilder, KEY_METADATA_COLUMN_NAME, contentFile::keyMetadata,
                    (blkBldr, value) -> VARBINARY.writeSlice(blkBldr, Slices.wrappedHeapBuffer(value)));
            // split_offset
            writeValueOrNull(pageBuilder, SPLIT_OFFSETS_COLUMN_NAME, contentFile::splitOffsets,
                    FilesTablePageSource::writeLongInArray);
            // equality_ids
            writeValueOrNull(pageBuilder, EQUALITY_IDS_COLUMN_NAME, contentFile::equalityFieldIds,
                    FilesTablePageSource::writeIntegerInArray);
            // sort_order_id
            writeValueOrNull(pageBuilder, SORT_ORDER_ID_COLUMN_NAME, contentFile::sortOrderId,
                    (blkBldr, value) -> INTEGER.writeLong(blkBldr, value));
            // readable_metrics
            writeValueOrNull(pageBuilder, READABLE_METRICS_COLUMN_NAME, () -> metadataSchema.findField(MetricsUtil.READABLE_METRICS),
                    (blkBldr, value) -> VARCHAR.writeString(blkBldr, readableMetricsToJson(readableMetricsStruct(schema, contentFile, value.type().asStructType()), primitiveFields)));
            readTimeNanos += System.nanoTime() - start;
        }

        if (!pageBuilder.isEmpty()) {
            Page page = pageBuilder.build();
            completedPositions += page.getPositionCount();
            pageBuilder.reset();
            return SourcePage.create(page);
        }

        close();
        return null;
    }

    @Override
    public long getMemoryUsage()
    {
        return pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private <T> void writeValueOrNull(PageBuilder pageBuilder, String columnName, Supplier<T> valueSupplier, BiConsumer<BlockBuilder, T> valueWriter)
    {
        Integer channel = columnNameToIndex.get(columnName);
        if (channel == null) {
            return;
        }

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
        T value = valueSupplier.get();
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            valueWriter.accept(blockBuilder, value);
        }
    }

    private static void writeLongInArray(BlockBuilder blockBuilder, List<Long> values)
    {
        if (blockBuilder instanceof ArrayBlockBuilder arrayBlockBuilder) {
            arrayBlockBuilder.buildEntry(builder ->
                    values.forEach(value -> BIGINT.writeLong(builder, value)));
        }
    }

    private static void writeIntegerInArray(BlockBuilder blockBuilder, List<Integer> values)
    {
        if (blockBuilder instanceof ArrayBlockBuilder arrayBlockBuilder) {
            arrayBlockBuilder.buildEntry(builder ->
                    values.forEach(value -> INTEGER.writeInt(builder, value)));
        }
    }

    private static void writeIntegerBigintInMap(BlockBuilder blockBuilder, Map<Integer, Long> values)
    {
        if (blockBuilder instanceof MapBlockBuilder mapBlockBuilder) {
            mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
                INTEGER.writeInt(keyBuilder, key);
                BIGINT.writeLong(valueBuilder, value);
            }));
        }
    }

    private void writeIntegerVarcharInMap(BlockBuilder blockBuilder, Map<Integer, ByteBuffer> values)
    {
        if (blockBuilder instanceof MapBlockBuilder mapBlockBuilder) {
            mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
                values.forEach((key, value) -> {
                    if (idToTypeMapping.containsKey(key)) {
                        INTEGER.writeInt(keyBuilder, key);
                        VARCHAR.writeString(valueBuilder, Transforms.identity().toHumanString(
                                idToTypeMapping.get(key),
                                Conversions.fromByteBuffer(idToTypeMapping.get(key), value)));
                    }
                });
            });
        }
    }
}
