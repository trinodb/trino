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
package io.trino.plugin.iceberg.system.entries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.plugin.iceberg.system.EntriesTable;
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
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.IcebergManifestUtils.ManifestEntryWithMetadata;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.IcebergUtil.readerForManifest;
import static io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex.createStructLikeWrapper;
import static io.trino.plugin.iceberg.system.EntriesTable.DATA_FILE_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.EntriesTable.FILE_SEQUENCE_NUMBER_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.EntriesTable.READABLE_METRICS_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.EntriesTable.SEQUENCE_NUMBER_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.EntriesTable.SNAPSHOT_ID_COLUMN_NAME;
import static io.trino.plugin.iceberg.system.EntriesTable.STATUS_COLUMN_NAME;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getPartitionColumnType;
import static io.trino.plugin.iceberg.util.SystemTableUtil.partitionTypes;
import static io.trino.plugin.iceberg.util.SystemTableUtil.readableMetricsToJson;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.IcebergManifestUtils.entriesWithMetadata;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.apache.iceberg.MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER;
import static org.apache.iceberg.MetadataColumns.ROW_ID;
import static org.apache.iceberg.MetricsUtil.readableMetricsStruct;

/**
 * Reads a single manifest and emits its {@code $entries}/{@code $all_entries} rows: every entry (ADDED/EXISTING/DELETED)
 * with its status and sequence metadata, plus the {@code data_file} ROW and {@code readable_metrics}. Mirrors
 * {@link io.trino.plugin.iceberg.system.files.FilesTablePageSource} but, unlike {@code $files} (live files only),
 * iterates <em>all</em> entries and nests the file columns inside the {@code data_file} ROW.
 */
public final class EntriesTablePageSource
        implements ConnectorPageSource
{
    private final Closer closer = Closer.create();
    private final Schema schema;
    private final Schema metadataSchema;
    private final Map<Integer, PrimitiveType> idToTypeMapping;
    private final Map<Integer, PartitionSpec> idToPartitionSpecMapping;
    private final Optional<IcebergPartitionColumn> partitionColumn;
    private final List<Type> partitionTrinoTypes;
    private final List<org.apache.iceberg.types.Type> partitionIcebergTypes;
    private final List<NestedField> primitiveFields;
    private final RowType dataFileType;
    private final Type readableMetricsType;
    private final Iterator<ManifestEntryWithMetadata> entryIterator;
    private final Map<String, Integer> columnNameToIndex;
    private final PageBuilder pageBuilder;
    private final long completedBytes;
    private long completedPositions;
    private boolean closed;

    public EntriesTablePageSource(
            TypeManager typeManager,
            TrinoFileSystem trinoFileSystem,
            ForwardingFileIoFactory fileIoFactory,
            List<String> requiredColumns,
            EntriesTableSplit split)
    {
        this.schema = SchemaParser.fromJson(requireNonNull(split.schemaJson(), "schemaJson is null"));
        this.metadataSchema = SchemaParser.fromJson(requireNonNull(split.metadataSchemaJson(), "metadataSchemaJson is null"));
        this.idToPartitionSpecMapping = split.partitionSpecsByIdJson().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> PartitionSpecParser.fromJson(schema, entry.getValue())));
        // Row id and last updated sequence number may be written to a v3 file, so include their types for bounds conversion.
        this.idToTypeMapping = ImmutableMap.<Integer, PrimitiveType>builder()
                .putAll(primitiveFieldTypes(schema))
                .put(ROW_ID.fieldId(), (PrimitiveType) ROW_ID.type())
                .put(LAST_UPDATED_SEQUENCE_NUMBER.fieldId(), (PrimitiveType) LAST_UPDATED_SEQUENCE_NUMBER.type())
                .buildOrThrow();
        List<PartitionField> partitionFields = getAllPartitionFields(schema, idToPartitionSpecMapping);
        this.partitionColumn = getPartitionColumnType(typeManager, partitionFields, schema);
        this.partitionTrinoTypes = partitionColumn.isPresent()
                ? partitionColumn.get().rowType().getFields().stream().map(RowType.Field::getType).collect(toImmutableList())
                : ImmutableList.of();
        this.partitionIcebergTypes = partitionTypes(partitionFields, idToTypeMapping);
        this.primitiveFields = IcebergUtil.primitiveFields(schema).stream()
                .sorted(Comparator.comparing(NestedField::name))
                .collect(toImmutableList());
        this.dataFileType = EntriesTable.dataFileRowType(typeManager, schema, idToPartitionSpecMapping);
        this.readableMetricsType = typeManager.getType(new TypeSignature(JSON));

        ManifestReader<? extends ContentFile<?>> manifestReader = closer.register(
                readerForManifest(split.manifestFile(), fileIoFactory.create(trinoFileSystem), idToPartitionSpecMapping));
        this.entryIterator = closer.register(entriesWithMetadata(manifestReader).iterator());

        this.pageBuilder = new PageBuilder(requiredColumns.stream().map(this::columnType).collect(toImmutableList()));
        this.columnNameToIndex = mapWithIndex(
                requiredColumns.stream(),
                (columnName, position) -> immutableEntry(columnName, Long.valueOf(position).intValue()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        this.completedBytes = split.manifestFile().length();
    }

    private Type columnType(String columnName)
    {
        return switch (columnName) {
            case STATUS_COLUMN_NAME -> INTEGER;
            case SNAPSHOT_ID_COLUMN_NAME, SEQUENCE_NUMBER_COLUMN_NAME, FILE_SEQUENCE_NUMBER_COLUMN_NAME -> BIGINT;
            case DATA_FILE_COLUMN_NAME -> dataFileType;
            case READABLE_METRICS_COLUMN_NAME -> readableMetricsType; // JSON is varchar-backed; values written via VARCHAR.writeString
            default -> throw new IllegalArgumentException("Unexpected column: " + columnName);
        };
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
        return 0;
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

        while (entryIterator.hasNext() && !pageBuilder.isFull()) {
            pageBuilder.declarePosition();
            ManifestEntryWithMetadata entry = entryIterator.next();
            ContentFile<?> contentFile = entry.file();

            writeInteger(STATUS_COLUMN_NAME, entry.status());
            writeBigint(SNAPSHOT_ID_COLUMN_NAME, entry.snapshotId());
            writeBigint(SEQUENCE_NUMBER_COLUMN_NAME, entry.sequenceNumber());
            writeBigint(FILE_SEQUENCE_NUMBER_COLUMN_NAME, entry.fileSequenceNumber());
            writeDataFile(contentFile);
            writeReadableMetrics(contentFile);
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

    private void writeInteger(String columnName, Integer value)
    {
        Integer channel = columnNameToIndex.get(columnName);
        if (channel == null) {
            return;
        }
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            INTEGER.writeLong(blockBuilder, value);
        }
    }

    private void writeBigint(String columnName, Long value)
    {
        Integer channel = columnNameToIndex.get(columnName);
        if (channel == null) {
            return;
        }
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, value);
        }
    }

    private void writeReadableMetrics(ContentFile<?> contentFile)
    {
        Integer channel = columnNameToIndex.get(READABLE_METRICS_COLUMN_NAME);
        if (channel == null) {
            return;
        }
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
        NestedField readableMetricsField = metadataSchema.findField(MetricsUtil.READABLE_METRICS);
        if (readableMetricsField == null) {
            blockBuilder.appendNull();
            return;
        }
        String json = readableMetricsToJson(
                readableMetricsStruct(schema, contentFile, readableMetricsField.type().asStructType()),
                primitiveFields);
        VARCHAR.writeString(blockBuilder, json);
    }

    private void writeDataFile(ContentFile<?> contentFile)
    {
        Integer channel = columnNameToIndex.get(DATA_FILE_COLUMN_NAME);
        if (channel == null) {
            return;
        }
        RowBlockBuilder blockBuilder = (RowBlockBuilder) pageBuilder.getBlockBuilder(channel);
        FileContent content = contentFile.content();
        blockBuilder.buildEntry(fieldBuilders -> {
            int field = 0;
            INTEGER.writeLong(fieldBuilders.get(field++), content.id());
            VARCHAR.writeString(fieldBuilders.get(field++), contentFile.location());
            VARCHAR.writeString(fieldBuilders.get(field++), contentFile.format().toString());
            INTEGER.writeLong(fieldBuilders.get(field++), contentFile.specId());

            if (partitionColumn.isPresent()) {
                appendPartition((RowBlockBuilder) fieldBuilders.get(field++), contentFile);
            }

            BIGINT.writeLong(fieldBuilders.get(field++), contentFile.recordCount());
            BIGINT.writeLong(fieldBuilders.get(field++), contentFile.fileSizeInBytes());
            appendIntegerBigintMap((MapBlockBuilder) fieldBuilders.get(field++), contentFile.columnSizes());
            appendIntegerBigintMap((MapBlockBuilder) fieldBuilders.get(field++), contentFile.valueCounts());
            appendIntegerBigintMap((MapBlockBuilder) fieldBuilders.get(field++), contentFile.nullValueCounts());
            appendIntegerBigintMap((MapBlockBuilder) fieldBuilders.get(field++), contentFile.nanValueCounts());

            if (content == FileContent.POSITION_DELETES) {
                appendBoundsForPositionDelete((MapBlockBuilder) fieldBuilders.get(field++), contentFile.lowerBounds());
                appendBoundsForPositionDelete((MapBlockBuilder) fieldBuilders.get(field++), contentFile.upperBounds());
            }
            else {
                appendIntegerVarcharMap((MapBlockBuilder) fieldBuilders.get(field++), contentFile.lowerBounds());
                appendIntegerVarcharMap((MapBlockBuilder) fieldBuilders.get(field++), contentFile.upperBounds());
            }

            ByteBuffer keyMetadata = contentFile.keyMetadata();
            if (keyMetadata == null) {
                fieldBuilders.get(field++).appendNull();
            }
            else {
                VARBINARY.writeSlice(fieldBuilders.get(field++), Slices.wrappedHeapBuffer(keyMetadata));
            }

            appendBigintArray((ArrayBlockBuilder) fieldBuilders.get(field++), contentFile.splitOffsets());

            // equality_ids only on equality-delete files
            List<Integer> equalityIds = content == FileContent.EQUALITY_DELETES ? contentFile.equalityFieldIds() : null;
            appendIntegerArray((ArrayBlockBuilder) fieldBuilders.get(field++), equalityIds);

            // sort_order_id is optional per the Iceberg spec (null = unsorted) and absent on position-delete files
            Integer sortOrderId = content == FileContent.POSITION_DELETES ? null : contentFile.sortOrderId();
            if (sortOrderId == null) {
                fieldBuilders.get(field).appendNull();
            }
            else {
                INTEGER.writeLong(fieldBuilders.get(field), sortOrderId.longValue());
            }
        });
    }

    private void appendPartition(RowBlockBuilder partitionBlockBuilder, ContentFile<?> contentFile)
    {
        IcebergPartitionColumn column = partitionColumn.orElseThrow();
        PartitionSpec partitionSpec = idToPartitionSpecMapping.get(contentFile.specId());
        StructLikeWrapperWithFieldIdToIndex partitionStruct = createStructLikeWrapper(partitionSpec, contentFile.partition());
        partitionBlockBuilder.buildEntry(fields -> {
            for (int i = 0; i < partitionTrinoTypes.size(); i++) {
                Type trinoType = partitionTrinoTypes.get(i);
                Object value = null;
                Integer fieldId = column.fieldIds().get(i);
                if (partitionStruct.getFieldIdToIndex().containsKey(fieldId)) {
                    org.apache.iceberg.types.Type icebergType = partitionIcebergTypes.get(i);
                    value = convertIcebergValueToTrino(
                            icebergType,
                            partitionStruct.getStructLikeWrapper().get().get(partitionStruct.getFieldIdToIndex().get(fieldId), icebergType.typeId().javaClass()));
                }
                writeNativeValue(trinoType, fields.get(i), value);
            }
        });
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

    private static void appendIntegerBigintMap(MapBlockBuilder blockBuilder, Map<Integer, Long> values)
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

    private void appendIntegerVarcharMap(MapBlockBuilder blockBuilder, Map<Integer, ByteBuffer> values)
    {
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
            org.apache.iceberg.types.Type type = idToTypeMapping.get(key);
            INTEGER.writeLong(keyBuilder, key);
            VARCHAR.writeString(valueBuilder, Transforms.identity().toHumanString(type, Conversions.fromByteBuffer(type, value)));
        }));
    }

    private static void appendBoundsForPositionDelete(MapBlockBuilder blockBuilder, Map<Integer, ByteBuffer> values)
    {
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            ByteBuffer pos = values.get(DELETE_FILE_POS.fieldId());
            if (pos != null) {
                INTEGER.writeLong(keyBuilder, DELETE_FILE_POS.fieldId());
                VARCHAR.writeString(valueBuilder, Transforms.identity().toHumanString(Types.LongType.get(), Conversions.fromByteBuffer(Types.LongType.get(), pos)));
            }
            ByteBuffer path = values.get(DELETE_FILE_PATH.fieldId());
            if (path != null) {
                INTEGER.writeLong(keyBuilder, DELETE_FILE_PATH.fieldId());
                VARCHAR.writeString(valueBuilder, Transforms.identity().toHumanString(Types.StringType.get(), Conversions.fromByteBuffer(Types.StringType.get(), path)));
            }
        });
    }

    private static void appendBigintArray(ArrayBlockBuilder blockBuilder, List<Long> values)
    {
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }
        blockBuilder.buildEntry(elementBuilder -> values.forEach(value -> BIGINT.writeLong(elementBuilder, value)));
    }

    private static void appendIntegerArray(ArrayBlockBuilder blockBuilder, List<Integer> values)
    {
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }
        blockBuilder.buildEntry(elementBuilder -> values.forEach(value -> INTEGER.writeLong(elementBuilder, value)));
    }
}
