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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.util.JsonUtils;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetricsUtil.ReadableColMetricsStruct;
import org.apache.iceberg.MetricsUtil.ReadableMetricsStruct;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionColumnType;
import static io.trino.plugin.iceberg.IcebergUtil.partitionTypes;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.MetricsUtil.readableMetricsStruct;

public class FilesTable
        implements SystemTable
{
    private static final JsonFactory JSON_FACTORY = JsonUtils.jsonFactoryBuilder().build();

    private static final String CONTENT_COLUMN_NAME = "content";
    private static final String FILE_PATH_COLUMN_NAME = "file_path";
    private static final String FILE_FORMAT_COLUMN_NAME = "file_format";
    private static final String SPEC_ID_COLUMN_NAME = "spec_id";
    private static final String PARTITION_COLUMN_NAME = "partition";
    private static final String RECORD_COUNT_COLUMN_NAME = "record_count";
    private static final String FILE_SIZE_IN_BYTES_COLUMN_NAME = "file_size_in_bytes";
    private static final String COLUMN_SIZES_COLUMN_NAME = "column_sizes";
    private static final String VALUE_COUNTS_COLUMN_NAME = "value_counts";
    private static final String NULL_VALUE_COUNTS_COLUMN_NAME = "null_value_counts";
    private static final String NAN_VALUE_COUNTS_COLUMN_NAME = "nan_value_counts";
    private static final String LOWER_BOUNDS_COLUMN_NAME = "lower_bounds";
    private static final String UPPER_BOUNDS_COLUMN_NAME = "upper_bounds";
    private static final String KEY_METADATA_COLUMN_NAME = "key_metadata";
    private static final String SPLIT_OFFSETS_COLUMN_NAME = "split_offsets";
    private static final String EQUALITY_IDS_COLUMN_NAME = "equality_ids";
    private static final String SORT_ORDER_ID_COLUMN_NAME = "sort_order_id";
    private static final String READABLE_METRICS_COLUMN_NAME = "readable_metrics";

    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;
    private final Map<Integer, Type> idToTypeMapping;
    private final List<PartitionField> partitionFields;
    private final Optional<IcebergPartitionColumn> partitionColumnType;
    private final Map<Integer, Type.PrimitiveType> idToPrimitiveTypeMapping;
    private final List<Types.NestedField> primitiveFields;
    private final ExecutorService executor;
    private final IcebergFileSystemFactory fileSystemFactory;

    public FilesTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, Optional<Long> snapshotId, ExecutorService executor, IcebergFileSystemFactory fileSystemFactory)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");

        this.idToTypeMapping = getIcebergIdToTypeMapping(icebergTable.schema());
        this.partitionFields = PartitionsTable.getAllPartitionFields(icebergTable);
        this.partitionColumnType = getPartitionColumnType(partitionFields, icebergTable.schema(), typeManager);
        this.idToPrimitiveTypeMapping = IcebergUtil.primitiveFieldTypes(icebergTable.schema());
        this.primitiveFields = IcebergUtil.primitiveFields(icebergTable.schema()).stream()
                .sorted(Comparator.comparing(Types.NestedField::name))
                .collect(toImmutableList());

        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        columns.add(new ColumnMetadata(CONTENT_COLUMN_NAME, INTEGER));
        columns.add(new ColumnMetadata(FILE_PATH_COLUMN_NAME, VARCHAR));
        columns.add(new ColumnMetadata(FILE_FORMAT_COLUMN_NAME, VARCHAR));
        columns.add(new ColumnMetadata(SPEC_ID_COLUMN_NAME, INTEGER));
        partitionColumnType.ifPresent(type -> columns.add(new ColumnMetadata(PARTITION_COLUMN_NAME, type.rowType())));
        columns.add(new ColumnMetadata(RECORD_COUNT_COLUMN_NAME, BIGINT));
        columns.add(new ColumnMetadata(FILE_SIZE_IN_BYTES_COLUMN_NAME, BIGINT));
        columns.add(new ColumnMetadata(COLUMN_SIZES_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(VALUE_COUNTS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(NULL_VALUE_COUNTS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(NAN_VALUE_COUNTS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(LOWER_BOUNDS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))));
        columns.add(new ColumnMetadata(UPPER_BOUNDS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))));
        columns.add(new ColumnMetadata(KEY_METADATA_COLUMN_NAME, VARBINARY));
        columns.add(new ColumnMetadata(SPLIT_OFFSETS_COLUMN_NAME, new ArrayType(BIGINT)));
        columns.add(new ColumnMetadata(EQUALITY_IDS_COLUMN_NAME, new ArrayType(INTEGER)));
        columns.add(new ColumnMetadata(SORT_ORDER_ID_COLUMN_NAME, INTEGER));
        columns.add(new ColumnMetadata(READABLE_METRICS_COLUMN_NAME, typeManager.getType(new TypeSignature(JSON))));

        this.tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), columns.build());
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
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
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split)
    {
        return new FilesTablePageSource(
                fileSystemFactory.create(session.getIdentity(), Collections.emptyMap()),
                icebergTable.schema(),
                icebergTable.specs(),
                idToTypeMapping,
                partitionFields,
                partitionColumnType,
                idToPrimitiveTypeMapping,
                primitiveFields,
                tableMetadata.getColumns().stream().map(ColumnMetadata::getType).toList(),
                (FilesTableSplit) split);
    }

    @Override
    public Optional<ConnectorSplitSource> splitSource(ConnectorSession connectorSession, TupleDomain<ColumnHandle> constraint)
    {
        TableScan scan = icebergTable.newScan()
                .includeColumnStats()
                .planWith(executor);

        snapshotId.ifPresent(scan::useSnapshot);

        return Optional.of(new FilesTableSplitSource(scan.snapshot().allManifests(icebergTable.io())));
    }

    public record FilesTableSplit(String manifestFileEncoded)
            implements ConnectorSplit
    {
    }

    public static class FilesTablePageSource
            implements ConnectorPageSource
    {
        private final Closer closer;
        private final Schema schema;
        private final Map<Integer, Type> idToTypeMapping;
        private final List<PartitionField> partitionFields;
        private final Optional<IcebergPartitionColumn> partitionColumnType;
        private final Map<Integer, Type.PrimitiveType> idToPrimitiveTypeMapping;
        private final List<Types.NestedField> primitiveFields;
        private final Iterator<? extends ContentFile<?>> contentItr;
        private final PageBuilder pageBuilder;

        public FilesTablePageSource(
                TrinoFileSystem trinoFileSystem,
                Schema schema,
                Map<Integer, PartitionSpec> partitionSpecs,
                Map<Integer, Type> idToTypeMapping,
                List<PartitionField> partitionFields,
                Optional<IcebergPartitionColumn> partitionColumnType,
                Map<Integer, Type.PrimitiveType> idToPrimitiveTypeMapping,
                List<Types.NestedField> primitiveFields,
                List<? extends io.trino.spi.type.Type> columnTypes,
                FilesTableSplit split)
        {
            this.closer = Closer.create();
            this.schema = schema;
            this.idToTypeMapping = idToTypeMapping;
            this.partitionColumnType = partitionColumnType;
            this.idToPrimitiveTypeMapping = idToPrimitiveTypeMapping;
            this.partitionFields = partitionFields;
            this.primitiveFields = primitiveFields;
            try {
                ManifestFile manifestFile = ManifestFiles.decode(Base64.getDecoder().decode(split.manifestFileEncoded()));
                ManifestReader<? extends ContentFile<?>> manifestReader = closer.register(manifestFile.content() == ManifestContent.DATA ?
                        ManifestFiles.read(manifestFile, new ForwardingFileIo(trinoFileSystem)) :
                        ManifestFiles.readDeleteManifest(manifestFile, new ForwardingFileIo(trinoFileSystem), partitionSpecs));
                this.contentItr = manifestReader.iterator();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.pageBuilder = new PageBuilder(columnTypes);
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            while (contentItr.hasNext() && !pageBuilder.isFull()) {
                pageBuilder.declarePosition();
                ContentFile<?> contentFile = contentItr.next();
                contentFile.columnSizes();

                // content
                INTEGER.writeLong(pageBuilder.getBlockBuilder(0), contentFile.content().id());
                // file_path
                VARCHAR.writeString(pageBuilder.getBlockBuilder(1), contentFile.location());
                // file_format
                VARCHAR.writeString(pageBuilder.getBlockBuilder(2), contentFile.format().toString());
                // spec_id
                INTEGER.writeLong(pageBuilder.getBlockBuilder(3), contentFile.specId());
                // partitions
                if (partitionColumnType.isPresent()) {
                    List<Type> partitionTypes = partitionTypes(partitionFields, idToPrimitiveTypeMapping);
                    List<io.trino.spi.type.Type> partitionColumnTypes = partitionColumnType.orElseThrow().rowType().getFields().stream()
                            .map(RowType.Field::getType)
                            .collect(toImmutableList());

                    if (pageBuilder.getBlockBuilder(4) instanceof RowBlockBuilder rowBlockBuilder) {
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
                BIGINT.writeLong(pageBuilder.getBlockBuilder(5), contentFile.recordCount());
                // file_size_in_bytes
                BIGINT.writeLong(pageBuilder.getBlockBuilder(6), contentFile.fileSizeInBytes());
                // column_sizes
                writeIntegerBigint(pageBuilder.getBlockBuilder(7), contentFile.columnSizes());
                // value_counts
                writeIntegerBigint(pageBuilder.getBlockBuilder(8), contentFile.valueCounts());
                // null_value_counts
                writeIntegerBigint(pageBuilder.getBlockBuilder(9), contentFile.nullValueCounts());
                // lower_bounds
                writeIntegerVarchar(pageBuilder.getBlockBuilder(10), contentFile.lowerBounds());
                // upper_bounds
                writeIntegerVarchar(pageBuilder.getBlockBuilder(11), contentFile.upperBounds());
                // key_metadata
                VARBINARY.writeSlice(pageBuilder.getBlockBuilder(12), Slices.wrappedHeapBuffer(contentFile.keyMetadata()));
                // split_offset
                contentFile.splitOffsets().forEach(offset -> BIGINT.writeLong(pageBuilder.getBlockBuilder(13), offset));
                // equality_ids
                contentFile.equalityFieldIds().forEach(id -> INTEGER.writeLong(pageBuilder.getBlockBuilder(14), id));
                // sort_order_id
                INTEGER.writeLong(pageBuilder.getBlockBuilder(15), contentFile.sortOrderId());
                // readable_metrics
                VARCHAR.writeString(
                        pageBuilder.getBlockBuilder(16),
                        toJson(readableMetricsStruct(schema, contentFile, schema.findField(READABLE_METRICS_COLUMN_NAME).type().asStructType()), primitiveFields));
            }

            if (!pageBuilder.isEmpty()) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return SourcePage.create(page);
            }

            return null;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
            closer.close();
        }

        private static void writeIntegerBigint(BlockBuilder blockBuilder, Map<Integer, Long> values)
        {
            if (blockBuilder instanceof MapBlockBuilder mapBlockBuilder) {
                mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
                    INTEGER.writeLong(keyBuilder, key);
                    BIGINT.writeLong(valueBuilder, value);
                }));
            }
        }

        private void writeIntegerVarchar(BlockBuilder blockBuilder, Map<Integer, ByteBuffer> values)
        {
            if (blockBuilder instanceof MapBlockBuilder mapBlockBuilder) {
                mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
                    values.forEach((key, value) -> {
                        INTEGER.writeLong(keyBuilder, key);
                        VARCHAR.writeString(valueBuilder, Transforms.identity().toHumanString(
                                idToTypeMapping.get(key),
                                Conversions.fromByteBuffer(idToTypeMapping.get(key), value)));
                    });
                });
            }
        }
    }

    public static class FilesTableSplitSource
            implements ConnectorSplitSource
    {
        private final Iterator<ManifestFile> manifestFileItr;

        public FilesTableSplitSource(List<ManifestFile> manifestFiles)
        {
            this.manifestFileItr = requireNonNull(manifestFiles, "manifestFiles is null").iterator();
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            List<ConnectorSplit> splits = new ArrayList<>();

            while (manifestFileItr.hasNext() && splits.size() < maxSize) {
                try {
                    splits.add(new FilesTableSplit(Base64.getEncoder().encodeToString(ManifestFiles.encode(manifestFileItr.next()))));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return completedFuture(new ConnectorSplitBatch(splits, !manifestFileItr.hasNext()));
        }

        @Override
        public void close()
        {
            // do nothing
        }

        @Override
        public boolean isFinished()
        {
            return manifestFileItr != null && !manifestFileItr.hasNext();
        }
    }

    static String toJson(ReadableMetricsStruct readableMetrics, List<Types.NestedField> primitiveFields)
    {
        StringWriter writer = new StringWriter();
        try {
            JsonGenerator generator = JSON_FACTORY.createGenerator(writer);
            generator.writeStartObject();

            for (int i = 0; i < readableMetrics.size(); i++) {
                Types.NestedField field = primitiveFields.get(i);
                generator.writeFieldName(field.name());

                generator.writeStartObject();
                ReadableColMetricsStruct columnMetrics = readableMetrics.get(i, ReadableColMetricsStruct.class);

                generator.writeFieldName("column_size");
                Long columnSize = columnMetrics.get(0, Long.class);
                if (columnSize == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(columnSize);
                }

                generator.writeFieldName("value_count");
                Long valueCount = columnMetrics.get(1, Long.class);
                if (valueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(valueCount);
                }

                generator.writeFieldName("null_value_count");
                Long nullValueCount = columnMetrics.get(2, Long.class);
                if (nullValueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(nullValueCount);
                }

                generator.writeFieldName("nan_value_count");
                Long nanValueCount = columnMetrics.get(3, Long.class);
                if (nanValueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(nanValueCount);
                }

                generator.writeFieldName("lower_bound");
                SingleValueParser.toJson(field.type(), columnMetrics.get(4, Object.class), generator);

                generator.writeFieldName("upper_bound");
                SingleValueParser.toJson(field.type(), columnMetrics.get(5, Object.class), generator);

                generator.writeEndObject();
            }

            generator.writeEndObject();
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException("JSON conversion failed for: " + readableMetrics, e);
        }
    }

    static Map<Integer, Type> getIcebergIdToTypeMapping(Schema schema)
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
