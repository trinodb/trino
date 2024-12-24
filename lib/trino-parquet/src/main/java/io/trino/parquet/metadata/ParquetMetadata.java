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
package io.trino.parquet.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.parquet.reader.TrinoColumnIndexStore;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.parquet.ParquetMetadataConverter.convertEncodingStats;
import static io.trino.parquet.ParquetMetadataConverter.getEncoding;
import static io.trino.parquet.ParquetMetadataConverter.getLogicalTypeAnnotation;
import static io.trino.parquet.ParquetMetadataConverter.getPrimitive;
import static io.trino.parquet.ParquetMetadataConverter.toColumnIndexReference;
import static io.trino.parquet.ParquetMetadataConverter.toOffsetIndexReference;
import static io.trino.parquet.ParquetValidationUtils.validateParquet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class ParquetMetadata
{
    private static final Logger log = Logger.get(ParquetMetadata.class);

    private final FileMetaData fileMetaData;
    private final MessageType messageType;
    private final ParquetDataSourceId dataSourceId;
    private final FileMetadata parquetMetadata;
    private final Optional<DiskRange> diskRange;

    public ParquetMetadata(FileMetaData fileMetaData, ParquetDataSourceId dataSourceId, Optional<DiskRange> diskRange)
            throws ParquetCorruptionException
    {
        this.fileMetaData = requireNonNull(fileMetaData, "fileMetaData is null");
        this.messageType = readMessageType();
        this.dataSourceId = requireNonNull(dataSourceId, "dataSourceId is null");
        this.diskRange = requireNonNull(diskRange, "range is null");
        this.parquetMetadata = new FileMetadata(messageType, keyValueMetaData(fileMetaData), fileMetaData.getCreated_by());
    }

    public FileMetadata getFileMetaData()
    {
        return parquetMetadata;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataSourceId", dataSourceId)
                .add("fileMetaData", fileMetaData)
                .add("diskRange", diskRange)
                .toString();
    }

    private List<RowGroupOffset> getRowGroups()
    {
        List<RowGroup> rowGroups = fileMetaData.getRow_groups();
        if (rowGroups == null) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<RowGroupOffset> builder = ImmutableList.builder();
        long lastRowCount = 0;
        long fileRowCount = 0;
        for (RowGroup rowGroup : rowGroups) {
            fileRowCount += lastRowCount;
            lastRowCount = rowGroup.getNum_rows();
            if (diskRange.isPresent()) {
                /*
                 * Only return row group when offset range covering blockStart.
                 * If blockStart is not covered, row group is not returned even if offset range overlaps.
                 * This can avoid row group duplication in case of multiple splits.
                 */
                long blockStart = rowGroup.getColumns().getFirst().meta_data.data_page_offset;
                boolean splitContainsBlock = diskRange.get().getOffset() <= blockStart && blockStart < diskRange.get().getEnd();
                if (!splitContainsBlock) {
                    continue;
                }
            }

            builder.add(new RowGroupOffset(rowGroup, fileRowCount));
        }

        return builder.build();
    }

    private ColumnChunkMetadata toColumnChunkMetadata(MessageType messageType, ColumnChunk columnChunk, ColumnPath columnPath)
    {
        ColumnMetaData metaData = columnChunk.meta_data;
        PrimitiveType primitiveType = messageType.getType(columnPath.toArray()).asPrimitiveType();
        ColumnChunkMetadata column = ColumnChunkMetadata.get(
                columnPath,
                primitiveType,
                CompressionCodecName.fromParquet(metaData.codec),
                convertEncodingStats(metaData.encoding_stats),
                readEncodings(metaData.encodings),
                MetadataReader.readStats(Optional.ofNullable(fileMetaData.getCreated_by()), Optional.ofNullable(metaData.statistics), primitiveType),
                metaData.data_page_offset,
                metaData.dictionary_page_offset,
                metaData.num_values,
                metaData.total_compressed_size,
                metaData.total_uncompressed_size);
        column.setColumnIndexReference(toColumnIndexReference(columnChunk));
        column.setOffsetIndexReference(toOffsetIndexReference(columnChunk));
        column.setBloomFilterOffset(metaData.bloom_filter_offset);

        return column;
    }

    public List<RowGroupInfo> getRowGroupInfo()
            throws ParquetCorruptionException
    {
        return getRowGroupInfo(Optional.empty(), Optional.empty());
    }

    public List<RowGroupInfo> getRowGroupInfo(Optional<ParquetDataSource> dataSource, Optional<Map<List<String>, ColumnDescriptor>> descriptorsByPath)
            throws ParquetCorruptionException
    {
        Optional<Set<ColumnPath>> filterColumnPaths = descriptorsByPath.map(dp ->
                dp.keySet().stream()
                        .map(p -> p.toArray(new String[0]))
                        .map(ColumnPath::get)
                        .collect(toImmutableSet()));
        ImmutableList.Builder<RowGroupInfo> rowGroupInfoBuilder = ImmutableList.builder();
        for (RowGroupOffset rowGroupOffset : getRowGroups()) {
            List<ColumnChunk> columns = rowGroupOffset.rowGroup.getColumns();
            validateParquet(!columns.isEmpty(), dataSourceId, "No columns in row group: %s", rowGroupOffset.rowGroup);
            String filePath = columns.getFirst().getFile_path();

            ImmutableMap.Builder<ColumnPath, ColumnChunkMetadata> columnMetadataBuilder = ImmutableMap.builderWithExpectedSize(columns.size());

            for (ColumnChunk columnChunk : columns) {
                checkState((filePath == null && columnChunk.getFile_path() == null)
                                || (filePath != null && filePath.equals(columnChunk.getFile_path())),
                        "all column chunks of the same row group must be in the same file [%s]", dataSourceId);
                ColumnPath columnPath = toColumnPath(columnChunk);
                if (filterColumnPaths.isEmpty() || filterColumnPaths.get().contains(columnPath)) {
                    ColumnChunkMetadata chunkMetadata = toColumnChunkMetadata(messageType, columnChunk, columnPath);
                    columnMetadataBuilder.put(columnPath, chunkMetadata);
                }
            }
            Map<ColumnPath, ColumnChunkMetadata> columnChunkMetadata = columnMetadataBuilder.buildOrThrow();

            if (filterColumnPaths.isPresent() && filterColumnPaths.get().size() != columnChunkMetadata.size()) {
                Set<List<String>> existingPaths = columns.stream()
                        .map(ParquetMetadata::toColumnPath)
                        .map(p -> ImmutableList.copyOf(p.toArray()))
                        .collect(toImmutableSet());
                for (Map.Entry<List<String>, ColumnDescriptor> entry : descriptorsByPath.get().entrySet()) {
                    if (!existingPaths.contains(entry.getKey())) {
                        throw new ParquetCorruptionException(dataSourceId, "Metadata is missing for column: %s", entry.getValue());
                    }
                }
            }

            PrunedBlockMetadata columnsMetadata = new PrunedBlockMetadata(rowGroupOffset.rowGroup.getNum_rows(), dataSourceId, columnChunkMetadata);
            Optional<ColumnIndexStore> indexStore = Optional.empty();
            if (filterColumnPaths.isPresent() && dataSource.isPresent()) {
                indexStore = Optional.of(new TrinoColumnIndexStore(dataSource.get(), columnsMetadata.getBlockMetadata(), filterColumnPaths.get(), ImmutableSet.of()));
            }
            rowGroupInfoBuilder.add(new RowGroupInfo(columnsMetadata, rowGroupOffset.offset, indexStore));
        }

        return rowGroupInfoBuilder.build();
    }

    private MessageType readMessageType()
            throws ParquetCorruptionException
    {
        List<SchemaElement> schema = fileMetaData.getSchema();
        validateParquet(!schema.isEmpty(), dataSourceId, "Schema is empty");

        Iterator<SchemaElement> schemaIterator = schema.iterator();
        SchemaElement rootSchema = schemaIterator.next();
        Types.MessageTypeBuilder builder = Types.buildMessage();
        readTypeSchema(builder, schemaIterator, rootSchema.getNum_children());
        return builder.named(rootSchema.name);
    }

    private static ColumnPath toColumnPath(ColumnChunk columnChunk)
    {
        String[] paths = columnChunk.meta_data.path_in_schema.stream()
                .map(value -> value.toLowerCase(Locale.ENGLISH))
                .toArray(String[]::new);
        return ColumnPath.get(paths);
    }

    private static void readTypeSchema(Types.GroupBuilder<?> builder, Iterator<SchemaElement> schemaIterator, int typeCount)
    {
        for (int i = 0; i < typeCount; i++) {
            SchemaElement element = schemaIterator.next();
            Types.Builder<?, ?> typeBuilder;
            if (element.type == null) {
                typeBuilder = builder.group(Type.Repetition.valueOf(element.repetition_type.name()));
                readTypeSchema((Types.GroupBuilder<?>) typeBuilder, schemaIterator, element.num_children);
            }
            else {
                Types.PrimitiveBuilder<?> primitiveBuilder = builder.primitive(getPrimitive(element.type), Type.Repetition.valueOf(element.repetition_type.name()));
                if (element.isSetType_length()) {
                    primitiveBuilder.length(element.type_length);
                }
                if (element.isSetPrecision()) {
                    primitiveBuilder.precision(element.precision);
                }
                if (element.isSetScale()) {
                    primitiveBuilder.scale(element.scale);
                }
                typeBuilder = primitiveBuilder;
            }

            // Reading of element.logicalType and element.converted_type corresponds to parquet-mr's code at
            // https://github.com/apache/parquet-mr/blob/apache-parquet-1.12.0/parquet-hadoop/src/main/java/org/apache/parquet/format/converter/ParquetMetadataConverter.java#L1568-L1582
            LogicalTypeAnnotation annotationFromLogicalType = null;
            if (element.isSetLogicalType()) {
                annotationFromLogicalType = getLogicalTypeAnnotation(element.logicalType);
                typeBuilder.as(annotationFromLogicalType);
            }
            if (element.isSetConverted_type()) {
                LogicalTypeAnnotation annotationFromConvertedType = getLogicalTypeAnnotation(element.converted_type, element);
                if (annotationFromLogicalType != null) {
                    // Both element.logicalType and element.converted_type set
                    if (annotationFromLogicalType.toOriginalType() == annotationFromConvertedType.toOriginalType()) {
                        // element.converted_type matches element.logicalType, even though annotationFromLogicalType may differ from annotationFromConvertedType
                        // Following parquet-mr behavior, we favor LogicalTypeAnnotation derived from element.logicalType, as potentially containing more information.
                    }
                    else {
                        // Following parquet-mr behavior, issue warning and let converted_type take precedence.
                        log.warn("Converted type and logical type metadata map to different OriginalType (convertedType: %s, logical type: %s). Using value in converted type.",
                                element.converted_type, element.logicalType);
                        // parquet-mr reads only OriginalType from converted_type. We retain full LogicalTypeAnnotation
                        // 1. for compatibility, as previous Trino reader code would read LogicalTypeAnnotation from element.converted_type and some additional fields.
                        // 2. so that we override LogicalTypeAnnotation annotation read from element.logicalType in case of mismatch detected.
                        typeBuilder.as(annotationFromConvertedType);
                    }
                }
                else {
                    // parquet-mr reads only OriginalType from converted_type. We retain full LogicalTypeAnnotation for compatibility, as previous
                    // Trino reader code would read LogicalTypeAnnotation from element.converted_type and some additional fields.
                    typeBuilder.as(annotationFromConvertedType);
                }
            }

            if (element.isSetField_id()) {
                typeBuilder.id(element.field_id);
            }
            typeBuilder.named(element.name.toLowerCase(Locale.ENGLISH));
        }
    }

    private static Set<Encoding> readEncodings(List<org.apache.parquet.format.Encoding> encodings)
    {
        Set<org.apache.parquet.column.Encoding> columnEncodings = new HashSet<>();
        for (org.apache.parquet.format.Encoding encoding : encodings) {
            columnEncodings.add(getEncoding(encoding));
        }
        return Collections.unmodifiableSet(columnEncodings);
    }

    private static Map<String, String> keyValueMetaData(FileMetaData fileMetaData)
    {
        if (fileMetaData.getKey_value_metadata() == null) {
            return ImmutableMap.of();
        }
        return fileMetaData.getKey_value_metadata().stream().collect(toMap(KeyValue::getKey, KeyValue::getValue));
    }

    private record RowGroupOffset(RowGroup rowGroup, long offset)
    {
    }
}
