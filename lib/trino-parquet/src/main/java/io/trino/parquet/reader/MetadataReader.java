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
package io.trino.parquet.reader;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetWriteValidation;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.parquet.ParquetValidationUtils.validateParquet;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport.WRITER_TIMEZONE;
import static org.apache.parquet.format.Util.readFileMetaData;
import static org.apache.parquet.format.converter.ParquetMetadataConverterUtil.getLogicalTypeAnnotation;

public final class MetadataReader
{
    private static final Logger log = Logger.get(MetadataReader.class);

    private static final Slice MAGIC = Slices.utf8Slice("PAR1");
    private static final int POST_SCRIPT_SIZE = Integer.BYTES + MAGIC.length();
    // Typical 1GB files produced by Trino were found to have footer size between 30-40KB
    private static final int EXPECTED_FOOTER_SIZE = 48 * 1024;
    private static final ParquetMetadataConverter PARQUET_METADATA_CONVERTER = new ParquetMetadataConverter();

    private MetadataReader() {}

    public static ParquetMetadata readFooter(ParquetDataSource dataSource, Optional<ParquetWriteValidation> parquetWriteValidation)
            throws IOException
    {
        // Parquet File Layout:
        //
        // MAGIC
        // variable: Data
        // variable: Metadata
        // 4 bytes: MetadataLength
        // MAGIC

        validateParquet(dataSource.getEstimatedSize() >= MAGIC.length() + POST_SCRIPT_SIZE, "%s is not a valid Parquet File", dataSource.getId());

        // Read the tail of the file
        long estimatedFileSize = dataSource.getEstimatedSize();
        long expectedReadSize = min(estimatedFileSize, EXPECTED_FOOTER_SIZE);
        Slice buffer = dataSource.readTail(toIntExact(expectedReadSize));

        Slice magic = buffer.slice(buffer.length() - MAGIC.length(), MAGIC.length());
        validateParquet(MAGIC.equals(magic), "Not valid Parquet file: %s expected magic number: %s got: %s", dataSource.getId(), MAGIC.toStringUtf8(), magic.toStringUtf8());

        int metadataLength = buffer.getInt(buffer.length() - POST_SCRIPT_SIZE);
        long metadataIndex = estimatedFileSize - POST_SCRIPT_SIZE - metadataLength;
        validateParquet(
                metadataIndex >= MAGIC.length() && metadataIndex < estimatedFileSize - POST_SCRIPT_SIZE,
                "Corrupted Parquet file: %s metadata index: %s out of range",
                dataSource.getId(),
                metadataIndex);

        int completeFooterSize = metadataLength + POST_SCRIPT_SIZE;
        if (completeFooterSize > buffer.length()) {
            // initial read was not large enough, so just read again with the correct size
            buffer = dataSource.readTail(completeFooterSize);
        }
        InputStream metadataStream = buffer.slice(buffer.length() - completeFooterSize, metadataLength).getInput();

        FileMetaData fileMetaData = readFileMetaData(metadataStream);
        List<SchemaElement> schema = fileMetaData.getSchema();
        validateParquet(!schema.isEmpty(), "Empty Parquet schema in file: %s", dataSource.getId());

        MessageType messageType = readParquetSchema(schema);
        List<BlockMetaData> blocks = new ArrayList<>();
        List<RowGroup> rowGroups = fileMetaData.getRow_groups();
        if (rowGroups != null) {
            for (RowGroup rowGroup : rowGroups) {
                BlockMetaData blockMetaData = new BlockMetaData();
                blockMetaData.setRowCount(rowGroup.getNum_rows());
                blockMetaData.setTotalByteSize(rowGroup.getTotal_byte_size());
                List<ColumnChunk> columns = rowGroup.getColumns();
                validateParquet(!columns.isEmpty(), "No columns in row group: %s", rowGroup);
                String filePath = columns.get(0).getFile_path();
                for (ColumnChunk columnChunk : columns) {
                    validateParquet(
                            (filePath == null && columnChunk.getFile_path() == null)
                                    || (filePath != null && filePath.equals(columnChunk.getFile_path())),
                            "all column chunks of the same row group must be in the same file");
                    ColumnMetaData metaData = columnChunk.meta_data;
                    String[] path = metaData.path_in_schema.stream()
                            .map(value -> value.toLowerCase(Locale.ENGLISH))
                            .toArray(String[]::new);
                    ColumnPath columnPath = ColumnPath.get(path);
                    PrimitiveType primitiveType = messageType.getType(columnPath.toArray()).asPrimitiveType();
                    ColumnChunkMetaData column = ColumnChunkMetaData.get(
                            columnPath,
                            primitiveType,
                            CompressionCodecName.fromParquet(metaData.codec),
                            PARQUET_METADATA_CONVERTER.convertEncodingStats(metaData.encoding_stats),
                            readEncodings(metaData.encodings),
                            readStats(Optional.ofNullable(fileMetaData.getCreated_by()), Optional.ofNullable(metaData.statistics), primitiveType),
                            metaData.data_page_offset,
                            metaData.dictionary_page_offset,
                            metaData.num_values,
                            metaData.total_compressed_size,
                            metaData.total_uncompressed_size);
                    column.setColumnIndexReference(toColumnIndexReference(columnChunk));
                    column.setOffsetIndexReference(toOffsetIndexReference(columnChunk));
                    column.setBloomFilterOffset(metaData.bloom_filter_offset);
                    blockMetaData.addColumn(column);
                }
                blockMetaData.setPath(filePath);
                blocks.add(blockMetaData);
            }
        }

        Map<String, String> keyValueMetaData = new HashMap<>();
        List<KeyValue> keyValueList = fileMetaData.getKey_value_metadata();
        if (keyValueList != null) {
            for (KeyValue keyValue : keyValueList) {
                keyValueMetaData.put(keyValue.key, keyValue.value);
            }
        }
        org.apache.parquet.hadoop.metadata.FileMetaData parquetFileMetadata = new org.apache.parquet.hadoop.metadata.FileMetaData(
                messageType,
                keyValueMetaData,
                fileMetaData.getCreated_by());
        validateFileMetadata(dataSource.getId(), parquetFileMetadata, parquetWriteValidation);
        return new ParquetMetadata(parquetFileMetadata, blocks);
    }

    private static MessageType readParquetSchema(List<SchemaElement> schema)
    {
        Iterator<SchemaElement> schemaIterator = schema.iterator();
        SchemaElement rootSchema = schemaIterator.next();
        Types.MessageTypeBuilder builder = Types.buildMessage();
        readTypeSchema(builder, schemaIterator, rootSchema.getNum_children());
        return builder.named(rootSchema.name);
    }

    private static void readTypeSchema(Types.GroupBuilder<?> builder, Iterator<SchemaElement> schemaIterator, int typeCount)
    {
        ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
        for (int i = 0; i < typeCount; i++) {
            SchemaElement element = schemaIterator.next();
            Types.Builder<?, ?> typeBuilder;
            if (element.type == null) {
                typeBuilder = builder.group(Repetition.valueOf(element.repetition_type.name()));
                readTypeSchema((Types.GroupBuilder<?>) typeBuilder, schemaIterator, element.num_children);
            }
            else {
                Types.PrimitiveBuilder<?> primitiveBuilder = builder.primitive(getTypeName(element.type), Repetition.valueOf(element.repetition_type.name()));
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
                annotationFromLogicalType = getLogicalTypeAnnotation(parquetMetadataConverter, element.logicalType);
                typeBuilder.as(annotationFromLogicalType);
            }
            if (element.isSetConverted_type()) {
                LogicalTypeAnnotation annotationFromConvertedType = getLogicalTypeAnnotation(parquetMetadataConverter, element.converted_type, element);
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

    public static org.apache.parquet.column.statistics.Statistics<?> readStats(Optional<String> fileCreatedBy, Optional<Statistics> statisticsFromFile, PrimitiveType type)
    {
        Statistics statistics = statisticsFromFile.orElse(null);
        org.apache.parquet.column.statistics.Statistics<?> columnStatistics = new ParquetMetadataConverter().fromParquetStatistics(fileCreatedBy.orElse(null), statistics, type);

        if (isStringType(type)
                && statistics != null
                && !statistics.isSetMin_value() && !statistics.isSetMax_value() // the min,max fields used for UTF8 since Parquet PARQUET-1025
                && statistics.isSetMin() && statistics.isSetMax()  // the min,max fields used for UTF8 before Parquet PARQUET-1025
                && columnStatistics.genericGetMin() == null && columnStatistics.genericGetMax() == null
                && !CorruptStatistics.shouldIgnoreStatistics(fileCreatedBy.orElse(null), type.getPrimitiveTypeName())) {
            tryReadOldUtf8Stats(statistics, (BinaryStatistics) columnStatistics);
        }

        return columnStatistics;
    }

    private static boolean isStringType(PrimitiveType type)
    {
        if (type.getLogicalTypeAnnotation() == null) {
            return false;
        }

        return type.getLogicalTypeAnnotation()
                .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Boolean>()
                {
                    @Override
                    public Optional<Boolean> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType)
                    {
                        return Optional.of(TRUE);
                    }
                })
                .orElse(FALSE);
    }

    private static void tryReadOldUtf8Stats(Statistics statistics, BinaryStatistics columnStatistics)
    {
        byte[] min = statistics.getMin();
        byte[] max = statistics.getMax();

        if (Arrays.equals(min, max)) {
            // If min=max, then there is single value only
            min = min.clone();
            max = min;
        }
        else {
            int commonPrefix = commonPrefix(min, max);

            // For min we can retain all-ASCII, because this produces a strictly lower value.
            int minGoodLength = commonPrefix;
            while (minGoodLength < min.length && isAscii(min[minGoodLength])) {
                minGoodLength++;
            }

            // For max we can be sure only of the part matching the min. When they differ, we can consider only one next, and only if both are ASCII
            int maxGoodLength = commonPrefix;
            if (maxGoodLength < max.length && maxGoodLength < min.length && isAscii(min[maxGoodLength]) && isAscii(max[maxGoodLength])) {
                maxGoodLength++;
            }
            // Incrementing 127 would overflow. Incrementing within non-ASCII can have side-effects.
            while (maxGoodLength > 0 && (max[maxGoodLength - 1] == 127 || !isAscii(max[maxGoodLength - 1]))) {
                maxGoodLength--;
            }
            if (maxGoodLength == 0) {
                // We can return just min bound, but code downstream likely expects both are present or both are absent.
                return;
            }

            min = Arrays.copyOf(min, minGoodLength);
            max = Arrays.copyOf(max, maxGoodLength);
            max[maxGoodLength - 1]++;
        }

        columnStatistics.setMinMaxFromBytes(min, max);
        if (!columnStatistics.isNumNullsSet() && statistics.isSetNull_count()) {
            columnStatistics.setNumNulls(statistics.getNull_count());
        }
    }

    private static boolean isAscii(byte b)
    {
        return 0 <= b;
    }

    private static int commonPrefix(byte[] a, byte[] b)
    {
        int commonPrefixLength = 0;
        while (commonPrefixLength < a.length && commonPrefixLength < b.length && a[commonPrefixLength] == b[commonPrefixLength]) {
            commonPrefixLength++;
        }
        return commonPrefixLength;
    }

    private static Set<org.apache.parquet.column.Encoding> readEncodings(List<Encoding> encodings)
    {
        Set<org.apache.parquet.column.Encoding> columnEncodings = new HashSet<>();
        for (Encoding encoding : encodings) {
            columnEncodings.add(org.apache.parquet.column.Encoding.valueOf(encoding.name()));
        }
        return Collections.unmodifiableSet(columnEncodings);
    }

    private static PrimitiveTypeName getTypeName(Type type)
    {
        switch (type) {
            case BYTE_ARRAY:
                return PrimitiveTypeName.BINARY;
            case INT64:
                return PrimitiveTypeName.INT64;
            case INT32:
                return PrimitiveTypeName.INT32;
            case BOOLEAN:
                return PrimitiveTypeName.BOOLEAN;
            case FLOAT:
                return PrimitiveTypeName.FLOAT;
            case DOUBLE:
                return PrimitiveTypeName.DOUBLE;
            case INT96:
                return PrimitiveTypeName.INT96;
            case FIXED_LEN_BYTE_ARRAY:
                return PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        }
        throw new IllegalArgumentException("Unknown type " + type);
    }

    private static IndexReference toColumnIndexReference(ColumnChunk columnChunk)
    {
        if (columnChunk.isSetColumn_index_offset() && columnChunk.isSetColumn_index_length()) {
            return new IndexReference(columnChunk.getColumn_index_offset(), columnChunk.getColumn_index_length());
        }
        return null;
    }

    private static IndexReference toOffsetIndexReference(ColumnChunk columnChunk)
    {
        if (columnChunk.isSetOffset_index_offset() && columnChunk.isSetOffset_index_length()) {
            return new IndexReference(columnChunk.getOffset_index_offset(), columnChunk.getOffset_index_length());
        }
        return null;
    }

    private static void validateFileMetadata(ParquetDataSourceId dataSourceId, org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData, Optional<ParquetWriteValidation> parquetWriteValidation)
            throws ParquetCorruptionException
    {
        if (parquetWriteValidation.isEmpty()) {
            return;
        }
        ParquetWriteValidation writeValidation = parquetWriteValidation.get();
        writeValidation.validateTimeZone(
                dataSourceId,
                Optional.ofNullable(fileMetaData.getKeyValueMetaData().get(WRITER_TIMEZONE)));
        writeValidation.validateColumns(dataSourceId, fileMetaData.getSchema());
    }
}
