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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.crypto.AesCipherUtils;
import io.trino.parquet.crypto.ColumnDecryptionContext;
import io.trino.parquet.crypto.FileDecryptionContext;
import io.trino.parquet.crypto.ModuleType;
import io.trino.parquet.reader.MetadataReader;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.EncryptionWithColumnKey;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.parquet.ParquetMetadataConverter.convertEncodingStats;
import static io.trino.parquet.ParquetMetadataConverter.getEncoding;
import static io.trino.parquet.ParquetMetadataConverter.getLogicalTypeAnnotation;
import static io.trino.parquet.ParquetMetadataConverter.getPrimitive;
import static io.trino.parquet.ParquetMetadataConverter.toColumnIndexReference;
import static io.trino.parquet.ParquetMetadataConverter.toOffsetIndexReference;
import static io.trino.parquet.ParquetValidationUtils.validateParquet;
import static io.trino.parquet.ParquetValidationUtils.validateParquetCrypto;
import static java.util.Objects.requireNonNull;

public class ParquetMetadata
{
    private static final Logger log = Logger.get(ParquetMetadata.class);

    private final FileMetaData parquetMetadata;
    private final ParquetDataSourceId dataSourceId;
    private final FileMetadata fileMetadata;
    private final Optional<FileDecryptionContext> decryptionContext;

    public ParquetMetadata(FileMetaData parquetMetadata, ParquetDataSourceId dataSourceId, Optional<FileDecryptionContext> decryptionContext)
            throws ParquetCorruptionException
    {
        this.fileMetadata = new FileMetadata(
                readMessageType(parquetMetadata, dataSourceId),
                keyValueMetaData(parquetMetadata),
                parquetMetadata.getCreated_by());
        this.parquetMetadata = parquetMetadata;
        this.dataSourceId = requireNonNull(dataSourceId, "dataSourceId is null");
        this.decryptionContext = requireNonNull(decryptionContext, "decryptionContext is null");
    }

    public FileMetadata getFileMetaData()
    {
        return fileMetadata;
    }

    public Optional<FileDecryptionContext> getDecryptionContext()
    {
        return decryptionContext;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("parquetMetadata", parquetMetadata)
                .toString();
    }

    public List<BlockMetadata> getBlocks()
            throws IOException
    {
        return getBlocks(0, Long.MAX_VALUE);
    }

    public List<BlockMetadata> getBlocks(long splitStart, long splitLength)
            throws IOException
    {
        List<SchemaElement> schema = parquetMetadata.getSchema();
        validateParquet(!schema.isEmpty(), dataSourceId, "Schema is empty");

        MessageType messageType = readParquetSchema(schema);
        List<BlockMetadata> blocks = new ArrayList<>();
        List<RowGroup> rowGroups = parquetMetadata.getRow_groups();

        long fileRowCount = 0;

        if (rowGroups != null) {
            for (RowGroup rowGroup : rowGroups) {
                long fileRowCountOffset = fileRowCount;
                fileRowCount += rowGroup.getNum_rows(); // Update fileRowCount for all row groups

                List<ColumnChunk> columns = rowGroup.getColumns();
                validateParquet(!columns.isEmpty(), dataSourceId, "No columns in row group: %s", rowGroup);
                String filePath = columns.get(0).getFile_path();

                ImmutableList.Builder<ColumnChunkMetadata> columnMetadataBuilder = ImmutableList.builderWithExpectedSize(columns.size());
                int columnOrdinal = -1;
                boolean splitContainsRowGroup = true;
                for (ColumnChunk columnChunk : columns) {
                    columnOrdinal++;
                    validateParquet(
                            (filePath == null && columnChunk.getFile_path() == null)
                                    || (filePath != null && filePath.equals(columnChunk.getFile_path())),
                            dataSourceId,
                            "all column chunks of the same row group must be in the same file");
                    ColumnCryptoMetaData cryptoMetaData = columnChunk.getCrypto_metadata();
                    ColumnMetaData metaData;
                    ColumnPath columnPath;
                    if (cryptoMetaData == null) {
                        // Plaintext column
                        metaData = columnChunk.getMeta_data();
                        columnPath = getPath(metaData.getPath_in_schema());
                        decryptionContext.ifPresent(context -> context.initPlaintextColumn(columnPath));
                    }
                    else {
                        validateParquetCrypto(decryptionContext.isPresent(), dataSourceId, "Column is encrypted, but no decryption context");
                        if (cryptoMetaData.isSetENCRYPTION_WITH_FOOTER_KEY()) {
                            // Column encrypted with footer key
                            validateParquetCrypto(columnChunk.getMeta_data() != null, dataSourceId, "Column metadata is null");
                            metaData = columnChunk.getMeta_data();
                            columnPath = getPath(metaData.getPath_in_schema());
                            decryptionContext.get().initializeColumnCryptoMetadata(columnPath, true, Optional.empty());
                        }
                        else {
                            // Column encrypted with column key
                            EncryptionWithColumnKey columnKeyStruct = cryptoMetaData.getENCRYPTION_WITH_COLUMN_KEY();
                            columnPath = getPath(columnKeyStruct.getPath_in_schema());
                            Optional<ColumnMetaData> decryptedMetadata = decryptColumnMetadata(columnPath, rowGroup, columnKeyStruct.getKey_metadata(), columnChunk, decryptionContext.get(), columnOrdinal);
                            if (decryptedMetadata.isEmpty()) {
                                // User does not have access to the column key. Column is considered hidden.
                                columnMetadataBuilder.add(new HiddenColumnChunkMetadata(dataSourceId, columnPath));
                                validateParquetCrypto(columnOrdinal != 0, dataSourceId, "First column of a row group is encrypted with an unknown column key. Cannot determine row group starting position.");
                                continue;
                            }
                            metaData = decryptedMetadata.get();
                        }
                    }

                    PrimitiveType primitiveType = messageType.getType(columnPath.toArray()).asPrimitiveType();
                    ColumnChunkMetadata column = ColumnChunkMetadata.get(
                            columnPath,
                            primitiveType,
                            CompressionCodecName.fromParquet(metaData.codec),
                            convertEncodingStats(metaData.encoding_stats),
                            readEncodings(metaData.encodings),
                            MetadataReader.readStats(Optional.ofNullable(parquetMetadata.getCreated_by()), Optional.ofNullable(metaData.statistics), primitiveType),
                            metaData.data_page_offset,
                            metaData.dictionary_page_offset,
                            metaData.num_values,
                            metaData.total_compressed_size,
                            metaData.total_uncompressed_size);
                    column.setColumnIndexReference(toColumnIndexReference(columnChunk));
                    column.setOffsetIndexReference(toOffsetIndexReference(columnChunk));
                    column.setBloomFilterOffset(metaData.bloom_filter_offset);
                    if (rowGroup.isSetOrdinal()) {
                        column.setRowGroupOrdinal(rowGroup.getOrdinal());
                    }
                    column.setColumnOrdinal(columnOrdinal);
                    columnMetadataBuilder.add(column);

                    // Skip row group if it doesn't overlap the split. Only first column starting position matches row group start and can be used for the check.
                    long rowGroupStart = getRowGroupStart(column);
                    splitContainsRowGroup = columnOrdinal != 0 || (splitStart <= rowGroupStart && rowGroupStart < splitStart + splitLength);
                    if (!splitContainsRowGroup) {
                        break;
                    }
                }
                if (!splitContainsRowGroup) {
                    continue;
                }
                blocks.add(new BlockMetadata(fileRowCountOffset, rowGroup.getNum_rows(), columnMetadataBuilder.build()));
            }
        }

        return blocks;
    }

    @VisibleForTesting
    public FileMetaData getParquetMetadata()
    {
        return parquetMetadata;
    }

    private static long getRowGroupStart(ColumnChunkMetadata column)
    {
        // Note: Do not rely on org.apache.parquet.format.RowGroup.getFile_offset or org.apache.parquet.format.ColumnChunk.getFile_offset
        // because some versions of parquet-cpp-arrow (and potentially other writers) set it incorrectly
        return column.getStartingPos();
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

    private static Optional<ColumnMetaData> decryptColumnMetadata(ColumnPath columnPath, RowGroup rowGroup, byte[] columnKeyMetadata, ColumnChunk columnChunk, FileDecryptionContext decryptionContext, int columnOrdinal)
            throws IOException
    {
        byte[] encryptedMetadataBuffer = columnChunk.getEncrypted_column_metadata();

        // Decrypt the ColumnMetaData
        Optional<ColumnDecryptionContext> columnDecryptionContext = decryptionContext.initializeColumnCryptoMetadata(columnPath, false, Optional.ofNullable(columnKeyMetadata));
        if (columnDecryptionContext.isEmpty()) {
            return Optional.empty();
        }

        ByteArrayInputStream tempInputStream = new ByteArrayInputStream(encryptedMetadataBuffer);
        byte[] columnMetaDataAAD = AesCipherUtils.createModuleAAD(decryptionContext.getFileAad(), ModuleType.ColumnMetaData, rowGroup.ordinal, columnOrdinal, -1);
        return Optional.of(Util.readColumnMetaData(tempInputStream, columnDecryptionContext.get().metadataDecryptor(), columnMetaDataAAD));
    }

    private static ColumnPath getPath(List<String> pathInSchema)
    {
        requireNonNull(pathInSchema, "pathInSchema is null");
        String[] path = pathInSchema.stream()
                .map(value -> value.toLowerCase(Locale.ENGLISH))
                .toArray(String[]::new);
        return ColumnPath.get(path);
    }

    private static Set<Encoding> readEncodings(List<org.apache.parquet.format.Encoding> encodings)
    {
        Set<org.apache.parquet.column.Encoding> columnEncodings = new HashSet<>();
        for (org.apache.parquet.format.Encoding encoding : encodings) {
            columnEncodings.add(getEncoding(encoding));
        }
        return Collections.unmodifiableSet(columnEncodings);
    }

    private static MessageType readMessageType(FileMetaData parquetMetadata, ParquetDataSourceId dataSourceId)
            throws ParquetCorruptionException
    {
        List<SchemaElement> schema = parquetMetadata.getSchema();
        validateParquet(!schema.isEmpty(), dataSourceId, "Schema is empty");

        Iterator<SchemaElement> schemaIterator = schema.iterator();
        SchemaElement rootSchema = schemaIterator.next();
        Types.MessageTypeBuilder builder = Types.buildMessage();
        readTypeSchema(builder, schemaIterator, rootSchema.getNum_children());
        return builder.named(rootSchema.name);
    }

    private static Map<String, String> keyValueMetaData(FileMetaData parquetMetadata)
    {
        if (parquetMetadata.getKey_value_metadata() == null) {
            return ImmutableMap.of();
        }
        return parquetMetadata.getKey_value_metadata()
                .stream()
                .collect(toImmutableMap(KeyValue::getKey, KeyValue::getValue, (_, second) -> second));
    }
}
