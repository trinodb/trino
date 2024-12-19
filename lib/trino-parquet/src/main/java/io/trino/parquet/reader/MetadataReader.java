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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetWriteValidation;
import io.trino.parquet.crypto.AesCipher;
import io.trino.parquet.crypto.AesGcmEncryptor;
import io.trino.parquet.crypto.HiddenColumnChunkMetaData;
import io.trino.parquet.crypto.InternalColumnDecryptionSetup;
import io.trino.parquet.crypto.InternalFileDecryptor;
import io.trino.parquet.crypto.KeyAccessDeniedException;
import io.trino.parquet.crypto.ModuleCipherFactory.ModuleType;
import io.trino.parquet.crypto.ParquetCryptoRuntimeException;
import io.trino.parquet.crypto.TagVerificationException;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.format.BlockCipher.Decryptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.EncryptionWithColumnKey;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import java.io.ByteArrayInputStream;
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
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetMetadataConverter.convertEncodingStats;
import static io.trino.parquet.ParquetMetadataConverter.fromParquetStatistics;
import static io.trino.parquet.ParquetMetadataConverter.getEncoding;
import static io.trino.parquet.ParquetMetadataConverter.getLogicalTypeAnnotation;
import static io.trino.parquet.ParquetMetadataConverter.getPrimitive;
import static io.trino.parquet.ParquetMetadataConverter.toColumnIndexReference;
import static io.trino.parquet.ParquetMetadataConverter.toOffsetIndexReference;
import static io.trino.parquet.ParquetValidationUtils.validateParquet;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.apache.parquet.format.Util.readFileCryptoMetaData;
import static org.apache.parquet.format.Util.readFileMetaData;

public final class MetadataReader
{
    private static final Logger log = Logger.get(MetadataReader.class);

    private static final Slice MAGIC = Slices.utf8Slice("PAR1");
    private static final Slice EMAGIC = Slices.utf8Slice("PARE");
    private static final int POST_SCRIPT_SIZE = Integer.BYTES + MAGIC.length();
    // Typical 1GB files produced by Trino were found to have footer size between 30-40KB
    private static final int EXPECTED_FOOTER_SIZE = 48 * 1024;

    private MetadataReader() {}

    public static ParquetMetadata readFooter(ParquetDataSource dataSource, Optional<ParquetWriteValidation> parquetWriteValidation, Optional<InternalFileDecryptor> fileDecryptor)
            throws IOException
    {
        // Parquet File Layout:
        //
        // MAGIC
        // variable: Data
        // variable: Metadata
        // 4 bytes: MetadataLength
        // MAGIC

        validateParquet((dataSource.getEstimatedSize() >= MAGIC.length() + POST_SCRIPT_SIZE) ||
                (dataSource.getEstimatedSize() >= EMAGIC.length() + POST_SCRIPT_SIZE), dataSource.getId(),
                "%s is not a valid Parquet File", dataSource.getId());

        // Read the tail of the file
        long estimatedFileSize = dataSource.getEstimatedSize();
        long expectedReadSize = min(estimatedFileSize, EXPECTED_FOOTER_SIZE);
        Slice buffer = dataSource.readTail(toIntExact(expectedReadSize));

        Slice magic = buffer.slice(buffer.length() - MAGIC.length(), MAGIC.length());
        validateParquet(MAGIC.equals(magic) || EMAGIC.equals(magic), dataSource.getId(), "Expected magic number: %s or %s got: %s", MAGIC.toStringUtf8(), EMAGIC.toStringUtf8(), magic.toStringUtf8());

        boolean encryptedFooterMode = EMAGIC.equals(magic);
        checkArgument(!encryptedFooterMode || !(fileDecryptor.isEmpty() || fileDecryptor.get().getDecryptionProperties() == null), "fileDecryptionProperties cannot be null when encryptedFooterMode is true");
        int metadataLength = buffer.getInt(buffer.length() - POST_SCRIPT_SIZE);
        long metadataIndex = estimatedFileSize - POST_SCRIPT_SIZE - metadataLength;
        validateParquet(
                metadataIndex >= MAGIC.length() && metadataIndex < estimatedFileSize - POST_SCRIPT_SIZE,
                dataSource.getId(),
                "Metadata index: %s out of range",
                metadataIndex);

        int completeFooterSize = metadataLength + POST_SCRIPT_SIZE;
        if (completeFooterSize > buffer.length()) {
            // initial read was not large enough, so just read again with the correct size
            buffer = dataSource.readTail(completeFooterSize);
        }
        InputStream metadataStream = buffer.slice(buffer.length() - completeFooterSize, metadataLength).getInput();

        Decryptor footerDecryptor = null;
        byte[] aad = null;

        if (encryptedFooterMode) {
            FileCryptoMetaData fileCryptoMetaData = readFileCryptoMetaData(metadataStream);
            fileDecryptor.get().setFileCryptoMetaData(fileCryptoMetaData.getEncryption_algorithm(), true, fileCryptoMetaData.getKey_metadata());
            footerDecryptor = fileDecryptor.get().fetchFooterDecryptor();
            aad = AesCipher.createFooterAAD(fileDecryptor.get().getFileAAD());
        }
        FileMetaData fileMetaData = readFileMetaData(metadataStream, footerDecryptor, aad);
        if (!encryptedFooterMode && fileDecryptor.isPresent()) {
            if (!fileMetaData.isSetEncryption_algorithm()) { // Plaintext file
                fileDecryptor.get().setPlaintextFile();
                // Done to detect files that were not encrypted by mistake
                if (!fileDecryptor.get().plaintextFilesAllowed()) {
                    throw new ParquetCryptoRuntimeException("Applying decryptor on plaintext file");
                }
            }
            else {  // Encrypted file with plaintext footer
                // if no fileDecryptor, can still read plaintext columns
                fileDecryptor.get().setFileCryptoMetaData(fileMetaData.getEncryption_algorithm(), false,
                        fileMetaData.getFooter_signing_key_metadata());
                if (fileDecryptor.get().checkFooterIntegrity()) {
                    verifyFooterIntegrity(metadataStream, fileDecryptor.get(), metadataLength);
                }
            }
        }
        ParquetDataSourceId id = dataSource.getId();
        ParquetMetadata parquetMetadata = createParquetMetadata(fileMetaData, id, fileDecryptor, encryptedFooterMode);
        validateFileMetadata(id, parquetMetadata.getFileMetaData(), parquetWriteValidation);
        validateFileMetadata(dataSource.getId(), parquetMetadata.getFileMetaData(), parquetWriteValidation);
        return parquetMetadata;
    }

    public static ParquetMetadata createParquetMetadata(FileMetaData fileMetaData,
                                                        ParquetDataSourceId dataSourceId,
                                                        Optional<InternalFileDecryptor> fileDecryptor,
                                                        boolean encryptedFooterMode)
            throws ParquetCorruptionException
    {
        List<SchemaElement> schema = fileMetaData.getSchema();
        validateParquet(!schema.isEmpty(), dataSourceId, "Schema is empty");

        MessageType messageType = readParquetSchema(schema);
        List<BlockMetadata> blocks = new ArrayList<>();
        List<RowGroup> rowGroups = fileMetaData.getRow_groups();
        if (rowGroups != null) {
            for (RowGroup rowGroup : rowGroups) {
                List<ColumnChunk> columns = rowGroup.getColumns();
                validateParquet(!columns.isEmpty(), dataSourceId, "No columns in row group: %s", rowGroup);
                String filePath = columns.get(0).getFile_path();
                int columnOrdinal = -1;
                ImmutableList.Builder<ColumnChunkMetadata> columnMetadataBuilder = ImmutableList.builderWithExpectedSize(columns.size());
                for (ColumnChunk columnChunk : columns) {
                    columnOrdinal++;
                    validateParquet(
                            (filePath == null && columnChunk.getFile_path() == null)
                                    || (filePath != null && filePath.equals(columnChunk.getFile_path())),
                            dataSourceId,
                            "all column chunks of the same row group must be in the same file");
                    ColumnCryptoMetaData cryptoMetaData = columnChunk.getCrypto_metadata();
                    ColumnMetaData metaData = columnChunk.meta_data;
                    ColumnPath columnPath = null;
                    boolean encryptedMetadata = false;
                    if (cryptoMetaData == null) {
                        columnPath = getPath(metaData);
                        if (fileDecryptor.isPresent() && !fileDecryptor.get().plaintextFile()) {
                            // mark this column as plaintext in encrypted file decryptor
                            fileDecryptor.get().setColumnCryptoMetadata(columnPath, false, false, (byte[]) null, columnOrdinal);
                        }
                    }
                    else { // Encrypted column
                        if (cryptoMetaData.isSetENCRYPTION_WITH_FOOTER_KEY()) { // Column encrypted with footer key
                            if (!encryptedFooterMode) {
                                throw new ParquetCryptoRuntimeException("Column encrypted with footer key in file with plaintext footer");
                            }
                            if (null == metaData) {
                                throw new ParquetCryptoRuntimeException("ColumnMetaData not set in Encryption with Footer key");
                            }
                            if (fileDecryptor.isEmpty()) {
                                throw new ParquetCryptoRuntimeException("Column encrypted with footer key: No keys available");
                            }
                            columnPath = getPath(metaData);
                            fileDecryptor.get().setColumnCryptoMetadata(columnPath, true, true, (byte[]) null, columnOrdinal);
                        }
                        else { // Column encrypted with column key
                            encryptedMetadata = true;
                        }
                    }
                    try {
                        if (encryptedMetadata) {
                            // TODO: We decrypted data before filter projection. This could send unnecessary traffic to KMS.
                            //  In parquet-mr, it uses lazy decyrption but that required to change ColumnChunkMetadata. We will improve it alter.
                            metaData = decryptMetadata(rowGroup, cryptoMetaData, columnChunk, fileDecryptor.get(), columnOrdinal);
                            columnPath = getPath(metaData);
                        }
                        PrimitiveType primitiveType = messageType.getType(columnPath.toArray()).asPrimitiveType();
                        ColumnChunkMetadata column = ColumnChunkMetadata.get(
                                columnPath,
                                primitiveType,
                                CompressionCodecName.fromParquet(metaData.codec),
                                convertEncodingStats(metaData.encoding_stats),
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

                        if (rowGroup.isSetOrdinal()) {
                            column.setRowGroupOrdinal(rowGroup.getOrdinal());
                        }
                        columnMetadataBuilder.add(column);
                    }
                    catch (KeyAccessDeniedException e) {
                        ColumnChunkMetadata column = new HiddenColumnChunkMetaData(columnPath, filePath);
                        columnMetadataBuilder.add(column);
                    }
                }
                blocks.add(new BlockMetadata(rowGroup.getNum_rows(), rowGroup.getTotal_byte_size(), rowGroup.getOrdinal(), columnMetadataBuilder.build()));
            }
        }

        Map<String, String> keyValueMetaData = new HashMap<>();
        List<KeyValue> keyValueList = fileMetaData.getKey_value_metadata();
        if (keyValueList != null) {
            for (KeyValue keyValue : keyValueList) {
                keyValueMetaData.put(keyValue.key, keyValue.value);
            }
        }
        FileMetadata parquetFileMetadata = new FileMetadata(
                messageType,
                keyValueMetaData,
                fileMetaData.getCreated_by());
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
        for (int i = 0; i < typeCount; i++) {
            SchemaElement element = schemaIterator.next();
            Types.Builder<?, ?> typeBuilder;
            if (element.type == null) {
                typeBuilder = builder.group(Repetition.valueOf(element.repetition_type.name()));
                readTypeSchema((Types.GroupBuilder<?>) typeBuilder, schemaIterator, element.num_children);
            }
            else {
                Types.PrimitiveBuilder<?> primitiveBuilder = builder.primitive(getPrimitive(element.type), Repetition.valueOf(element.repetition_type.name()));
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

    public static org.apache.parquet.column.statistics.Statistics<?> readStats(Optional<String> fileCreatedBy, Optional<Statistics> statisticsFromFile, PrimitiveType type)
    {
        Statistics statistics = statisticsFromFile.orElse(null);
        org.apache.parquet.column.statistics.Statistics<?> columnStatistics = fromParquetStatistics(fileCreatedBy.orElse(null), statistics, type);

        if (isStringType(type)
                && statistics != null
                && !statistics.isSetMin_value() && !statistics.isSetMax_value() // the min,max fields used for UTF8 since Parquet PARQUET-1025
                && statistics.isSetMin() && statistics.isSetMax()  // the min,max fields used for UTF8 before Parquet PARQUET-1025
                && columnStatistics.genericGetMin() == null && columnStatistics.genericGetMax() == null
                && !CorruptStatistics.shouldIgnoreStatistics(fileCreatedBy.orElse(null), type.getPrimitiveTypeName())) {
            columnStatistics = tryReadOldUtf8Stats(statistics, (BinaryStatistics) columnStatistics);
        }

        return columnStatistics;
    }

    /**
     * If a column is encrypted and user doesn't provide correct key to decrypt, that column is hidden to current request.
     * This method find out the first non-hidden column.
     *
     * @param block BlockMetaData
     * @return first non hidden column id.
     */
    public static Integer findFirstNonHiddenColumnId(BlockMetadata block)
    {
        List<ColumnChunkMetadata> columns = block.columns();
        for (int i = 0; i < columns.size(); i++) {
            if (!HiddenColumnChunkMetaData.isHiddenColumn(columns.get(i))) {
                return i;
            }
        }
        // all columns are hidden (encrypted but not accessible to current user)
        return null;
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

    private static org.apache.parquet.column.statistics.Statistics<?> tryReadOldUtf8Stats(Statistics statistics, BinaryStatistics columnStatistics)
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
                return columnStatistics;
            }

            min = Arrays.copyOf(min, minGoodLength);
            max = Arrays.copyOf(max, maxGoodLength);
            max[maxGoodLength - 1]++;
        }

        return org.apache.parquet.column.statistics.Statistics
                .getBuilderForReading(columnStatistics.type())
                       .withMin(min)
                       .withMax(max)
                       .withNumNulls(!columnStatistics.isNumNullsSet() && statistics.isSetNull_count() ? statistics.getNull_count() : columnStatistics.getNumNulls())
                       .build();
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
            columnEncodings.add(getEncoding(encoding));
        }
        return Collections.unmodifiableSet(columnEncodings);
    }

    private static void validateFileMetadata(ParquetDataSourceId dataSourceId, FileMetadata fileMetaData, Optional<ParquetWriteValidation> parquetWriteValidation)
            throws ParquetCorruptionException
    {
        if (parquetWriteValidation.isEmpty()) {
            return;
        }
        ParquetWriteValidation writeValidation = parquetWriteValidation.get();
        writeValidation.validateTimeZone(
                dataSourceId,
                Optional.ofNullable(fileMetaData.getKeyValueMetaData().get("writer.time.zone")));
        writeValidation.validateColumns(dataSourceId, fileMetaData.getSchema());
    }

    private static ColumnMetaData decryptMetadata(RowGroup rowGroup, ColumnCryptoMetaData cryptoMetaData, ColumnChunk columnChunk, InternalFileDecryptor fileDecryptor, int columnOrdinal)
    {
        EncryptionWithColumnKey columnKeyStruct = cryptoMetaData.getENCRYPTION_WITH_COLUMN_KEY();
        List<String> pathList = columnKeyStruct.getPath_in_schema().stream()
                .map(value -> value.toLowerCase(Locale.ENGLISH)).collect(Collectors.toList());

        byte[] columnKeyMetadata = columnKeyStruct.getKey_metadata();
        ColumnPath columnPath = ColumnPath.get(pathList.toArray(new String[pathList.size()]));
        byte[] encryptedMetadataBuffer = columnChunk.getEncrypted_column_metadata();

        // Decrypt the ColumnMetaData
        InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.setColumnCryptoMetadata(columnPath, true, false, columnKeyMetadata, columnOrdinal);
        ByteArrayInputStream tempInputStream = new ByteArrayInputStream(encryptedMetadataBuffer);
        byte[] columnMetaDataAAD = AesCipher.createModuleAAD(fileDecryptor.getFileAAD(), ModuleType.ColumnMetaData, rowGroup.ordinal, columnOrdinal, -1);
        try {
            return Util.readColumnMetaData(tempInputStream, columnDecryptionSetup.getMetaDataDecryptor(), columnMetaDataAAD);
        }
        catch (IOException e) {
            throw new ParquetCryptoRuntimeException(columnPath + ". Failed to decrypt column metadata", e);
        }
    }

    /*public static ColumnChunkMetadata buildColumnChunkMetaData(Optional<String> fileCreatedBy, ColumnMetaData metaData, ColumnPath columnPath, PrimitiveType type)
    {
        return ColumnChunkMetadata.get(
                columnPath,
                type,
                CompressionCodecName.fromParquet(metaData.codec),
                PARQUET_METADATA_CONVERTER.convertEncodingStats(metaData.encoding_stats),
                readEncodings(metaData.encodings),
                readStats(fileCreatedBy, Optional.ofNullable(metaData.statistics), type),
                metaData.data_page_offset,
                metaData.dictionary_page_offset,
                metaData.num_values,
                metaData.total_compressed_size,
                metaData.total_uncompressed_size);
    }*/

    private static ColumnPath getPath(ColumnMetaData metaData)
    {
        String[] path = metaData.path_in_schema.stream()
                .map(value -> value.toLowerCase(Locale.ENGLISH))
                .toArray(String[]::new);
        return ColumnPath.get(path);
    }

    private static void verifyFooterIntegrity(InputStream metadataStream, InternalFileDecryptor fileDecryptor, int combinedFooterLength)
            throws IOException
    {
        byte[] nonce = new byte[AesCipher.NONCE_LENGTH];
        metadataStream.read(nonce);
        byte[] gcmTag = new byte[AesCipher.GCM_TAG_LENGTH];
        metadataStream.read(gcmTag);

        AesGcmEncryptor footerSigner = fileDecryptor.createSignedFooterEncryptor();
        int footerSignatureLength = AesCipher.NONCE_LENGTH + AesCipher.GCM_TAG_LENGTH;
        byte[] serializedFooter = new byte[combinedFooterLength - footerSignatureLength];

        //InputStream doesn't implement reset(). Here is to workaround
        ((BasicSliceInput) metadataStream).setPosition(0);
        metadataStream.read(serializedFooter, 0, serializedFooter.length);

        byte[] signedFooterAAD = AesCipher.createFooterAAD(fileDecryptor.getFileAAD());
        byte[] encryptedFooterBytes = footerSigner.encrypt(false, serializedFooter, nonce, signedFooterAAD);
        byte[] calculatedTag = new byte[AesCipher.GCM_TAG_LENGTH];
        System.arraycopy(encryptedFooterBytes, encryptedFooterBytes.length - AesCipher.GCM_TAG_LENGTH, calculatedTag, 0, AesCipher.GCM_TAG_LENGTH);
        if (!Arrays.equals(gcmTag, calculatedTag)) {
            throw new TagVerificationException("Signature mismatch in plaintext footer");
        }
    }
}
