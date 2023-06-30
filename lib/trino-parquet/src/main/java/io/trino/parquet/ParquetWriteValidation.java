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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.parquet.ColumnStatisticsValidation.ColumnStatistics;
import static io.trino.parquet.ParquetValidationUtils.validateParquet;
import static io.trino.parquet.ParquetWriteValidation.IndexReferenceValidation.fromIndexReference;
import static java.util.Objects.requireNonNull;

public class ParquetWriteValidation
{
    private static final ParquetMetadataConverter METADATA_CONVERTER = new ParquetMetadataConverter();

    private final String createdBy;
    private final Optional<String> timeZoneId;
    private final List<ColumnDescriptor> columns;
    private final List<RowGroup> rowGroups;
    private final WriteChecksum checksum;
    private final List<Type> types;
    private final List<String> columnNames;

    private ParquetWriteValidation(
            String createdBy,
            Optional<String> timeZoneId,
            List<ColumnDescriptor> columns,
            List<RowGroup> rowGroups,
            WriteChecksum checksum,
            List<Type> types,
            List<String> columnNames)
    {
        this.createdBy = requireNonNull(createdBy, "createdBy is null");
        checkArgument(!createdBy.isEmpty(), "createdBy is empty");
        this.timeZoneId = requireNonNull(timeZoneId, "timeZoneId is null");
        this.columns = requireNonNull(columns, "columnPaths is null");
        this.rowGroups = requireNonNull(rowGroups, "rowGroups is null");
        this.checksum = requireNonNull(checksum, "checksum is null");
        this.types = requireNonNull(types, "types is null");
        this.columnNames = requireNonNull(columnNames, "columnNames is null");
    }

    public String getCreatedBy()
    {
        return createdBy;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    public void validateTimeZone(ParquetDataSourceId dataSourceId, Optional<String> actualTimeZoneId)
            throws ParquetCorruptionException
    {
        validateParquet(
                timeZoneId.equals(actualTimeZoneId),
                dataSourceId,
                "Found unexpected time zone %s, expected %s",
                actualTimeZoneId,
                timeZoneId);
    }

    public void validateColumns(ParquetDataSourceId dataSourceId, MessageType schema)
            throws ParquetCorruptionException
    {
        List<ColumnDescriptor> actualColumns = schema.getColumns();
        validateParquet(
                actualColumns.size() == columns.size(),
                dataSourceId,
                "Found columns %s, expected %s",
                actualColumns,
                columns);
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            validateColumnDescriptorsSame(actualColumns.get(columnIndex), columns.get(columnIndex), dataSourceId);
        }
    }

    public void validateBlocksMetadata(ParquetDataSourceId dataSourceId, List<BlockMetaData> blocksMetaData)
            throws ParquetCorruptionException
    {
        validateParquet(
                blocksMetaData.size() == rowGroups.size(),
                dataSourceId,
                "Number of row groups %d did not match %d",
                blocksMetaData.size(),
                rowGroups.size());
        for (int rowGroupIndex = 0; rowGroupIndex < blocksMetaData.size(); rowGroupIndex++) {
            BlockMetaData block = blocksMetaData.get(rowGroupIndex);
            RowGroup rowGroup = rowGroups.get(rowGroupIndex);
            validateParquet(
                    block.getRowCount() == rowGroup.getNum_rows(),
                    dataSourceId,
                    "Number of rows %d in row group %d did not match %d",
                    block.getRowCount(),
                    rowGroupIndex,
                    rowGroup.getNum_rows());

            List<ColumnChunkMetaData> columnChunkMetaData = block.getColumns();
            validateParquet(
                    columnChunkMetaData.size() == rowGroup.getColumnsSize(),
                    dataSourceId,
                    "Number of columns %d in row group %d did not match %d",
                    columnChunkMetaData.size(),
                    rowGroupIndex,
                    rowGroup.getColumnsSize());

            for (int columnIndex = 0; columnIndex < columnChunkMetaData.size(); columnIndex++) {
                ColumnChunkMetaData actualColumnMetadata = columnChunkMetaData.get(columnIndex);
                ColumnChunk columnChunk = rowGroup.getColumns().get(columnIndex);
                ColumnMetaData expectedColumnMetadata = columnChunk.getMeta_data();
                verifyColumnMetadataMatch(
                        actualColumnMetadata.getCodec().getParquetCompressionCodec().equals(expectedColumnMetadata.getCodec()),
                        "Compression codec",
                        actualColumnMetadata.getCodec(),
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedColumnMetadata.getCodec());

                verifyColumnMetadataMatch(
                        actualColumnMetadata.getPrimitiveType().getPrimitiveTypeName().equals(METADATA_CONVERTER.getPrimitive(expectedColumnMetadata.getType())),
                        "Type",
                        actualColumnMetadata.getPrimitiveType().getPrimitiveTypeName(),
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedColumnMetadata.getType());

                verifyColumnMetadataMatch(
                        areEncodingsSame(actualColumnMetadata.getEncodings(), expectedColumnMetadata.getEncodings()),
                        "Encodings",
                        actualColumnMetadata.getEncodings(),
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedColumnMetadata.getEncodings());

                verifyColumnMetadataMatch(
                        areStatisticsSame(actualColumnMetadata.getStatistics(), expectedColumnMetadata.getStatistics()),
                        "Statistics",
                        actualColumnMetadata.getStatistics(),
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedColumnMetadata.getStatistics());

                verifyColumnMetadataMatch(
                        actualColumnMetadata.getFirstDataPageOffset() == expectedColumnMetadata.getData_page_offset(),
                        "Data page offset",
                        actualColumnMetadata.getFirstDataPageOffset(),
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedColumnMetadata.getData_page_offset());

                verifyColumnMetadataMatch(
                        actualColumnMetadata.getDictionaryPageOffset() == expectedColumnMetadata.getDictionary_page_offset(),
                        "Dictionary page offset",
                        actualColumnMetadata.getDictionaryPageOffset(),
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedColumnMetadata.getDictionary_page_offset());

                verifyColumnMetadataMatch(
                        actualColumnMetadata.getValueCount() == expectedColumnMetadata.getNum_values(),
                        "Value count",
                        actualColumnMetadata.getValueCount(),
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedColumnMetadata.getNum_values());

                verifyColumnMetadataMatch(
                        actualColumnMetadata.getTotalUncompressedSize() == expectedColumnMetadata.getTotal_uncompressed_size(),
                        "Total uncompressed size",
                        actualColumnMetadata.getTotalUncompressedSize(),
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedColumnMetadata.getTotal_uncompressed_size());

                verifyColumnMetadataMatch(
                        actualColumnMetadata.getTotalSize() == expectedColumnMetadata.getTotal_compressed_size(),
                        "Total size",
                        actualColumnMetadata.getTotalSize(),
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedColumnMetadata.getTotal_compressed_size());

                IndexReferenceValidation expectedColumnIndexReference = new IndexReferenceValidation(columnChunk.getColumn_index_offset(), columnChunk.getColumn_index_length());
                IndexReference actualColumnIndexReference = actualColumnMetadata.getColumnIndexReference();
                verifyColumnMetadataMatch(
                        actualColumnIndexReference == null || fromIndexReference(actualColumnMetadata.getColumnIndexReference()).equals(expectedColumnIndexReference),
                        "Column index reference",
                        actualColumnIndexReference,
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedColumnIndexReference);

                IndexReferenceValidation expectedOffsetIndexReference = new IndexReferenceValidation(columnChunk.getOffset_index_offset(), columnChunk.getOffset_index_length());
                IndexReference actualOffsetIndexReference = actualColumnMetadata.getOffsetIndexReference();
                verifyColumnMetadataMatch(
                        actualOffsetIndexReference == null || fromIndexReference(actualOffsetIndexReference).equals(expectedOffsetIndexReference),
                        "Offset index reference",
                        actualOffsetIndexReference,
                        actualColumnMetadata.getPath(),
                        rowGroupIndex,
                        dataSourceId,
                        expectedOffsetIndexReference);
            }
        }
    }

    public void validateChecksum(ParquetDataSourceId dataSourceId, WriteChecksum actualChecksum)
            throws ParquetCorruptionException
    {
        validateParquet(
                checksum.totalRowCount() == actualChecksum.totalRowCount(),
                dataSourceId,
                "Write validation failed: Expected row count %d, found %d",
                checksum.totalRowCount(),
                actualChecksum.totalRowCount());

        List<Long> columnHashes = actualChecksum.columnHashes();
        for (int columnIndex = 0; columnIndex < columnHashes.size(); columnIndex++) {
            long expectedHash = checksum.columnHashes().get(columnIndex);
            validateParquet(
                    expectedHash == columnHashes.get(columnIndex),
                    dataSourceId,
                    "Invalid checksum for column %s: Expected hash %d, found %d",
                    columnIndex,
                    expectedHash,
                    columnHashes.get(columnIndex));
        }
    }

    public record WriteChecksum(long totalRowCount, List<Long> columnHashes)
    {
        public WriteChecksum(long totalRowCount, List<Long> columnHashes)
        {
            this.totalRowCount = totalRowCount;
            this.columnHashes = ImmutableList.copyOf(requireNonNull(columnHashes, "columnHashes is null"));
        }
    }

    public static class WriteChecksumBuilder
    {
        private final List<ValidationHash> validationHashes;
        private final List<XxHash64> columnHashes;
        private final byte[] longBuffer = new byte[Long.BYTES];
        private final Slice longSlice = Slices.wrappedBuffer(longBuffer);

        private long totalRowCount;

        private WriteChecksumBuilder(List<Type> types)
        {
            this.validationHashes = requireNonNull(types, "types is null").stream()
                    .map(ValidationHash::createValidationHash)
                    .collect(toImmutableList());

            ImmutableList.Builder<XxHash64> columnHashes = ImmutableList.builder();
            for (Type ignored : types) {
                columnHashes.add(new XxHash64());
            }
            this.columnHashes = columnHashes.build();
        }

        public static WriteChecksumBuilder createWriteChecksumBuilder(List<Type> readTypes)
        {
            return new WriteChecksumBuilder(readTypes);
        }

        public void addPage(Page page)
        {
            requireNonNull(page, "page is null");
            checkArgument(
                    page.getChannelCount() == columnHashes.size(),
                    "Invalid page: page channels count %s did not match columns count %s",
                    page.getChannelCount(),
                    columnHashes.size());

            for (int channel = 0; channel < columnHashes.size(); channel++) {
                ValidationHash validationHash = validationHashes.get(channel);
                Block block = page.getBlock(channel);
                XxHash64 xxHash64 = columnHashes.get(channel);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    long hash = validationHash.hash(block, position);
                    longSlice.setLong(0, hash);
                    xxHash64.update(longBuffer);
                }
            }
            totalRowCount += page.getPositionCount();
        }

        public WriteChecksum build()
        {
            return new WriteChecksum(
                    totalRowCount,
                    columnHashes.stream()
                            .map(XxHash64::hash)
                            .collect(toImmutableList()));
        }
    }

    public void validateRowGroupStatistics(ParquetDataSourceId dataSourceId, BlockMetaData blockMetaData, List<ColumnStatistics> actualColumnStatistics)
            throws ParquetCorruptionException
    {
        List<ColumnChunkMetaData> columnChunks = blockMetaData.getColumns();
        checkArgument(
                columnChunks.size() == actualColumnStatistics.size(),
                "Column chunk metadata count %s did not match column fields count %s",
                columnChunks.size(),
                actualColumnStatistics.size());

        for (int columnIndex = 0; columnIndex < columnChunks.size(); columnIndex++) {
            ColumnChunkMetaData columnMetaData = columnChunks.get(columnIndex);
            ColumnStatistics columnStatistics = actualColumnStatistics.get(columnIndex);
            long expectedValuesCount = columnMetaData.getValueCount();
            validateParquet(
                    expectedValuesCount == columnStatistics.valuesCount(),
                    dataSourceId,
                    "Invalid values count for column %s: Expected %d, found %d",
                    columnIndex,
                    expectedValuesCount,
                    columnStatistics.valuesCount());

            Statistics<?> parquetStatistics = columnMetaData.getStatistics();
            if (parquetStatistics.isNumNullsSet()) {
                long expectedNullsCount = parquetStatistics.getNumNulls();
                validateParquet(
                        expectedNullsCount == columnStatistics.nonLeafValuesCount(),
                        dataSourceId,
                        "Invalid nulls count for column %s: Expected %d, found %d",
                        columnIndex,
                        expectedNullsCount,
                        columnStatistics.nonLeafValuesCount());
            }
        }
    }

    public static class StatisticsValidation
    {
        private final List<Type> types;
        private List<ColumnStatisticsValidation> columnStatisticsValidations;

        private StatisticsValidation(List<Type> types)
        {
            this.types = requireNonNull(types, "types is null");
            this.columnStatisticsValidations = types.stream()
                    .map(ColumnStatisticsValidation::new)
                    .collect(toImmutableList());
        }

        public static StatisticsValidation createStatisticsValidationBuilder(List<Type> readTypes)
        {
            return new StatisticsValidation(readTypes);
        }

        public void addPage(Page page)
        {
            requireNonNull(page, "page is null");
            checkArgument(
                    page.getChannelCount() == columnStatisticsValidations.size(),
                    "Invalid page: page channels count %s did not match columns count %s",
                    page.getChannelCount(),
                    columnStatisticsValidations.size());

            for (int channel = 0; channel < columnStatisticsValidations.size(); channel++) {
                ColumnStatisticsValidation columnStatisticsValidation = columnStatisticsValidations.get(channel);
                columnStatisticsValidation.addBlock(page.getBlock(channel));
            }
        }

        public void reset()
        {
            this.columnStatisticsValidations = types.stream()
                    .map(ColumnStatisticsValidation::new)
                    .collect(toImmutableList());
        }

        public List<ColumnStatistics> build()
        {
            return this.columnStatisticsValidations.stream()
                    .flatMap(validation -> validation.build().stream())
                    .collect(toImmutableList());
        }
    }

    public static class ParquetWriteValidationBuilder
    {
        private static final int INSTANCE_SIZE = instanceSize(ParquetWriteValidationBuilder.class);
        private static final int COLUMN_DESCRIPTOR_INSTANCE_SIZE = instanceSize(ColumnDescriptor.class);
        private static final int PRIMITIVE_TYPE_INSTANCE_SIZE = instanceSize(PrimitiveType.class);

        private final List<Type> types;
        private final List<String> columnNames;
        private final WriteChecksumBuilder checksum;

        private String createdBy;
        private Optional<String> timeZoneId = Optional.empty();
        private List<ColumnDescriptor> columns;
        private List<RowGroup> rowGroups;
        private long retainedSize = INSTANCE_SIZE;

        public ParquetWriteValidationBuilder(List<Type> types, List<String> columnNames)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
            checkArgument(
                    types.size() == columnNames.size(),
                    "Types count %s did not match column names count %s",
                    types.size(),
                    columnNames.size());
            this.checksum = new WriteChecksumBuilder(types);
            retainedSize += estimatedSizeOf(types, type -> 0)
                    + estimatedSizeOf(columnNames, SizeOf::estimatedSizeOf);
        }

        public long getRetainedSize()
        {
            return retainedSize;
        }

        public void setCreatedBy(String createdBy)
        {
            this.createdBy = createdBy;
            retainedSize += estimatedSizeOf(createdBy);
        }

        public void setTimeZone(Optional<String> timeZoneId)
        {
            this.timeZoneId = timeZoneId;
            timeZoneId.ifPresent(id -> retainedSize += estimatedSizeOf(id));
        }

        public void setColumns(List<ColumnDescriptor> columns)
        {
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            retainedSize += estimatedSizeOf(columns, descriptor -> {
                return COLUMN_DESCRIPTOR_INSTANCE_SIZE
                        + (2 * SIZE_OF_INT) // maxRep, maxDef
                        + estimatedSizeOfStringArray(descriptor.getPath())
                        + PRIMITIVE_TYPE_INSTANCE_SIZE
                        + (3 * SIZE_OF_INT); // primitive, length, columnOrder
            });
        }

        public void setRowGroups(List<RowGroup> rowGroups)
        {
            this.rowGroups = ImmutableList.copyOf(requireNonNull(rowGroups, "rowGroups is null"));
        }

        public void addPage(Page page)
        {
            checksum.addPage(page);
        }

        public ParquetWriteValidation build()
        {
            return new ParquetWriteValidation(
                    createdBy,
                    timeZoneId,
                    columns,
                    rowGroups,
                    checksum.build(),
                    types,
                    columnNames);
        }
    }

    // parquet-mr IndexReference class lacks equals and toString implementations
    static class IndexReferenceValidation
    {
        private final long offset;
        private final int length;

        private IndexReferenceValidation(long offset, int length)
        {
            this.offset = offset;
            this.length = length;
        }

        static IndexReferenceValidation fromIndexReference(IndexReference indexReference)
        {
            return new IndexReferenceValidation(indexReference.getOffset(), indexReference.getLength());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IndexReferenceValidation that = (IndexReferenceValidation) o;
            return offset == that.offset && length == that.length;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(offset, length);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("offset", offset)
                    .add("length", length)
                    .toString();
        }
    }

    private static <T, U> void verifyColumnMetadataMatch(
            boolean condition,
            String name,
            T actual,
            ColumnPath path,
            int rowGroup,
            ParquetDataSourceId dataSourceId,
            U expected)
            throws ParquetCorruptionException
    {
        if (!condition) {
            throw new ParquetCorruptionException(
                    dataSourceId,
                    "%s [%s] for column %s in row group %d did not match [%s]",
                    name,
                    actual,
                    path,
                    rowGroup,
                    expected);
        }
    }

    private static boolean areEncodingsSame(Set<org.apache.parquet.column.Encoding> actual, List<org.apache.parquet.format.Encoding> expected)
    {
        return actual.equals(expected.stream().map(METADATA_CONVERTER::getEncoding).collect(toImmutableSet()));
    }

    private static boolean areStatisticsSame(org.apache.parquet.column.statistics.Statistics<?> actual, org.apache.parquet.format.Statistics expected)
    {
        Statistics.Builder expectedStatsBuilder = Statistics.getBuilderForReading(actual.type());
        if (expected.isSetNull_count()) {
            expectedStatsBuilder.withNumNulls(expected.getNull_count());
        }
        if (expected.isSetMin_value()) {
            expectedStatsBuilder.withMin(expected.getMin_value());
        }
        if (expected.isSetMax_value()) {
            expectedStatsBuilder.withMax(expected.getMax_value());
        }
        return actual.equals(expectedStatsBuilder.build());
    }

    private static void validateColumnDescriptorsSame(ColumnDescriptor actual, ColumnDescriptor expected, ParquetDataSourceId dataSourceId)
            throws ParquetCorruptionException
    {
        // Column names are lower-cased by MetadataReader#readFooter
        validateParquet(
                Arrays.equals(actual.getPath(), Arrays.stream(expected.getPath()).map(field -> field.toLowerCase(Locale.ENGLISH)).toArray()),
                dataSourceId,
                "Column path %s did not match expected column path %s",
                actual.getPath(),
                expected.getPath());

        validateParquet(
                actual.getMaxDefinitionLevel() == expected.getMaxDefinitionLevel(),
                dataSourceId,
                "Column %s max definition level %d did not match expected max definition level %d",
                actual.getPath(),
                actual.getMaxDefinitionLevel(),
                expected.getMaxDefinitionLevel());

        validateParquet(
                actual.getMaxRepetitionLevel() == expected.getMaxRepetitionLevel(),
                dataSourceId,
                "Column %s max repetition level %d did not match expected max repetition level %d",
                actual.getPath(),
                actual.getMaxRepetitionLevel(),
                expected.getMaxRepetitionLevel());

        PrimitiveType actualPrimitiveType = actual.getPrimitiveType();
        PrimitiveType expectedPrimitiveType = expected.getPrimitiveType();
        // We don't use PrimitiveType#equals directly because column names are lower-cased by MetadataReader#readFooter
        validateParquet(
                actualPrimitiveType.getPrimitiveTypeName().equals(expectedPrimitiveType.getPrimitiveTypeName())
                        && actualPrimitiveType.getTypeLength() == expectedPrimitiveType.getTypeLength()
                        && actualPrimitiveType.getRepetition().equals(expectedPrimitiveType.getRepetition())
                        && actualPrimitiveType.getName().equals(expectedPrimitiveType.getName().toLowerCase(Locale.ENGLISH))
                        && Objects.equals(actualPrimitiveType.getLogicalTypeAnnotation(), expectedPrimitiveType.getLogicalTypeAnnotation()),
                dataSourceId,
                "Column %s primitive type %s did not match expected primitive type %s",
                actual.getPath(),
                actualPrimitiveType,
                expectedPrimitiveType);
    }

    private static long estimatedSizeOfStringArray(String[] path)
    {
        long size = sizeOf(path);
        for (String field : path) {
            size += estimatedSizeOf(field);
        }
        return size;
    }
}
