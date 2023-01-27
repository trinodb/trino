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
package io.trino.parquet.reader.flat;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.ColumnChunk;
import io.trino.parquet.reader.ColumnReader;
import io.trino.parquet.reader.FilteredRowRanges;
import io.trino.parquet.reader.PageReader;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.decoders.ValueDecoder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.Type;
import org.apache.parquet.io.ParquetDecodingException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.decoders.ValueDecoder.ValueDecodersProvider;
import static io.trino.parquet.reader.decoders.ValueDecoders.getDictionaryDecoder;
import static io.trino.parquet.reader.flat.RowRangesIterator.createRowRangesIterator;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FlatColumnReader<BufferType>
        implements ColumnReader
{
    private static final int[] EMPTY_DEFINITION_LEVELS = new int[0];
    private static final int[] EMPTY_REPETITION_LEVELS = new int[0];

    private final PrimitiveField field;
    private final ValueDecodersProvider<BufferType> decodersProvider;
    private final ColumnAdapter<BufferType> columnAdapter;
    private final LocalMemoryContext memoryContext;
    private PageReader pageReader;
    private RowRangesIterator rowRanges;

    private int readOffset;
    private int remainingPageValueCount;
    @Nullable
    private DictionaryDecoder<BufferType> dictionaryDecoder;
    private boolean produceDictionaryBlock;
    private FlatDefinitionLevelDecoder definitionLevelDecoder;
    private ValueDecoder<BufferType> valueDecoder;

    private int nextBatchSize;

    public FlatColumnReader(
            PrimitiveField field,
            ValueDecodersProvider<BufferType> decodersProvider,
            ColumnAdapter<BufferType> columnAdapter,
            LocalMemoryContext memoryContext)
    {
        this.field = requireNonNull(field, "field is null");
        this.decodersProvider = requireNonNull(decodersProvider, "decoders is null");
        this.columnAdapter = requireNonNull(columnAdapter, "columnAdapter is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    private void seek()
    {
        int remainingInBatch = readOffset;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                remainingInBatch = seekToNextPage(remainingInBatch);
                if (remainingInBatch == 0) {
                    break;
                }
                if (remainingPageValueCount == 0) {
                    throwEndOfBatchException(remainingInBatch);
                }
            }

            int chunkSize = Math.min(remainingPageValueCount, remainingInBatch);
            int nonNullCount;
            if (isNonNull()) {
                nonNullCount = chunkSize;
            }
            else {
                nonNullCount = definitionLevelDecoder.skip(chunkSize);
            }
            valueDecoder.skip(nonNullCount);
            remainingInBatch -= rowRanges.seekForward(chunkSize);
            remainingPageValueCount -= chunkSize;
        }
    }

    @VisibleForTesting
    ColumnChunk readNullable()
    {
        NullableValuesBuffer<BufferType> valuesBuffer = createNullableValuesBuffer(nextBatchSize);
        boolean[] isNull = new boolean[nextBatchSize];
        int remainingInBatch = nextBatchSize;
        int offset = 0;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextPage()) {
                    throwEndOfBatchException(remainingInBatch);
                }
            }

            if (skipToRowRangesStart()) {
                continue;
            }
            int chunkSize = rowRanges.advanceRange(Math.min(remainingPageValueCount, remainingInBatch));
            int nonNullCount = definitionLevelDecoder.readNext(isNull, offset, chunkSize);

            valuesBuffer.readNullableValues(valueDecoder, isNull, offset, nonNullCount, chunkSize);

            offset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingPageValueCount -= chunkSize;
        }
        return valuesBuffer.createNullableBlock(isNull, field.getType());
    }

    @VisibleForTesting
    ColumnChunk readNoNull()
    {
        NonNullValuesBuffer<BufferType> valuesBuffer = createNonNullValuesBuffer(nextBatchSize);
        int remainingInBatch = nextBatchSize;
        int offset = 0;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextPage()) {
                    throwEndOfBatchException(remainingInBatch);
                }
            }

            if (skipToRowRangesStart()) {
                continue;
            }
            int chunkSize = rowRanges.advanceRange(Math.min(remainingPageValueCount, remainingInBatch));

            valuesBuffer.readNonNullValues(valueDecoder, offset, chunkSize);
            offset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingPageValueCount -= chunkSize;
        }
        return valuesBuffer.createNonNullBlock(field.getType());
    }

    /**
     * Finds the number of values to be skipped in the current page to reach
     * the start of the current row range and uses that to skip ValueDecoder
     * and DefinitionLevelDecoder to the appropriate position.
     *
     * @return Whether to skip the entire remaining page
     */
    private boolean skipToRowRangesStart()
    {
        int skipCount = toIntExact(rowRanges.skipToRangeStart());
        if (skipCount >= remainingPageValueCount) {
            remainingPageValueCount = 0;
            return true;
        }
        if (skipCount > 0) {
            int nonNullsCount;
            if (isNonNull()) {
                nonNullsCount = skipCount;
            }
            else {
                nonNullsCount = definitionLevelDecoder.skip(skipCount);
            }
            valueDecoder.skip(nonNullsCount);
            remainingPageValueCount -= skipCount;
        }
        return false;
    }

    private boolean readNextPage()
    {
        if (!pageReader.hasNext()) {
            return false;
        }
        DataPage page = readPage();
        rowRanges.resetForNewPage(page.getFirstRowIndex());
        return true;
    }

    // When a large enough number of rows are skipped due to `seek` operation,
    // it is possible to skip decompressing and decoding parquet pages entirely.
    private int seekToNextPage(int remainingInBatch)
    {
        while (remainingInBatch > 0 && pageReader.hasNext()) {
            DataPage page = pageReader.getNextPage();
            rowRanges.resetForNewPage(page.getFirstRowIndex());
            if (remainingInBatch < page.getValueCount() || !rowRanges.isPageFullyConsumed(page.getValueCount())) {
                readPage();
                return remainingInBatch;
            }
            remainingInBatch -= page.getValueCount();
            remainingPageValueCount = 0;
            pageReader.skipNextPage();
        }
        return remainingInBatch;
    }

    private DataPage readPage()
    {
        DataPage page = pageReader.readPage();
        requireNonNull(page, "page is null");
        if (page instanceof DataPageV1) {
            readFlatPageV1((DataPageV1) page);
        }
        else if (page instanceof DataPageV2) {
            readFlatPageV2((DataPageV2) page);
        }
        // For a compressed data page, the memory used by the decompressed values data needs to be accounted
        // for separately as ParquetCompressionUtils#decompress allocates a new byte array for the decompressed result.
        // For an uncompressed data page, we read directly from input Slices whose memory usage is already accounted
        // for in AbstractParquetDataSource#ReferenceCountedReader.
        int dataPageSizeInBytes = pageReader.arePagesCompressed() ? page.getUncompressedSize() : 0;
        long dictionarySizeInBytes = dictionaryDecoder == null ? 0 : dictionaryDecoder.getRetainedSizeInBytes();
        memoryContext.setBytes(dataPageSizeInBytes + dictionarySizeInBytes);

        remainingPageValueCount = page.getValueCount();
        return page;
    }

    private void readFlatPageV1(DataPageV1 page)
    {
        Slice buffer = page.getSlice();
        ParquetEncoding definitionEncoding = page.getDefinitionLevelEncoding();

        checkArgument(isNonNull() || definitionEncoding == RLE, "Invalid definition level encoding: " + definitionEncoding);
        int alreadyRead = 0;
        if (definitionEncoding == RLE) {
            // Definition levels are skipped from file when the max definition level is 0 as the bit-width required to store them is 0.
            // This can happen for non-null (required) fields or nullable fields where all values are null.
            // See org.apache.parquet.column.Encoding.RLE.getValuesReader for reference.
            if (field.getDescriptor().getMaxDefinitionLevel() == 0) {
                definitionLevelDecoder = new ZeroDefinitionLevelDecoder();
            }
            else {
                int bufferSize = buffer.getInt(0); //  We need to read the size even if nulls are absent
                definitionLevelDecoder = new NullsDecoder(buffer.slice(Integer.BYTES, bufferSize));
                alreadyRead = bufferSize + Integer.BYTES;
            }
        }

        createValueDecoder(page.getValueEncoding(), buffer.slice(alreadyRead, buffer.length() - alreadyRead));
    }

    private void readFlatPageV2(DataPageV2 page)
    {
        int maxDefinitionLevel = field.getDescriptor().getMaxDefinitionLevel();
        checkArgument(maxDefinitionLevel >= 0 && maxDefinitionLevel <= 1, "Invalid max definition level: " + maxDefinitionLevel);

        definitionLevelDecoder = new NullsDecoder(page.getDefinitionLevels());

        createValueDecoder(page.getDataEncoding(), page.getSlice());
    }

    private void createValueDecoder(ParquetEncoding encoding, Slice data)
    {
        if (encoding == PLAIN_DICTIONARY || encoding == RLE_DICTIONARY) {
            if (dictionaryDecoder == null) {
                throw new ParquetDecodingException(format("Dictionary is missing for %s", field));
            }
            valueDecoder = dictionaryDecoder;
        }
        else {
            valueDecoder = decodersProvider.create(encoding, field);
        }
        valueDecoder.init(new SimpleSliceInputStream(data));
    }

    protected boolean isNonNull()
    {
        return field.isRequired() || pageReader.hasNoNulls();
    }

    @Override
    public boolean hasPageReader()
    {
        return pageReader != null;
    }

    @Override
    public void setPageReader(PageReader pageReader, Optional<FilteredRowRanges> rowRanges)
    {
        this.pageReader = requireNonNull(pageReader, "pageReader");
        // The dictionary page must be placed at the first position of the column chunk
        // if it is partly or completely dictionary encoded. At most one dictionary page
        // can be placed in a column chunk.
        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();

        // For dictionary based encodings - https://github.com/apache/parquet-format/blob/master/Encodings.md
        if (dictionaryPage != null) {
            dictionaryDecoder = getDictionaryDecoder(
                    dictionaryPage,
                    columnAdapter,
                    decodersProvider.create(PLAIN, field),
                    isNonNull());
            produceDictionaryBlock = shouldProduceDictionaryBlock(rowRanges);
        }
        this.rowRanges = createRowRangesIterator(rowRanges);
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public ColumnChunk readPrimitive()
    {
        ColumnChunk columnChunk;
        seek();
        if (isNonNull()) {
            columnChunk = readNoNull();
        }
        else {
            columnChunk = readNullable();
        }

        readOffset = 0;
        nextBatchSize = 0;
        return columnChunk;
    }

    private static void throwEndOfBatchException(int remainingInBatch)
    {
        throw new ParquetDecodingException(format("Corrupted Parquet file: extra %d values to be consumed when scanning current batch", remainingInBatch));
    }

    private boolean shouldProduceDictionaryBlock(Optional<FilteredRowRanges> filteredRowRanges)
    {
        // Parquet writer may choose to fall back to a non-dictionary encoding after starting with dictionary encoding if
        //   1. If the size of the dictionary exceeds a threshold (1MB for parquet-mr by default).
        //   2. Number of dictionary entries exceeds a threshold (Integer.MAX_VALUE for parquet-mr by default).
        // Trino dictionary blocks are produced only when the entire column chunk is dictionary encoded
        if (pageReader.hasOnlyDictionaryEncodedPages()) {
            // TODO: DictionaryBlocks are currently restricted to variable width types where dictionary processing is most beneficial.
            //   Dictionary processing for other data types can be enabled after validating improvements on benchmarks.
            if (!(field.getType() instanceof AbstractVariableWidthType)) {
                return false;
            }
            requireNonNull(dictionaryDecoder, "dictionaryDecoder is null");
            // Filtering of parquet pages using column indexes may result in the total number of values read from the
            // column chunk being lower than the size of the dictionary
            return filteredRowRanges.map(rowRanges -> rowRanges.getRowCount() > dictionaryDecoder.getDictionarySize())
                    .orElse(true);
        }
        return false;
    }

    private NonNullValuesBuffer<BufferType> createNonNullValuesBuffer(int batchSize)
    {
        if (produceDictionaryBlock) {
            return new DictionaryValuesBuffer<>(dictionaryDecoder, batchSize);
        }
        return new DataValuesBuffer<>(columnAdapter, batchSize);
    }

    private NullableValuesBuffer<BufferType> createNullableValuesBuffer(int batchSize)
    {
        if (produceDictionaryBlock) {
            return new DictionaryValuesBuffer<>(dictionaryDecoder, batchSize);
        }
        return new DataValuesBuffer<>(columnAdapter, batchSize);
    }

    private interface NonNullValuesBuffer<T>
    {
        void readNonNullValues(ValueDecoder<T> valueDecoder, int offset, int valuesCount);

        ColumnChunk createNonNullBlock(Type type);
    }

    private interface NullableValuesBuffer<T>
    {
        void readNullableValues(ValueDecoder<T> valueDecoder, boolean[] isNull, int offset, int nonNullCount, int valuesCount);

        ColumnChunk createNullableBlock(boolean[] isNull, Type type);
    }

    private static final class DataValuesBuffer<T>
            implements NonNullValuesBuffer<T>, NullableValuesBuffer<T>
    {
        private final ColumnAdapter<T> columnAdapter;
        private final T values;
        private final int batchSize;
        private int totalNullsCount;

        private DataValuesBuffer(ColumnAdapter<T> columnAdapter, int batchSize)
        {
            this.values = columnAdapter.createBuffer(batchSize);
            this.columnAdapter = columnAdapter;
            this.batchSize = batchSize;
        }

        @Override
        public void readNonNullValues(ValueDecoder<T> valueDecoder, int offset, int valuesCount)
        {
            valueDecoder.read(values, offset, valuesCount);
        }

        @Override
        public void readNullableValues(ValueDecoder<T> valueDecoder, boolean[] isNull, int offset, int nonNullCount, int valuesCount)
        {
            // Only nulls
            if (nonNullCount == 0) {
                // Unpack empty null table. This is almost always a no-op. However, in binary type
                // the last position offset needs to be propagated
                T tmpBuffer = columnAdapter.createTemporaryBuffer(offset, 0, values);
                columnAdapter.unpackNullValues(tmpBuffer, values, isNull, offset, 0, valuesCount);
            }
            // No nulls
            else if (nonNullCount == valuesCount) {
                valueDecoder.read(values, offset, nonNullCount);
            }
            else {
                // Read data values to a temporary array and unpack the nulls to the actual destination
                T tmpBuffer = columnAdapter.createTemporaryBuffer(offset, nonNullCount, values);
                valueDecoder.read(tmpBuffer, 0, nonNullCount);
                columnAdapter.unpackNullValues(tmpBuffer, values, isNull, offset, nonNullCount, valuesCount);
            }
            totalNullsCount += valuesCount - nonNullCount;
        }

        @Override
        public ColumnChunk createNonNullBlock(Type type)
        {
            checkState(
                    totalNullsCount == 0,
                    "totalNonNullsCount %s should be equal to 0 when creating non-null block",
                    totalNullsCount);
            return new ColumnChunk(columnAdapter.createNonNullBlock(values), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }

        @Override
        public ColumnChunk createNullableBlock(boolean[] isNull, Type type)
        {
            if (totalNullsCount == batchSize) {
                return new ColumnChunk(RunLengthEncodedBlock.create(type, null, batchSize), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
            }
            if (totalNullsCount == 0) {
                return new ColumnChunk(columnAdapter.createNonNullBlock(values), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
            }
            return new ColumnChunk(columnAdapter.createNullableBlock(isNull, values), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }
    }

    private static final class DictionaryValuesBuffer<T>
            implements NonNullValuesBuffer<T>, NullableValuesBuffer<T>
    {
        private final DictionaryDecoder<T> decoder;
        private final int[] ids;
        private final int batchSize;
        private int totalNullsCount;

        private DictionaryValuesBuffer(DictionaryDecoder<T> dictionaryDecoder, int batchSize)
        {
            this.ids = new int[batchSize];
            this.decoder = dictionaryDecoder;
            this.batchSize = batchSize;
        }

        @Override
        public void readNonNullValues(ValueDecoder<T> valueDecoder, int offset, int chunkSize)
        {
            decoder.readDictionaryIds(ids, offset, chunkSize);
        }

        @Override
        public void readNullableValues(ValueDecoder<T> valueDecoder, boolean[] isNull, int offset, int nonNullCount, int valuesCount)
        {
            // Parquet dictionary encodes only non-null values
            // Dictionary size is used as the id to denote nulls for Trino dictionary block
            if (nonNullCount == 0) {
                // Only nulls were encountered in chunkSize, add empty values for nulls
                Arrays.fill(ids, offset, offset + valuesCount, decoder.getDictionarySize());
            }
            // No nulls
            else if (nonNullCount == valuesCount) {
                decoder.readDictionaryIds(ids, offset, valuesCount);
            }
            else {
                // Read data values to a temporary array and unpack the nulls to the actual destination
                int[] tmpBuffer = new int[nonNullCount];
                decoder.readDictionaryIds(tmpBuffer, 0, nonNullCount);
                unpackDictionaryNullId(tmpBuffer, ids, isNull, offset, valuesCount, decoder.getDictionarySize());
            }
            totalNullsCount += valuesCount - nonNullCount;
        }

        @Override
        public ColumnChunk createNonNullBlock(Type type)
        {
            // This will return a nullable dictionary even if we are returning a batch of non-null values
            // for a nullable column. We avoid creating a new non-nullable dictionary to allow the engine
            // to optimize for the unchanged dictionary case.
            checkState(
                    totalNullsCount == 0,
                    "totalNonNullsCount %s should be equal to 0 when creating non-null block",
                    totalNullsCount);
            return createDictionaryBlock(ids, decoder.getDictionaryBlock());
        }

        @Override
        public ColumnChunk createNullableBlock(boolean[] isNull, Type type)
        {
            if (totalNullsCount == batchSize) {
                return new ColumnChunk(RunLengthEncodedBlock.create(type, null, batchSize), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
            }
            return createDictionaryBlock(ids, decoder.getDictionaryBlock());
        }

        private ColumnChunk createDictionaryBlock(int[] dictionaryIds, Block dictionary)
        {
            int positionsCount = dictionaryIds.length;
            return new ColumnChunk(
                    DictionaryBlock.create(positionsCount, dictionary, dictionaryIds),
                    EMPTY_DEFINITION_LEVELS,
                    EMPTY_REPETITION_LEVELS,
                    OptionalLong.of(getMaxDictionaryBlockSize(dictionary, positionsCount)));
        }

        private static void unpackDictionaryNullId(
                int[] source,
                int[] destination,
                boolean[] isNull,
                int destOffset,
                int chunkSize,
                int nullId)
        {
            int srcOffset = 0;
            for (int i = destOffset; i < destOffset + chunkSize; i++) {
                if (isNull[i]) {
                    destination[i] = nullId;
                }
                else {
                    destination[i] = source[srcOffset++];
                }
            }
        }

        private static long getMaxDictionaryBlockSize(Block dictionary, long batchSize)
        {
            // An approximate upper bound on size of DictionaryBlock is derived here instead of using
            // DictionaryBlock#getSizeInBytes directly because that method is expensive
            double maxDictionaryFractionUsed = Math.min((double) batchSize / dictionary.getPositionCount(), 1.0);
            return (long) (batchSize * Integer.BYTES + dictionary.getSizeInBytes() * maxDictionaryFractionUsed);
        }
    }
}
