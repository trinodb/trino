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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.AbstractColumnReader;
import io.trino.parquet.reader.ColumnChunk;
import io.trino.parquet.reader.FilteredRowRanges;
import io.trino.parquet.reader.PageReader;
import io.trino.parquet.reader.decoders.ValueDecoder;
import io.trino.parquet.reader.decoders.ValueDecoder.ValueDecodersProvider;
import io.trino.parquet.reader.flat.DictionaryDecoder.DictionaryDecoderProvider;
import io.trino.parquet.reader.flat.FlatDefinitionLevelDecoder.DefinitionLevelDecoderProvider;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.reader.flat.RowRangesIterator.ALL_ROW_RANGES_ITERATOR;
import static io.trino.spi.block.Bitmap.setBits;
import static io.trino.spi.block.Bitmap.wordsForBits;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class FlatColumnReader<BufferType>
        extends AbstractColumnReader<BufferType>
{
    private static final Logger log = Logger.get(FlatColumnReader.class);

    private static final int[] EMPTY_DEFINITION_LEVELS = new int[0];
    private static final int[] EMPTY_REPETITION_LEVELS = new int[0];
    private static final int MAX_PAGE_LOOKAHEAD = 8;

    private final DefinitionLevelDecoderProvider definitionLevelDecoderProvider;
    private final LocalMemoryContext memoryContext;

    private int remainingPageValueCount;
    private int deferredPageValueCount;
    private FlatDefinitionLevelDecoder definitionLevelDecoder;
    private ValueDecoder<BufferType> valueDecoder;
    private int readOffset;
    private int nextBatchSize;
    private long currentDataPageSizeInBytes;
    private final int[] skippedPageRanges = new int[MAX_PAGE_LOOKAHEAD * 2];
    private int skippedPageRangeCount;

    public FlatColumnReader(
            PrimitiveField field,
            ValueDecodersProvider<BufferType> decodersProvider,
            DefinitionLevelDecoderProvider definitionLevelDecoderProvider,
            DictionaryDecoderProvider<BufferType> dictionaryDecoderProvider,
            ColumnAdapter<BufferType> columnAdapter,
            LocalMemoryContext memoryContext)
    {
        super(field, decodersProvider, dictionaryDecoderProvider, columnAdapter);
        this.definitionLevelDecoderProvider = requireNonNull(definitionLevelDecoderProvider, "definitionLevelDecoderProvider is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    @Override
    public boolean hasPageReader()
    {
        return pageReader != null;
    }

    @Override
    public void setPageReader(PageReader pageReader, Optional<FilteredRowRanges> rowRanges)
    {
        super.setPageReader(pageReader, rowRanges);
        updateMemoryUsage();
    }

    @Override
    protected boolean isNonNull()
    {
        return field.isRequired() || pageReader.hasNoNulls();
    }

    @Override
    public ColumnChunk readPrimitive()
    {
        seek();
        ColumnChunk columnChunk;
        if (isNonNull()) {
            columnChunk = readNonNull();
        }
        else {
            columnChunk = readNullable();
        }

        readOffset = 0;
        nextBatchSize = 0;
        skippedPageRangeCount = 0;
        updateMemoryUsage();
        return columnChunk;
    }

    @Override
    public boolean supportsSelectedPositions()
    {
        return true;
    }

    @Override
    public int getDataPageReadCount()
    {
        return pageReader == null ? 0 : pageReader.getDataPageReadCount();
    }

    @Override
    public long preparePageFilteredRead(int[] positions, int offset, int positionCount, long maxBufferedBytes)
    {
        checkFromIndexSize(offset, positionCount, positions.length);
        checkArgument(maxBufferedBytes > 0, "maxBufferedBytes must be positive");
        skippedPageRangeCount = 0;
        if (pageReader == null || rowRanges != ALL_ROW_RANGES_ITERATOR) {
            return 0;
        }

        int valuesToConsume = readOffset + nextBatchSize;
        int pageStart = remainingPageValueCount;
        if (valuesToConsume <= pageStart) {
            return 0;
        }

        int selectedIndex = offset;
        int selectedEnd = offset + positionCount;
        long skippedPageBytes = 0;
        boolean firstPage = true;
        for (DataPage page : pageReader.getNextDataPages(valuesToConsume - pageStart + deferredPageValueCount, MAX_PAGE_LOOKAHEAD, maxBufferedBytes)) {
            int pageValueCount = page.getValueCount() - (firstPage ? deferredPageValueCount : 0);
            firstPage = false;
            int pageEnd = pageStart + pageValueCount;

            while (selectedIndex < selectedEnd && readOffset + positions[selectedIndex] < pageStart) {
                selectedIndex++;
            }
            boolean containsSelectedPosition = selectedIndex < selectedEnd && readOffset + positions[selectedIndex] < pageEnd;
            if (pageStart >= readOffset && pageEnd <= valuesToConsume && !containsSelectedPosition) {
                skippedPageBytes += page.getUncompressedSize();
                skippedPageRanges[skippedPageRangeCount * 2] = pageStart - readOffset;
                skippedPageRanges[skippedPageRangeCount * 2 + 1] = pageEnd - readOffset;
                skippedPageRangeCount++;
            }
            pageStart = pageEnd;
            if (pageStart >= valuesToConsume) {
                break;
            }
        }
        updateMemoryUsage();
        return skippedPageBytes;
    }

    @Override
    public ColumnChunk readPrimitive(int[] positions, int offset, int positionCount)
    {
        checkFromIndexSize(offset, positionCount, positions.length);

        seek();
        ColumnChunk columnChunk;
        if (isNonNull()) {
            columnChunk = readNonNull(positions, offset, positionCount);
        }
        else {
            columnChunk = readNullable(positions, offset, positionCount);
        }

        readOffset = 0;
        nextBatchSize = 0;
        skippedPageRangeCount = 0;
        updateMemoryUsage();
        return columnChunk;
    }

    @Override
    public ColumnChunk readPrimitivePageFiltered(int[] positions, int offset, int positionCount)
    {
        checkFromIndexSize(offset, positionCount, positions.length);
        checkState(skippedPageRangeCount > 0, "No skipped page ranges were identified");

        int skippedPositionCount = 0;
        for (int range = 0; range < skippedPageRangeCount; range++) {
            skippedPositionCount += skippedPageRanges[range * 2 + 1] - skippedPageRanges[range * 2];
        }
        int[] pagePositions = new int[nextBatchSize - skippedPositionCount];
        int pagePositionCount = 0;
        int range = 0;
        for (int position = 0; position < nextBatchSize; position++) {
            while (range < skippedPageRangeCount && position >= skippedPageRanges[range * 2 + 1]) {
                range++;
            }
            if (range >= skippedPageRangeCount || position < skippedPageRanges[range * 2]) {
                pagePositions[pagePositionCount++] = position;
            }
        }
        checkState(pagePositionCount == pagePositions.length, "Unexpected page position count");

        seek();
        ColumnChunk pageChunk;
        if (isNonNull()) {
            pageChunk = readNonNull(pagePositions, 0, pagePositions.length);
        }
        else {
            pageChunk = readNullable(pagePositions, 0, pagePositions.length);
        }

        int[] resultPositions = new int[positionCount];
        int skippedBefore = 0;
        range = 0;
        for (int index = 0; index < positionCount; index++) {
            int position = positions[offset + index];
            while (range < skippedPageRangeCount && skippedPageRanges[range * 2 + 1] <= position) {
                skippedBefore += skippedPageRanges[range * 2 + 1] - skippedPageRanges[range * 2];
                range++;
            }
            checkState(range >= skippedPageRangeCount || position < skippedPageRanges[range * 2], "Selected position is in a skipped page");
            resultPositions[index] = position - skippedBefore;
        }
        Block block = pageChunk.getBlock().getPositions(resultPositions, 0, resultPositions.length);
        ColumnChunk result = new ColumnChunk(
                block,
                pageChunk.getDefinitionLevels(),
                pageChunk.getRepetitionLevels(),
                OptionalLong.of(pageChunk.getMaxBlockSize()),
                pagePositions.length);

        readOffset = 0;
        nextBatchSize = 0;
        skippedPageRangeCount = 0;
        updateMemoryUsage();
        return result;
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    private void seek()
    {
        if (readOffset > 0) {
            log.debug("seek field %s, readOffset %d, remainingPageValueCount %d", field, readOffset, remainingPageValueCount);
        }
        skipRows(readOffset);
    }

    private void skipRows(int rowCount)
    {
        int remainingInBatch = rowCount;
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
        log.debug("readNullable field %s, nextBatchSize %d, remainingPageValueCount %d", field, nextBatchSize, remainingPageValueCount);
        NullableValuesBuffer<BufferType> valuesBuffer = createNullableValuesBuffer(nextBatchSize);
        long[] valueIsValid = new long[wordsForBits(nextBatchSize)];
        int remainingInBatch = nextBatchSize;
        int offset = 0;
        boolean validityMaterialized = false;
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
            int nonNullCount = definitionLevelDecoder.readNext(valueIsValid, offset, chunkSize);
            if (nonNullCount == chunkSize && validityMaterialized) {
                setBits(valueIsValid, 0, offset, chunkSize);
            }
            else if (nonNullCount < chunkSize && !validityMaterialized) {
                setBits(valueIsValid, 0, 0, offset);
                validityMaterialized = true;
            }

            valuesBuffer.readNullableValues(valueDecoder, valueIsValid, offset, nonNullCount, chunkSize);

            offset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingPageValueCount -= chunkSize;
        }
        return valuesBuffer.createNullableBlock(valueIsValid, field.getType());
    }

    private ColumnChunk readNullable(int[] positions, int offset, int positionCount)
    {
        log.debug("readNullable selected field %s, nextBatchSize %d, positionCount %d, remainingPageValueCount %d", field, nextBatchSize, positionCount, remainingPageValueCount);
        NullableValuesBuffer<BufferType> valuesBuffer = createNullableValuesBuffer(positionCount);
        long[] valueIsValid = new long[wordsForBits(positionCount)];
        readSelectedPositions(positions, offset, positionCount, (outputOffset, runLength) -> readNullableRows(valuesBuffer, valueIsValid, outputOffset, runLength));
        return valuesBuffer.createNullableBlock(valueIsValid, field.getType());
    }

    @VisibleForTesting
    ColumnChunk readNonNull()
    {
        log.debug("readNonNull field %s, nextBatchSize %d, remainingPageValueCount %d", field, nextBatchSize, remainingPageValueCount);
        NonNullValuesBuffer<BufferType> valuesBuffer = createNonNullValuesBuffer(nextBatchSize);
        readNonNullRows(valuesBuffer, 0, nextBatchSize);
        return valuesBuffer.createNonNullBlock(field.getType());
    }

    private ColumnChunk readNonNull(int[] positions, int offset, int positionCount)
    {
        log.debug("readNonNull selected field %s, nextBatchSize %d, positionCount %d, remainingPageValueCount %d", field, nextBatchSize, positionCount, remainingPageValueCount);
        NonNullValuesBuffer<BufferType> valuesBuffer = createNonNullValuesBuffer(positionCount);
        readSelectedPositions(positions, offset, positionCount, (outputOffset, runLength) -> readNonNullRows(valuesBuffer, outputOffset, runLength));
        return valuesBuffer.createNonNullBlock(field.getType());
    }

    private void readNonNullRows(NonNullValuesBuffer<BufferType> valuesBuffer, int offset, int rowCount)
    {
        int remainingInBatch = rowCount;
        int outputOffset = offset;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextPage()) {
                    throwEndOfBatchException(remainingInBatch);
                }
            }

            if (skipToRowRangesStart()) {
                continue;
            }
            int chunkSize = rowRanges.advanceRange(min(remainingPageValueCount, remainingInBatch));

            valuesBuffer.readNonNullValues(valueDecoder, outputOffset, chunkSize);
            outputOffset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingPageValueCount -= chunkSize;
        }
    }

    private void readNullableRows(NullableValuesBuffer<BufferType> valuesBuffer, long[] valueIsValid, int offset, int rowCount)
    {
        int remainingInBatch = rowCount;
        int outputOffset = offset;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextPage()) {
                    throwEndOfBatchException(remainingInBatch);
                }
            }

            if (skipToRowRangesStart()) {
                continue;
            }
            int chunkSize = rowRanges.advanceRange(min(remainingPageValueCount, remainingInBatch));
            int nonNullCount = definitionLevelDecoder.readNext(valueIsValid, outputOffset, chunkSize);
            if (nonNullCount == chunkSize) {
                setBits(valueIsValid, 0, outputOffset, chunkSize);
            }

            valuesBuffer.readNullableValues(valueDecoder, valueIsValid, outputOffset, nonNullCount, chunkSize);

            outputOffset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingPageValueCount -= chunkSize;
        }
    }

    private void readSelectedPositions(int[] positions, int offset, int positionCount, SelectedPositionsReader selectedPositionsReader)
    {
        int batchPosition = 0;
        int outputOffset = 0;
        int endOffset = offset + positionCount;
        for (int positionOffset = offset; positionOffset < endOffset; positionOffset++) {
            int position = positions[positionOffset];
            checkArgument(position >= batchPosition, "positions must be strictly ascending");
            checkArgument(position < nextBatchSize, "position %s is outside of batch size %s", position, nextBatchSize);
            skipRows(position - batchPosition);

            int runLength = 1;
            while (positionOffset + runLength < endOffset && positions[positionOffset + runLength] == position + runLength) {
                runLength++;
            }
            checkArgument(position + runLength <= nextBatchSize, "position %s is outside of batch size %s", position + runLength - 1, nextBatchSize);
            selectedPositionsReader.read(outputOffset, runLength);
            positionOffset += runLength - 1;
            batchPosition = position + runLength;
            outputOffset += runLength;
        }
        skipRows(nextBatchSize - batchPosition);
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
        if (skipCount > 0) {
            log.debug("skipCount %d, remainingPageValueCount %d", skipCount, remainingPageValueCount);
        }
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
        if (deferredPageValueCount > 0) {
            int skipCount = deferredPageValueCount;
            deferredPageValueCount = 0;
            int nonNullCount = isNonNull() ? skipCount : definitionLevelDecoder.skip(skipCount);
            valueDecoder.skip(nonNullCount);
            remainingPageValueCount -= skipCount;
        }
        return true;
    }

    // When a large enough number of rows are skipped due to `seek` operation,
    // it is possible to skip decompressing and decoding parquet pages entirely.
    private int seekToNextPage(int remainingInBatch)
    {
        while (remainingInBatch > 0 && pageReader.hasNext()) {
            DataPage page = pageReader.getNextPage();
            if (rowRanges == ALL_ROW_RANGES_ITERATOR) {
                int remainingPageValues = page.getValueCount() - deferredPageValueCount;
                if (remainingInBatch < remainingPageValues) {
                    deferredPageValueCount += remainingInBatch;
                    return 0;
                }
                remainingInBatch -= remainingPageValues;
                deferredPageValueCount = 0;
                remainingPageValueCount = 0;
                pageReader.skipNextPage();
                continue;
            }
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
        log.debug("readNextPage field %s, page %s", field, page);
        if (page instanceof DataPageV1 dataPageV1) {
            readFlatPageV1(dataPageV1);
        }
        else if (page instanceof DataPageV2 dataPageV2) {
            readFlatPageV2(dataPageV2);
        }
        // For a compressed data page, the memory used by the decompressed values data needs to be accounted
        // for separately as ParquetCompressionUtils#decompress allocates a new byte array for the decompressed result.
        // For an uncompressed data page, we read directly from input Slices whose memory usage is already accounted
        // for in AbstractParquetDataSource#ReferenceCountedReader.
        currentDataPageSizeInBytes = pageReader.arePagesCompressed() ? page.getUncompressedSize() : 0;
        updateMemoryUsage();

        remainingPageValueCount = page.getValueCount();
        return page;
    }

    private void updateMemoryUsage()
    {
        long dictionarySizeInBytes = dictionaryDecoder == null ? 0 : dictionaryDecoder.getRetainedSizeInBytes();
        memoryContext.setBytes(currentDataPageSizeInBytes + dictionarySizeInBytes + pageReader.getRetainedPageBytes());
    }

    private interface SelectedPositionsReader
    {
        void read(int outputOffset, int runLength);
    }

    private void readFlatPageV1(DataPageV1 page)
    {
        Slice buffer = page.getSlice();
        ParquetEncoding definitionEncoding = page.getDefinitionLevelEncoding();

        checkArgument(isNonNull() || definitionEncoding == RLE, "Invalid definition level encoding: %s", definitionEncoding);
        int alreadyRead = 0;
        if (definitionEncoding == RLE) {
            // Definition levels are skipped from file when the max definition level is 0 as the bit-width required to store them is 0.
            // This can happen for non-null (required) fields or nullable fields where all values are null.
            // See org.apache.parquet.column.Encoding.RLE.getValuesReader for reference.
            int maxDefinitionLevel = field.getDescriptor().getMaxDefinitionLevel();
            definitionLevelDecoder = definitionLevelDecoderProvider.create(maxDefinitionLevel);
            if (maxDefinitionLevel > 0) {
                int bufferSize = buffer.getInt(0); //  We need to read the size even if nulls are absent
                definitionLevelDecoder.init(buffer.slice(Integer.BYTES, bufferSize));
                alreadyRead = bufferSize + Integer.BYTES;
            }
        }

        valueDecoder = createValueDecoder(decodersProvider, page.getValueEncoding(), buffer.slice(alreadyRead, buffer.length() - alreadyRead));
    }

    private void readFlatPageV2(DataPageV2 page)
    {
        definitionLevelDecoder = definitionLevelDecoderProvider.create(field.getDescriptor().getMaxDefinitionLevel());
        definitionLevelDecoder.init(page.getDefinitionLevels());
        valueDecoder = createValueDecoder(decodersProvider, page.getDataEncoding(), page.getSlice());
    }

    private NonNullValuesBuffer<BufferType> createNonNullValuesBuffer(int batchSize)
    {
        if (produceDictionaryBlock()) {
            return new DictionaryValuesBuffer<>(field, dictionaryDecoder, batchSize);
        }
        return new DataValuesBuffer<>(field, columnAdapter, batchSize);
    }

    private NullableValuesBuffer<BufferType> createNullableValuesBuffer(int batchSize)
    {
        if (produceDictionaryBlock()) {
            return new DictionaryValuesBuffer<>(field, dictionaryDecoder, batchSize);
        }
        return new DataValuesBuffer<>(field, columnAdapter, batchSize);
    }

    private interface NonNullValuesBuffer<T>
    {
        void readNonNullValues(ValueDecoder<T> valueDecoder, int offset, int valuesCount);

        ColumnChunk createNonNullBlock(Type type);
    }

    private interface NullableValuesBuffer<T>
    {
        void readNullableValues(ValueDecoder<T> valueDecoder, long[] valueIsValid, int offset, int nonNullCount, int valuesCount);

        ColumnChunk createNullableBlock(long[] valueIsValid, Type type);
    }

    private static final class DataValuesBuffer<T>
            implements NonNullValuesBuffer<T>, NullableValuesBuffer<T>
    {
        private final PrimitiveField field;
        private final ColumnAdapter<T> columnAdapter;
        private final T values;
        private final int batchSize;
        private int totalNullsCount;

        private DataValuesBuffer(PrimitiveField field, ColumnAdapter<T> columnAdapter, int batchSize)
        {
            this.field = field;
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
        public void readNullableValues(ValueDecoder<T> valueDecoder, long[] valueIsValid, int offset, int nonNullCount, int valuesCount)
        {
            // Only nulls
            if (nonNullCount == 0) {
                // Unpack empty null table. This is almost always a no-op. However, in binary type
                // the last position offset needs to be propagated
                T tmpBuffer = columnAdapter.createTemporaryBuffer(offset, 0, values);
                columnAdapter.unpackNullValues(tmpBuffer, values, valueIsValid, offset, 0, valuesCount);
            }
            // No nulls
            else if (nonNullCount == valuesCount) {
                valueDecoder.read(values, offset, nonNullCount);
            }
            else {
                // Read data values to a temporary array and unpack the nulls to the actual destination
                T tmpBuffer = columnAdapter.createTemporaryBuffer(offset, nonNullCount, values);
                valueDecoder.read(tmpBuffer, 0, nonNullCount);
                columnAdapter.unpackNullValues(tmpBuffer, values, valueIsValid, offset, nonNullCount, valuesCount);
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
            log.debug("DataValuesBuffer createNonNullBlock field %s, totalNullsCount %d", field, totalNullsCount);
            return new ColumnChunk(columnAdapter.createNonNullBlock(values), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }

        @Override
        public ColumnChunk createNullableBlock(long[] valueIsValid, Type type)
        {
            log.debug("DataValuesBuffer createNullableBlock field %s, totalNullsCount %d, batchSize %d", field, totalNullsCount, batchSize);
            if (totalNullsCount == batchSize) {
                return new ColumnChunk(RunLengthEncodedBlock.create(type, null, batchSize), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
            }
            if (totalNullsCount == 0) {
                return new ColumnChunk(columnAdapter.createNonNullBlock(values), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
            }
            return new ColumnChunk(columnAdapter.createNullableBlock(valueIsValid, values), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }
    }

    private static final class DictionaryValuesBuffer<T>
            implements NonNullValuesBuffer<T>, NullableValuesBuffer<T>
    {
        private final PrimitiveField field;
        private final DictionaryDecoder<T> decoder;
        private final int[] ids;
        private final int batchSize;
        private int totalNullsCount;

        private DictionaryValuesBuffer(PrimitiveField field, DictionaryDecoder<T> dictionaryDecoder, int batchSize)
        {
            this.field = field;
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
        public void readNullableValues(ValueDecoder<T> valueDecoder, long[] valueIsValid, int offset, int nonNullCount, int valuesCount)
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
                unpackDictionaryNullId(tmpBuffer, ids, valueIsValid, offset, valuesCount, decoder.getDictionarySize());
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
            log.debug("DictionaryValuesBuffer createNonNullBlock field %s, totalNullsCount %d", field, totalNullsCount);
            return createDictionaryBlock(ids, decoder.getDictionaryBlock(), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }

        @Override
        public ColumnChunk createNullableBlock(long[] valueIsValid, Type type)
        {
            log.debug("DictionaryValuesBuffer createNullableBlock field %s, totalNullsCount %d, batchSize %d", field, totalNullsCount, batchSize);
            if (totalNullsCount == batchSize) {
                return new ColumnChunk(RunLengthEncodedBlock.create(type, null, batchSize), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
            }
            return createDictionaryBlock(ids, decoder.getDictionaryBlock(), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }
    }
}
