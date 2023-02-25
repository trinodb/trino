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

import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.decoders.RleBitPackingHybridDecoder;
import io.trino.parquet.reader.decoders.ValueDecoder;
import io.trino.parquet.reader.decoders.ValueDecoder.EmptyValueDecoder;
import io.trino.parquet.reader.flat.ColumnAdapter;
import io.trino.parquet.reader.flat.DictionaryDecoder;
import io.trino.spi.block.RunLengthEncodedBlock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.ParquetReaderUtils.castToByte;
import static io.trino.parquet.reader.decoders.ValueDecoder.ValueDecodersProvider;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;

/**
 * This class works similarly to FlatColumnReader. The difference is that the resulting number
 * of values might (and usually is) different from the number of chunks. Therefore the output buffers
 * are dynamically sized and some repetition/definition levels logic is added.
 * This reader is universal i.e. it will properly read flat data, yet flat readers are preferred
 * due to better performance.
 * <p>
 * Brief explanation of reading repetition and definition levels:
 * Repetition level equal to 0 means that we should start a new row, i.e. set of values
 * Any other value means that we continue adding to the current row
 * Following data (containing 3 rows):
 * repetition levels: 0,1,1,0,0,1,[0] (last 0 implicit)
 * values:            1,2,3,4,5,6
 * will result in sets of (1,2,3), (4), (5,6).
 * <p>
 * The biggest complication here is that in order to know if n-th value is the last in a row we need
 * to check the n-th+1 repetition level. So if the page has n values we need to wait for the beginning
 * of the next page to figure out whether the row is finished or contains additional values.
 * Above example split into 3 pages would look like:
 * repetition levels: 0,1  1,0  0,1
 * values:            1,2  3,4  5,6
 * Reading the first page will only give us information that the first row starts with values (1,2), but
 * we need to wait for another page to figure out that it contains another value (3). After reading another
 * row from page 2 we still need to read page 3 just to find out that the first repetition level is '0' and
 * the row is already over.
 * <p>
 * Definition levels encodes one of 3 options:
 * -value exists and is non-null (level = maxDef)
 * -value is null (level = maxDef - 1)
 * -there is no value (level &lt; maxDef - 1)
 * For non-nullable (REQUIRED) fields the (level = maxDef - 1) condition means non-existing value as well.
 * <p>
 * Quick example (maxDef level is 2):
 * Read 3 rows out of:
 * repetition levels: 0,1,1,0,0,1,0,...
 * definition levels: 0,1,2,1,0,2,...
 * values:            1,2,3,4,5,6,...
 * Resulting buffer:    n,3,n,  6
 * that is later translated to (n,3),(n),(6)
 * where n = null
 */
public class NestedColumnReader<BufferType>
        extends AbstractColumnReader<BufferType>
{
    private static final Logger log = Logger.get(NestedColumnReader.class);

    private final LocalMemoryContext memoryContext;

    private ValueDecoder<int[]> definitionLevelDecoder;
    private ValueDecoder<int[]> repetitionLevelDecoder;
    private ValueDecoder<BufferType> valueDecoder;
    private int[] repetitionBuffer;
    private int readOffset;
    private int nextBatchSize;
    private boolean pageLastRowUnfinished;
    // True if the last row of the previously read page was skipped, instead of read.
    // This way the remaining part of this row that may be stored in the next page
    // will be skipped as well
    private boolean pageLastRowSkipped;

    private int remainingPageValueCount;
    private int pageValueCount;

    public NestedColumnReader(
            PrimitiveField field,
            ValueDecodersProvider<BufferType> decodersProvider,
            ColumnAdapter<BufferType> columnAdapter,
            LocalMemoryContext memoryContext)
    {
        super(field, decodersProvider, columnAdapter);
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    @Override
    public boolean hasPageReader()
    {
        return pageReader != null;
    }

    @Override
    protected boolean isNonNull()
    {
        return field.isRequired();
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
        return columnChunk;
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    private ColumnChunk readNullable()
    {
        log.debug("readNullable field %s, nextBatchSize %d", field, nextBatchSize);
        NullableValuesBuffer<BufferType> data = createNullableValuesBuffer();
        BooleansBuffer isNull = new BooleansBuffer();
        IntegersBuffer outputRepetitionLevels = new IntegersBuffer();
        IntegersBuffer outputDefinitionLevels = new IntegersBuffer();
        int remainingInBatch = nextBatchSize;

        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextPage()) {
                    if (pageLastRowUnfinished && remainingInBatch == 1) {
                        break; // No more data so the last row is closed
                    }
                    throwEndOfBatchException(remainingInBatch);
                }

                if (pageLastRowUnfinished) {
                    int pageIndex = readUnfinishedRow();
                    if (pageLastRowSkipped) {
                        remainingPageValueCount -= pageIndex;
                        int[] definitionLevels = new int[pageIndex];
                        definitionLevelDecoder.read(definitionLevels, 0, definitionLevels.length);
                        int existingValueCount = countExistingValues(field.getDefinitionLevel(), definitionLevels);
                        valueDecoder.skip(existingValueCount);
                    }
                    else {
                        if (pageIndex > 0) {
                            int[] definitionLevels = new int[pageIndex];
                            int existingValueCount = readDefinitionLevels(outputDefinitionLevels, definitionLevels);
                            readNullableValues(data, isNull, outputRepetitionLevels, 0, pageIndex, definitionLevels, existingValueCount);
                        }
                        if (pageIndex == pageValueCount) { // Current row spans more than two Parquet pages
                            checkState(pageLastRowUnfinished, "pageLastRowUnfinished not set when run out of values to read");
                            continue;
                        }
                        remainingInBatch--;
                    }
                }
            }

            if (skip(field.getDefinitionLevel())) {
                continue;
            }
            ValueCount valueCount = getNextPositions(Math.min(rowRanges.getRowsLeftInCurrentRange(), remainingInBatch));
            rowRanges.advanceRange(valueCount.rows + (pageLastRowUnfinished ? 1 : 0));
            int pageValuesIndex = pageValueCount - remainingPageValueCount;

            int[] definitionLevels = new int[valueCount.values];
            int existingValueCount = readDefinitionLevels(outputDefinitionLevels, definitionLevels);
            readNullableValues(data, isNull, outputRepetitionLevels, pageValuesIndex, valueCount.values, definitionLevels, existingValueCount);

            remainingInBatch -= valueCount.rows;
        }

        return data.createNullableBlock(isNull, outputDefinitionLevels.getMergedBuffer(), outputRepetitionLevels.getMergedBuffer());
    }

    private ColumnChunk readNonNull()
    {
        log.debug("readNonNull field %s, nextBatchSize %d", field, nextBatchSize);
        NonNullValuesBuffer<BufferType> data = createNonNullValuesBuffer();
        IntegersBuffer outputRepetitionLevels = new IntegersBuffer();
        IntegersBuffer outputDefinitionLevels = new IntegersBuffer();
        int remainingInBatch = nextBatchSize;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextPage()) {
                    if (pageLastRowUnfinished && remainingInBatch == 1) {
                        break; // No more data so the last row is closed
                    }
                    throwEndOfBatchException(remainingInBatch);
                }

                if (pageLastRowUnfinished) {
                    int pageIndex = readUnfinishedRow();
                    if (pageLastRowSkipped) {
                        remainingPageValueCount -= pageIndex;
                        int[] definitionLevels = new int[pageIndex];
                        definitionLevelDecoder.read(definitionLevels, 0, definitionLevels.length);
                        int existingValueCount = countExistingValues(field.getDefinitionLevel(), definitionLevels);
                        valueDecoder.skip(existingValueCount);
                        if (pageIndex == pageValueCount) { // Current row spans more than two Parquet pages
                            continue;
                        }
                    }
                    else {
                        if (pageIndex > 0) {
                            readNonNullValues(data, outputRepetitionLevels, outputDefinitionLevels, 0, pageIndex);
                        }
                        if (pageIndex == pageValueCount) { // Current row spans more than two Parquet pages
                            continue;
                        }
                        remainingInBatch--;
                    }
                }
            }

            if (skip(field.getDefinitionLevel())) {
                continue;
            }
            ValueCount valueCount = getNextPositions(Math.min(rowRanges.getRowsLeftInCurrentRange(), remainingInBatch));
            rowRanges.advanceRange(valueCount.rows + (pageLastRowUnfinished ? 1 : 0));
            int pageValuesIndex = pageValueCount - remainingPageValueCount;
            readNonNullValues(data, outputRepetitionLevels, outputDefinitionLevels, pageValuesIndex, valueCount.values);

            remainingInBatch -= valueCount.rows;
        }

        return data.createNonNullBlock(outputDefinitionLevels.getMergedBuffer(), outputRepetitionLevels.getMergedBuffer());
    }

    private void seek()
    {
        if (readOffset > 0) {
            log.debug("seek field %s, readOffset %d, remainingPageValueCount %d, pageLastRowUnfinished %b", field, readOffset, remainingPageValueCount, pageLastRowUnfinished);
        }
        int remainingInBatch = readOffset;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextPage()) {
                    break;
                }
                if (pageLastRowUnfinished) {
                    int pageIndex = readUnfinishedRow();
                    if (pageIndex > 0) {
                        seek(pageIndex);
                    }
                    if (remainingPageValueCount == 0) { // The row spans more than two Parquet pages
                        checkState(pageLastRowUnfinished, "pageLastRowUnfinished not set when run out of values to skip");
                        checkState(pageLastRowSkipped, "pageLastRowSkipped not set when run out of values to skip");
                        continue;
                    }
                    remainingInBatch--;
                }
            }

            int chunkSize = Math.min(remainingPageValueCount, remainingInBatch);
            ValueCount valueCount = getNextPositions(toIntExact(chunkSize));

            seek(valueCount.values);

            int seek = rowRanges.seekForward(valueCount.rows + (pageLastRowUnfinished ? 1 : 0));
            remainingInBatch -= seek - (pageLastRowUnfinished ? 1 : 0);
        }
    }

    private int readDefinitionLevels(IntegersBuffer outputDefinitionLevels, int[] definitionLevels)
    {
        definitionLevelDecoder.read(definitionLevels, 0, definitionLevels.length);
        outputDefinitionLevels.add(definitionLevels);
        return countExistingValues(field.getDefinitionLevel() - 1, definitionLevels);
    }

    private void readNullableValues(
            NullableValuesBuffer<BufferType> data,
            BooleansBuffer isNull,
            IntegersBuffer outputRepetitionLevels,
            int pageValuesIndex,
            int valueCount,
            int[] definitionLevels,
            int existingValueCount)
    {
        boolean[] isNullChunk = new boolean[existingValueCount];
        isNull.add(isNullChunk);
        int nonNullCount = getNulls(definitionLevels, field.getDefinitionLevel(), isNullChunk);
        checkState(
                nonNullCount <= existingValueCount,
                "nonNullCount %s cannot be greater than existingValueCount %s, field %s",
                nonNullCount,
                existingValueCount,
                field);

        outputRepetitionLevels.add(Arrays.copyOfRange(repetitionBuffer, pageValuesIndex, pageValuesIndex + valueCount));

        data.readNullableValues(valueDecoder, isNullChunk, nonNullCount, existingValueCount);
        remainingPageValueCount -= valueCount;
    }

    private void readNonNullValues(NonNullValuesBuffer<BufferType> data, IntegersBuffer outputRepetitionLevels, IntegersBuffer outputDefinitionLevels, int pageValuesIndex, int valueCount)
    {
        int[] definitionLevels = new int[valueCount];
        definitionLevelDecoder.read(definitionLevels, 0, definitionLevels.length);
        int existingValueCount = countExistingValues(field.getDefinitionLevel(), definitionLevels);

        outputRepetitionLevels.add(Arrays.copyOfRange(repetitionBuffer, pageValuesIndex, pageValuesIndex + valueCount));
        outputDefinitionLevels.add(definitionLevels);

        if (existingValueCount > 0) {
            data.readNonNullValues(valueDecoder, existingValueCount);
        }
        remainingPageValueCount -= valueCount;
    }

    private boolean skip(int minDefinitionLevel)
    {
        long skipCount = rowRanges.skipToRangeStart();
        if (skipCount > 0) {
            log.debug("skipCount %d, remainingPageValueCount %d", skipCount, remainingPageValueCount);
        }
        if (skipCount >= remainingPageValueCount) {
            remainingPageValueCount = 0;
            pageLastRowUnfinished = true;
            pageLastRowSkipped = true;
            return true;
        }
        if (skipCount > 0) {
            ValueCount toSkip = getNextPositions(toIntExact(skipCount));
            if (toSkip.values == remainingPageValueCount) {
                remainingPageValueCount = 0;
                pageLastRowSkipped = true;
                return true;
            }
            int[] definitionLevels = new int[toSkip.values];
            definitionLevelDecoder.read(definitionLevels, 0, toSkip.values);
            int valuesToSkip = countExistingValues(minDefinitionLevel, definitionLevels);
            valueDecoder.skip(valuesToSkip);
            remainingPageValueCount -= toSkip.values;
        }
        return false;
    }

    private int getNulls(int[] definitionLevels, int maxDefinitionLevel, boolean[] localIsNull)
    {
        // Value is null if its definition level is equal to (max def level - 1)
        int outputIndex = 0;
        int nonNullCount = 0;
        for (int definitionLevel : definitionLevels) {
            boolean isValueNull = definitionLevel == maxDefinitionLevel - 1;
            boolean isValueNonNull = definitionLevel == maxDefinitionLevel;
            if (isValueNull) {
                localIsNull[outputIndex] = true;
            }
            outputIndex += castToByte(isValueNull | isValueNonNull);
            nonNullCount += castToByte(isValueNonNull);
        }
        return nonNullCount;
    }

    private int countExistingValues(int minDefinitionLevel, int[] definitionLevels)
    {
        int valueCount = 0;
        for (int definitionLevel : definitionLevels) {
            valueCount += castToByte(definitionLevel >= minDefinitionLevel);
        }
        return valueCount;
    }

    private int readUnfinishedRow()
    {
        int pageIndex = 0;
        while (pageIndex < remainingPageValueCount && repetitionBuffer[pageIndex] != 0) {
            pageIndex++;
        }
        return pageIndex;
    }

    /**
     * Calculates number of values upto a desired number of rows or the end of the page, whichever comes first.
     * Return two values:
     * -number of rows read fully. If the end of page is reached the number of rows is always lower by 1, since
     * the information whether the row is finished is stored in repetition levels of the next page. In that case
     * `pageLastRowUnfinished` is set.
     * -number of values read
     */
    private ValueCount getNextPositions(int desiredRowCount)
    {
        int valueCount = 0;
        int rowCount = 0;
        int pageValuesIndex = pageValueCount - remainingPageValueCount;
        for (; rowCount < desiredRowCount && valueCount < remainingPageValueCount - 1; valueCount++) {
            rowCount += castToByte(repetitionBuffer[pageValuesIndex + valueCount + 1] == 0);
        }

        boolean pageReadUptoLastValue = rowCount != desiredRowCount;
        if (pageReadUptoLastValue) {
            valueCount++;
            pageLastRowUnfinished = true;
            pageLastRowSkipped = false;
        }
        else {
            pageLastRowUnfinished = false;
        }

        return new ValueCount(rowCount, valueCount);
    }

    /**
     * Skip first `valueCount` values as a result of seek operation before reading the data
     */
    private void seek(int valueCount)
    {
        int[] definitionLevels = new int[valueCount];
        definitionLevelDecoder.read(definitionLevels, 0, definitionLevels.length);
        int maxDefinitionLevel = field.getDefinitionLevel();
        int valuesToSkip = 0;
        for (int definitionLevel : definitionLevels) {
            valuesToSkip += castToByte(definitionLevel == maxDefinitionLevel);
        }

        valueDecoder.skip(valuesToSkip);
        remainingPageValueCount -= valueCount;
        if (remainingPageValueCount == 0) {
            pageLastRowUnfinished = true;
            pageLastRowSkipped = true;
        }
    }

    private boolean readNextPage()
    {
        DataPage page = pageReader.readPage();
        if (page == null) {
            return false;
        }

        log.debug("readNextPage field %s, page %s, pageLastRowUnfinished %b", field, page, pageLastRowUnfinished);
        if (page instanceof DataPageV1) {
            readFlatPageV1((DataPageV1) page);
        }
        else if (page instanceof DataPageV2) {
            readFlatPageV2((DataPageV2) page);
        }

        pageValueCount = page.getValueCount();
        // We don't know how much values we will get in one batch so we read the whole page.
        // This makes decoding faster unless major parts of the page are skipped
        repetitionBuffer = new int[pageValueCount];
        repetitionLevelDecoder.read(repetitionBuffer, 0, pageValueCount);
        remainingPageValueCount = pageValueCount;
        rowRanges.resetForNewPage(page.getFirstRowIndex());

        // For a compressed data page, the memory used by the decompressed values data needs to be accounted
        // for separately as ParquetCompressionUtils#decompress allocates a new byte array for the decompressed result.
        // For an uncompressed data page, we read directly from input Slices whose memory usage is already accounted
        // for in AbstractParquetDataSource#ReferenceCountedReader.
        int dataPageSizeInBytes = pageReader.arePagesCompressed() ? page.getUncompressedSize() : 0;
        long dictionarySizeInBytes = dictionaryDecoder == null ? 0 : dictionaryDecoder.getRetainedSizeInBytes();
        long repetitionBufferSizeInBytes = sizeOf(repetitionBuffer);
        memoryContext.setBytes(dataPageSizeInBytes + dictionarySizeInBytes + repetitionBufferSizeInBytes);
        return true;
    }

    private void readFlatPageV1(DataPageV1 page)
    {
        Slice buffer = page.getSlice();
        ParquetEncoding definitionEncoding = page.getDefinitionLevelEncoding();
        ParquetEncoding repetitionEncoding = page.getRepetitionLevelEncoding();
        int maxDefinitionLevel = field.getDefinitionLevel();
        int maxRepetitionLevel = field.getRepetitionLevel();

        checkArgument(maxDefinitionLevel == 0 || definitionEncoding == RLE, "Invalid definition level encoding: " + definitionEncoding);
        checkArgument(maxRepetitionLevel == 0 || repetitionEncoding == RLE, "Invalid repetition level encoding: " + definitionEncoding);

        if (maxRepetitionLevel > 0) {
            int bufferSize = buffer.getInt(0); //  We need to read the size even if there is no repetition data
            repetitionLevelDecoder = new RleBitPackingHybridDecoder(getWidthFromMaxInt(maxRepetitionLevel));
            repetitionLevelDecoder.init(new SimpleSliceInputStream(buffer.slice(Integer.BYTES, bufferSize)));
            buffer = buffer.slice(bufferSize + Integer.BYTES, buffer.length() - bufferSize - Integer.BYTES);
        }
        else {
            repetitionLevelDecoder = new EmptyValueDecoder<>();
        }
        if (maxDefinitionLevel > 0) {
            int bufferSize = buffer.getInt(0); // We need to read the size even if there is no definition
            definitionLevelDecoder = new RleBitPackingHybridDecoder(getWidthFromMaxInt(field.getDefinitionLevel()));
            definitionLevelDecoder.init(new SimpleSliceInputStream(buffer.slice(Integer.BYTES, bufferSize)));
            buffer = buffer.slice(bufferSize + Integer.BYTES, buffer.length() - bufferSize - Integer.BYTES);
        }
        else {
            definitionLevelDecoder = new EmptyValueDecoder<>();
        }

        valueDecoder = createValueDecoder(decodersProvider, page.getValueEncoding(), buffer);
    }

    private void readFlatPageV2(DataPageV2 page)
    {
        int maxDefinitionLevel = field.getDefinitionLevel();
        int maxRepetitionLevel = field.getRepetitionLevel();

        if (maxDefinitionLevel == 0) {
            definitionLevelDecoder = new EmptyValueDecoder<>();
        }
        else {
            definitionLevelDecoder = new RleBitPackingHybridDecoder(getWidthFromMaxInt(maxDefinitionLevel));
            definitionLevelDecoder.init(new SimpleSliceInputStream(page.getDefinitionLevels()));
        }
        if (maxRepetitionLevel == 0) {
            repetitionLevelDecoder = new EmptyValueDecoder<>();
        }
        else {
            repetitionLevelDecoder = new RleBitPackingHybridDecoder(getWidthFromMaxInt(maxRepetitionLevel));
            repetitionLevelDecoder.init(new SimpleSliceInputStream(page.getRepetitionLevels()));
        }

        valueDecoder = createValueDecoder(decodersProvider, page.getDataEncoding(), page.getSlice());
    }

    /**
     * The reader will attempt to produce dictionary Trino pages using shared dictionary.
     * In case of data not being dictionary-encoded it falls back to normal decoding.
     */
    private NonNullValuesBuffer<BufferType> createNonNullValuesBuffer()
    {
        if (produceDictionaryBlock()) {
            return new DictionaryValuesBuffer<>(field, dictionaryDecoder);
        }
        return new DataValuesBuffer<>(field, columnAdapter);
    }

    private NullableValuesBuffer<BufferType> createNullableValuesBuffer()
    {
        if (produceDictionaryBlock()) {
            return new DictionaryValuesBuffer<>(field, dictionaryDecoder);
        }
        return new DataValuesBuffer<>(field, columnAdapter);
    }

    private interface NonNullValuesBuffer<T>
    {
        void readNonNullValues(ValueDecoder<T> valueDecoder, int existingValueCount);

        ColumnChunk createNonNullBlock(int[] definitions, int[] repetitions);
    }

    private interface NullableValuesBuffer<T>
    {
        void readNullableValues(ValueDecoder<T> valueDecoder, boolean[] isNull, int nonNullCount, int existingValueCount);

        ColumnChunk createNullableBlock(BooleansBuffer isNull, int[] definitions, int[] repetitions);
    }

    private static final class DataValuesBuffer<T>
            implements NonNullValuesBuffer<T>, NullableValuesBuffer<T>
    {
        private final PrimitiveField field;
        private final ColumnAdapter<T> columnAdapter;
        private final List<T> valueBuffers = new ArrayList<>();
        private int totalExistingValueCount;
        private int totalNonNullsCount;

        private DataValuesBuffer(PrimitiveField field, ColumnAdapter<T> columnAdapter)
        {
            this.field = field;
            this.columnAdapter = columnAdapter;
        }

        @Override
        public void readNonNullValues(ValueDecoder<T> valueDecoder, int existingValueCount)
        {
            T valueBuffer = columnAdapter.createBuffer(existingValueCount);
            valueDecoder.read(valueBuffer, 0, existingValueCount);
            valueBuffers.add(valueBuffer);
            totalNonNullsCount += existingValueCount;
            totalExistingValueCount += existingValueCount;
        }

        @Override
        public void readNullableValues(ValueDecoder<T> valueDecoder, boolean[] isNull, int nonNullCount, int existingValueCount)
        {
            // No nulls
            if (nonNullCount > 0 && nonNullCount == existingValueCount) {
                readNonNullValues(valueDecoder, existingValueCount);
                return;
            }

            // Only nulls
            if (nonNullCount == 0) {
                valueBuffers.add(columnAdapter.createBuffer(existingValueCount));
            }
            else {
                // Read data values to a temporary array and unpack the nulls to the actual destination
                T outBuffer = columnAdapter.createBuffer(existingValueCount);
                T tmpBuffer = columnAdapter.createTemporaryBuffer(0, nonNullCount, outBuffer);
                valueDecoder.read(tmpBuffer, 0, nonNullCount);
                columnAdapter.unpackNullValues(tmpBuffer, outBuffer, isNull, 0, nonNullCount, existingValueCount);
                valueBuffers.add(outBuffer);
            }
            totalNonNullsCount += nonNullCount;
            totalExistingValueCount += existingValueCount;
        }

        @Override
        public ColumnChunk createNonNullBlock(int[] definitions, int[] repetitions)
        {
            checkState(
                    totalNonNullsCount == totalExistingValueCount,
                    "totalNonNullsCount %s should be equal to totalExistingValueCount %s when creating non-null block",
                    totalNonNullsCount,
                    totalExistingValueCount);
            log.debug("DataValuesBuffer createNonNullBlock field %s, totalNonNullsCount %d, totalExistingValueCount %d", field, totalNonNullsCount, totalExistingValueCount);
            return new ColumnChunk(columnAdapter.createNonNullBlock(getMergedValues()), definitions, repetitions);
        }

        @Override
        public ColumnChunk createNullableBlock(BooleansBuffer isNull, int[] definitions, int[] repetitions)
        {
            log.debug("DataValuesBuffer createNullableBlock field %s, totalNonNullsCount %d, totalExistingValueCount %d", field, totalNonNullsCount, totalExistingValueCount);
            if (totalNonNullsCount == 0) {
                return new ColumnChunk(RunLengthEncodedBlock.create(field.getType(), null, totalExistingValueCount), definitions, repetitions);
            }
            if (totalNonNullsCount == totalExistingValueCount) {
                return new ColumnChunk(columnAdapter.createNonNullBlock(getMergedValues()), definitions, repetitions);
            }
            return new ColumnChunk(columnAdapter.createNullableBlock(isNull.getMergedBuffer(), getMergedValues()), definitions, repetitions);
        }

        private T getMergedValues()
        {
            if (valueBuffers.size() == 1) {
                return valueBuffers.get(0);
            }
            return columnAdapter.merge(valueBuffers);
        }
    }

    private static final class DictionaryValuesBuffer<T>
            implements NonNullValuesBuffer<T>, NullableValuesBuffer<T>
    {
        private final PrimitiveField field;
        private final DictionaryDecoder<T> dictionaryDecoder;
        private final IntegersBuffer ids;
        private int totalExistingValueCount;
        private int totalNonNullsCount;

        private DictionaryValuesBuffer(PrimitiveField field, DictionaryDecoder<T> dictionaryDecoder)
        {
            this.ids = new IntegersBuffer();
            this.field = field;
            this.dictionaryDecoder = dictionaryDecoder;
        }

        @Override
        public void readNonNullValues(ValueDecoder<T> valueDecoder, int existingValueCount)
        {
            int[] positionsBuffer = new int[existingValueCount];
            dictionaryDecoder.readDictionaryIds(positionsBuffer, 0, existingValueCount);
            ids.add(positionsBuffer);
            totalNonNullsCount += existingValueCount;
            totalExistingValueCount += existingValueCount;
        }

        @Override
        public void readNullableValues(ValueDecoder<T> valueDecoder, boolean[] isNull, int nonNullCount, int existingValueCount)
        {
            // No nulls
            if (nonNullCount > 0 && nonNullCount == existingValueCount) {
                readNonNullValues(valueDecoder, existingValueCount);
                return;
            }

            // Parquet dictionary encodes only non-null values
            // Dictionary size is used as the id to denote nulls for Trino dictionary block
            if (nonNullCount == 0) {
                // Only nulls were encountered in existingValueCount, add empty values for nulls
                int[] dummy = new int[existingValueCount];
                Arrays.fill(dummy, dictionaryDecoder.getDictionarySize());
                ids.add(dummy);
            }
            else {
                // Read data values to a temporary array and unpack the nulls to the actual destination
                int[] tmpBuffer = new int[nonNullCount];
                dictionaryDecoder.readDictionaryIds(tmpBuffer, 0, nonNullCount);
                int[] positionsBuffer = new int[existingValueCount];
                unpackDictionaryNullId(tmpBuffer, positionsBuffer, isNull, 0, existingValueCount, dictionaryDecoder.getDictionarySize());
                ids.add(positionsBuffer);
            }
            totalNonNullsCount += nonNullCount;
            totalExistingValueCount += existingValueCount;
        }

        @Override
        public ColumnChunk createNonNullBlock(int[] definitions, int[] repetitions)
        {
            // This will return a nullable dictionary even if we are returning a batch of non-null values
            // for a nullable column. We avoid creating a new non-nullable dictionary to allow the engine
            // to optimize for the unchanged dictionary case.
            checkState(
                    totalNonNullsCount == totalExistingValueCount,
                    "totalNonNullsCount %s should be equal to totalExistingValueCount %s when creating non-null block",
                    totalNonNullsCount,
                    totalExistingValueCount);
            log.debug("DictionaryValuesBuffer createNonNullBlock field %s, totalNonNullsCount %d, totalExistingValueCount %d", field, totalNonNullsCount, totalExistingValueCount);
            return createDictionaryBlock(ids.getMergedBuffer(), dictionaryDecoder.getDictionaryBlock(), definitions, repetitions);
        }

        @Override
        public ColumnChunk createNullableBlock(BooleansBuffer isNull, int[] definitions, int[] repetitions)
        {
            log.debug("DictionaryValuesBuffer createNullableBlock field %s, totalNonNullsCount %d, totalExistingValueCount %d", field, totalNonNullsCount, totalExistingValueCount);
            if (totalNonNullsCount == 0) {
                return new ColumnChunk(RunLengthEncodedBlock.create(field.getType(), null, totalExistingValueCount), definitions, repetitions);
            }
            return createDictionaryBlock(ids.getMergedBuffer(), dictionaryDecoder.getDictionaryBlock(), definitions, repetitions);
        }
    }

    private static class BooleansBuffer
    {
        private final List<boolean[]> buffers = new ArrayList<>();

        private void add(boolean[] buffer)
        {
            buffers.add(buffer);
        }

        private boolean[] getMergedBuffer()
        {
            if (buffers.size() == 1) {
                return buffers.get(0);
            }
            return Booleans.concat(buffers.toArray(boolean[][]::new));
        }
    }

    private static class IntegersBuffer
    {
        private final List<int[]> buffers = new ArrayList<>();

        private void add(int[] buffer)
        {
            buffers.add(buffer);
        }

        private int[] getMergedBuffer()
        {
            if (buffers.size() == 1) {
                return buffers.get(0);
            }
            return Ints.concat(buffers.toArray(int[][]::new));
        }
    }

    record ValueCount(int rows, int values) {}
}
