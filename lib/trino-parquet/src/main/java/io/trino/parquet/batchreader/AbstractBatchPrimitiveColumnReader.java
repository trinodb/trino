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
package io.trino.parquet.batchreader;

import io.airlift.slice.Slice;
import io.trino.parquet.ColumnReader;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.dictionary.Dictionary;
import io.trino.parquet.reader.ColumnChunk;
import io.trino.parquet.reader.LevelNullReader;
import io.trino.parquet.reader.LevelRLEReader;
import io.trino.parquet.reader.LevelReader;
import io.trino.parquet.reader.LevelValuesReader;
import io.trino.parquet.reader.PageReader;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.parquet.ParquetReaderUtils.toInputStream;
import static io.trino.parquet.ValuesType.DEFINITION_LEVEL;
import static io.trino.parquet.ValuesType.REPETITION_LEVEL;
import static io.trino.parquet.ValuesType.VALUES;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractBatchPrimitiveColumnReader
        implements ColumnReader
{
    protected static final int EMPTY_LEVEL_VALUE = -1;
    protected ColumnDescriptor columnDescriptor;

    protected ValuesReader valuesReader;

    protected int nextBatchSize;
    protected LevelReader repetitionReader;
    protected LevelReader definitionReader;
    protected long totalValueCount;
    protected PageReader pageReader;
    protected RowRanges rowRanges;
    protected Dictionary dictionary;
    protected DataPage page;
    protected int remainingValueCountInPage;
    protected int readOffset;
    protected long currentRow;
    protected RowRangeIterator rowRangeIterator;

    /* Utility methods */

    protected static final int[] emptyIntArray = new int[0];

    protected int[] definitionLevels;
    protected int[] definitionLevelCounts;

    // Definition levels array can be reused. This ensures it's large enough for the current batch
    protected void allocateDefinitionLevels(int batchSize)
    {
        if (definitionLevels == null || definitionLevels.length < batchSize) {
            definitionLevels = new int[batchSize];
        }
    }

    // Make a table of how many steps to go to pass a run of consecutive nulls or nonnulls
    protected void countDefinitionSteps(int batchSize, int maxDef)
    {
        if (batchSize < 1) {
            return;
        }

        if (definitionLevelCounts == null || definitionLevelCounts.length < batchSize) {
            definitionLevelCounts = new int[batchSize];
        }

        int i = batchSize - 1;
        definitionLevelCounts[i] = 1;
        boolean lastNull = definitionLevels[i] != maxDef;

        i--;
        while (i >= 0) {
            boolean isNull = definitionLevels[i] != maxDef;
            if (isNull == lastNull) {
                definitionLevelCounts[i] = definitionLevelCounts[i + 1] + 1;
            }
            else {
                definitionLevelCounts[i] = 1;
            }
            lastNull = isNull;
            i--;
        }
    }

    protected boolean isIntegerType(Type type)
    {
        return type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT);
    }

    // Counts how many nulls or nonnulls are indicated by the definition levels after the provided starting index
    // When a level != the max definition level, that indicates a null value in the column.
//    protected static int countLevels(int[] defLevels, int maxDef, int start, int endExclusive, boolean countNull)
//    {
//        int count = 0;
//        while (start < endExclusive && (defLevels[start] != maxDef) == countNull) {
//            start++;
//            count++;
//        }
//        return count;
//    }

    protected static int countNonnulls(int[] defLevels, int maxDef, int start, int endExclusive)
    {
        int count = 0;
        while (start < endExclusive) {
            if (defLevels[start] == maxDef) {
                count++;
            }
            start++;
        }
        return count;
    }

    protected static int countNonnulls(int[] defLevels, int maxDef, int startDef, int endDefExcl, boolean[] isNull, int nullStart)
    {
        int count = 0;
        while (startDef < endDefExcl) {
            if (defLevels[startDef] == maxDef) {
                count++;
            }
            else {
                isNull[nullStart] = true; // Assumes array defaults to all false
            }
            startDef++;
            nullStart++;
        }
        return count;
    }

    // Convert int array to short array
    protected static void copyIntToShort(int[] src, short[] dst)
    {
        for (int i = 0; i < src.length; i++) {
            int value = src[i];
            if (value > Short.MAX_VALUE) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d exceeds MAX_SHORT", value));
            }
            if (value < Short.MIN_VALUE) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d is less than MIN_SHORT", value));
            }
            dst[i] = (short) value;
        }
    }

    // Convert int array to byte array
    protected static void copyIntToByte(int[] src, byte[] dst)
    {
        for (int i = 0; i < src.length; i++) {
            int value = src[i];
            if (value > Byte.MAX_VALUE) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d exceeds MAX_BYTE", value));
            }
            if (value < Byte.MIN_VALUE) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d is less than MIN_BYTE", value));
            }
            dst[i] = (byte) value;
        }
    }

    // Convert long array to short array
    protected static void copyLongToShort(long[] src, short[] dst)
    {
        for (int i = 0; i < src.length; i++) {
            long value = src[i];
            if (value > Short.MAX_VALUE) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d exceeds MAX_SHORT", value));
            }
            if (value < Short.MIN_VALUE) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d is less than MIN_SHORT", value));
            }
            dst[i] = (short) value;
        }
    }

    // Convert long array to byte array
    protected static void copyLongToByte(long[] src, byte[] dst)
    {
        for (int i = 0; i < src.length; i++) {
            long value = src[i];
            if (value > Byte.MAX_VALUE) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d exceeds MAX_BYTE", value));
            }
            if (value < Byte.MIN_VALUE) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d is less than MIN_BYTE", value));
            }
            dst[i] = (byte) value;
        }
    }

    // Convert long array to int array
    protected static void copyLongToInt(long[] src, int[] dst)
    {
        for (int i = 0; i < src.length; i++) {
            long value = src[i];
            if (value > Integer.MAX_VALUE) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d exceeds MAX_INT", value));
            }
            if (value < Integer.MIN_VALUE) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d is less than MIN_INT", value));
            }
            dst[i] = (int) value;
        }
    }

    // Convert long array to int array
    protected static void copyIntToLong(int[] src, long[] dst)
    {
        for (int i = 0; i < src.length; i++) {
            dst[i] = src[i];
        }
    }

    /* Page reading methods */

    @Override
    public PageReader getPageReader()
    {
        return pageReader;
    }

    @Override
    public void setPageReader(PageReader pageReader, RowRanges rowRanges)
    {
        this.pageReader = requireNonNull(pageReader, "pageReader");
        this.rowRanges = rowRanges;
        this.rowRangeIterator = null;

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        if (dictionaryPage != null) {
            try {
                dictionary = dictionaryPage.getEncoding().initDictionary(columnDescriptor, dictionaryPage);
            }
            catch (IOException e) {
                throw new ParquetDecodingException("could not decode the dictionary for " + columnDescriptor, e);
            }
        }
        else {
            dictionary = null;
        }
        checkArgument(pageReader.getTotalValueCount() > 0, "page is empty");
        totalValueCount = pageReader.getTotalValueCount();

        if (rowRanges != null) {
            rowRangeIterator = new RowRangeIterator(rowRanges);
            // If rowRanges is empty for a row-group, then no page needs to be read, and we should not reach here
            checkArgument(!rowRangeIterator.terminated(), "rowRanges is empty");
        }
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset = readOffset + nextBatchSize;
        nextBatchSize = batchSize;
    }

    protected boolean checkValidPage()
    {
        if (remainingValueCountInPage == 0) {
            page = null;
            valuesReader = null;
            if (!readNextPage()) {
                return false;
            }
        }
        return true;
    }

    protected boolean readNextPage()
    {
        verify(page == null, "readNextPage has to be called when page is null");
        page = pageReader.readPage();
        if (page == null) {
            // we have read all pages
            return false;
        }
        remainingValueCountInPage = page.getValueCount();
        if (page instanceof DataPageV1) {
            valuesReader = readPageV1((DataPageV1) page);
        }
        else {
            valuesReader = readPageV2((DataPageV2) page);
        }
        return true;
    }

    private ValuesReader readPageV1(DataPageV1 page)
    {
        ValuesReader rlReader = page.getRepetitionLevelEncoding().getValuesReader(columnDescriptor, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDefinitionLevelEncoding().getValuesReader(columnDescriptor, DEFINITION_LEVEL);
        repetitionReader = new LevelValuesReader(rlReader);
        definitionReader = new LevelValuesReader(dlReader);
        try {
            ByteBufferInputStream in = toInputStream(page.getSlice());
            rlReader.initFromPage(page.getValueCount(), in);
            dlReader.initFromPage(page.getValueCount(), in);
            return initDataReader(page.getValueEncoding(), page.getValueCount(), in, page.getFirstRowIndex());
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page " + page + " in column " + columnDescriptor, e);
        }
    }

    private ValuesReader readPageV2(DataPageV2 page)
    {
        repetitionReader = buildLevelRLEReader(columnDescriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        definitionReader = buildLevelRLEReader(columnDescriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        return initDataReader(page.getDataEncoding(), page.getValueCount(), toInputStream(page.getSlice()), page.getFirstRowIndex());
    }

    private LevelReader buildLevelRLEReader(int maxLevel, Slice slice)
    {
        if (maxLevel == 0) {
            return new LevelNullReader();
        }
        return new LevelRLEReader(new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), ByteBufferInputStream.wrap(slice.toByteBuffer())));
    }

    private ValuesReader initDataReader(ParquetEncoding dataEncoding, int valueCount, ByteBufferInputStream in, OptionalLong firstRowIndex)
    {
        ValuesReader valuesReader;
        if (dataEncoding.usesDictionary()) {
            if (dictionary == null) {
                throw new ParquetDecodingException("Dictionary is missing for Page");
            }
            valuesReader = dataEncoding.getDictionaryBasedValuesReader(columnDescriptor, VALUES, dictionary);
        }
        else {
            valuesReader = dataEncoding.getValuesReader(columnDescriptor, VALUES);
        }

        try {
            valuesReader.initFromPage(valueCount, in);
            if (firstRowIndex.isPresent()) {
                currentRow = firstRowIndex.getAsLong();
            }
            return valuesReader;
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page in column " + columnDescriptor, e);
        }
    }

    /* Seek methods */

    // Seek to current read offset
    protected void seek(Field field)
    {
        if (readOffset == 0) {
            return;
        }

        boolean hasRanges = rowRangeIterator != null;
        boolean hasNulls = !field.isRequired();

        if (hasRanges) {
            if (hasNulls) {
                seekGeneric();
            }
            else {
                seekNonullsRanges();
            }
        }
        else {
            if (hasNulls) {
                seekNullsNoranges();
            }
            else {
                seekNonullsNoranges();
            }
        }
    }

    // Generic seek to current read offset; handles nulls and optional ranges
    private void seekGeneric()
    {
        allocateDefinitionLevels(readOffset);
        int remainingInBatch = readOffset;
        int definitionIndex = 0;
        int definitionEnd = 0;
        int maxDef = columnDescriptor.getMaxDefinitionLevel();

        outerloop:
        while (remainingInBatch > 0) {
            if (!checkValidPage()) {
                break;
            }

            if (definitionIndex == definitionEnd) {
                int readLevels = Math.min(remainingValueCountInPage, remainingInBatch);
                definitionReader.readLevels(definitionLevels, 0, readLevels);
                countDefinitionSteps(readLevels, maxDef);
                definitionEnd = readLevels;
                definitionIndex = 0;
            }

            int skipCount = 0;
            while (definitionIndex < definitionEnd) {
                int stepCount;

                // Compute how many nulls or non-nulls
                boolean doingNulls = definitionLevels[definitionIndex] != maxDef;
                int valueNullCount = definitionLevelCounts[definitionIndex];

                // Compute how many skips or run length
                boolean doingSkip;
                if (rowRangeIterator != null) {
                    RowRangeIterator.RowRange range = rowRangeIterator.getCurrentRange();
                    if (range.terminal()) {
                        break outerloop;
                    }

                    long targetRow = range.start();
                    doingSkip = currentRow < targetRow;
                    if (doingSkip) {
                        stepCount = (int) Math.min(targetRow - currentRow, valueNullCount);
                    }
                    else {
                        stepCount = (int) Math.min(range.count(), valueNullCount);
                    }
                }
                else {
                    doingSkip = false;
                    stepCount = valueNullCount;
                }

                if (!doingNulls) {
                    skipCount += stepCount;
                }

                if (!doingSkip) {
                    if (rowRangeIterator != null) {
                        rowRangeIterator.advance(stepCount);
                    }

                    remainingInBatch -= stepCount;
                }

                currentRow += stepCount;
                remainingValueCountInPage -= stepCount;
                definitionIndex += stepCount;
            }

            valuesReader.skip(skipCount);
        }

        checkArgument(remainingInBatch == 0, "remainingInBatch %s must be equal to 0", remainingInBatch);
    }

    // Seek to current read offset; handles nulls, no support for ranges
    private void seekNullsNoranges()
    {
        allocateDefinitionLevels(readOffset);
        int remainingInBatch = readOffset;
        int maxDef = columnDescriptor.getMaxDefinitionLevel();

        while (remainingInBatch > 0) {
            if (!checkValidPage()) {
                break;
            }

            int levelCount = Math.min(remainingValueCountInPage, remainingInBatch);
            definitionReader.readLevels(definitionLevels, 0, levelCount);
            int skipCount = countNonnulls(definitionLevels, maxDef, 0, levelCount);
            valuesReader.skip(skipCount);
            remainingInBatch -= levelCount;
            remainingValueCountInPage -= levelCount;
        }

        checkArgument(remainingInBatch == 0, "remainingInBatch %s must be equal to 0", remainingInBatch);
    }

    // Generic seek to current read offset; no nulls, requires ranges
    private void seekNonullsRanges()
    {
        int remainingInBatch = readOffset;

        outerloop:
        while (remainingInBatch > 0) {
            if (!checkValidPage()) {
                break;
            }

            int skipCount = 0;
            while (remainingInBatch > 0 && remainingValueCountInPage > 0) {
                RowRangeIterator.RowRange range = rowRangeIterator.getCurrentRange();
                if (range.terminal()) {
                    break outerloop;
                }

                long targetRow = range.start();
                boolean doingSkip = currentRow < targetRow;

                int stepCount;
                int countLimit = Math.min(remainingValueCountInPage, remainingInBatch);
                if (doingSkip) {
                    stepCount = (int) Math.min(targetRow - currentRow, countLimit);
                }
                else {
                    stepCount = (int) Math.min(range.count(), countLimit);
                    rowRangeIterator.advance(stepCount);
                    remainingInBatch -= stepCount;
                }

                skipCount += stepCount;
                currentRow += stepCount;
                remainingValueCountInPage -= stepCount;
            }

            valuesReader.skip(skipCount);
        }

        checkArgument(remainingInBatch == 0, "remainingInBatch %s must be equal to 0", remainingInBatch);
    }

    // Seek to current read offset; handles nulls, no support for ranges
    private void seekNonullsNoranges()
    {
        int remainingInBatch = readOffset;

        while (remainingInBatch > 0) {
            if (!checkValidPage()) {
                break;
            }

            int skipCount = Math.min(remainingValueCountInPage, remainingInBatch);
            valuesReader.skip(skipCount);
            remainingInBatch -= skipCount;
            remainingValueCountInPage -= skipCount;
        }

        checkArgument(remainingInBatch == 0, "remainingInBatch %s must be equal to 0", remainingInBatch);
    }

    /* Read methods */

    @Override
    public ColumnChunk readPrimitive(Field field)
    {
        ColumnChunk columnChunk = null;
        seek(field);

        boolean hasRanges = rowRangeIterator != null;
        boolean hasNulls = !field.isRequired();

        allocateValuesArray(nextBatchSize);

        if (hasRanges) {
            if (hasNulls) {
                columnChunk = readGeneric(field);
            }
            else {
                columnChunk = readNonullsRanges(field);
            }
        }
        else {
            if (hasNulls) {
                columnChunk = readNullsNoranges(field);
            }
            else {
                columnChunk = readNonullsNoranges(field);
            }
        }

        readOffset = 0;
        nextBatchSize = 0;
        return columnChunk;
    }

    // Read a column batch. Handles both null values and optionally ranges
    private ColumnChunk readGeneric(Field field)
    {
        allocateDefinitionLevels(nextBatchSize);
        boolean[] isNull = new boolean[nextBatchSize];

        int remainingInBatch = nextBatchSize;
        int valuePosition = 0;
        int totalNonNullCount = 0;
        int definitionIndex = 0;
        int definitionEnd = 0;
        int maxDef = columnDescriptor.getMaxDefinitionLevel();

        while (remainingInBatch > 0) {
            if (!checkValidPage()) {
                break;
            }

            if (definitionIndex == definitionEnd) {
                int readLevels = Math.min(remainingValueCountInPage, remainingInBatch);
                definitionReader.readLevels(definitionLevels, 0, readLevels);
                countDefinitionSteps(readLevels, maxDef);
                definitionEnd = readLevels;
                definitionIndex = 0;
            }

            // Compute how many nulls or non-nulls
            boolean doingNulls = definitionLevels[definitionIndex] != maxDef;
            int valueNullCount = definitionLevelCounts[definitionIndex];

            // Compute how many skips or run length
            boolean doingSkip;
            int stepCount;
            if (rowRangeIterator != null) {
                RowRangeIterator.RowRange range = rowRangeIterator.getCurrentRange();
                if (range.terminal()) {
                    break;
                }
                long targetRow = range.start();
                doingSkip = currentRow < targetRow;
                if (doingSkip) {
                    stepCount = (int) Math.min(targetRow - currentRow, valueNullCount);
                }
                else {
                    stepCount = (int) Math.min(range.count(), valueNullCount);
                }
            }
            else {
                doingSkip = false;
                stepCount = valueNullCount;
            }

            if (doingSkip) {
                if (!doingNulls) {
                    valuesReader.skip(stepCount);
                }
            }
            else {
                if (!doingNulls) {
                    readValues(valuePosition, stepCount);
                    valuePosition += stepCount;
                    totalNonNullCount += stepCount;
                }
                else {
                    int s = valuePosition;
                    int e = valuePosition + stepCount;
                    for (int i = s; i < e; i++) {
                        isNull[i] = true;
                    }
                    valuePosition = e;
                }

                if (rowRangeIterator != null) {
                    rowRangeIterator.advance(stepCount);
                }

                remainingInBatch -= stepCount;
            }

            currentRow += stepCount;
            remainingValueCountInPage -= stepCount;
            definitionIndex += stepCount;
        }

        checkArgument(valuePosition == nextBatchSize, "valuePosition %s must be equal to nextBatchSize %s", valuePosition, nextBatchSize);

        return makeColumnChunk(field, totalNonNullCount, nextBatchSize, isNull);
    }

    // Read a column batch, handles nulls, does not support ranges
    private ColumnChunk readNullsNoranges(Field field)
    {
        allocateDefinitionLevels(nextBatchSize);
        boolean[] isNull = new boolean[nextBatchSize];

        int remainingInBatch = nextBatchSize;
        int valuePosition = 0;
        int totalNonNullCount = 0;
        int maxDef = columnDescriptor.getMaxDefinitionLevel();

        while (remainingInBatch > 0) {
            if (!checkValidPage()) {
                break;
            }

            int chunkSize = Math.min(remainingValueCountInPage, remainingInBatch);
            definitionReader.readLevels(definitionLevels, 0, chunkSize);
            int nonnullCount = countNonnulls(definitionLevels, maxDef, 0, chunkSize, isNull, valuePosition);

            if (nonnullCount > 0) {
                totalNonNullCount += nonnullCount;

                // Read all values in chunk consecutively
                readValues(valuePosition, nonnullCount);

                // Relocate values to correct positions where isNull[i] is false
                relocateNonnulls(valuePosition, chunkSize, nonnullCount, isNull);
            }

            valuePosition += chunkSize;
            remainingInBatch -= chunkSize;
            remainingValueCountInPage -= chunkSize;
        }

        checkArgument(valuePosition == nextBatchSize, "valuePosition %s must be equal to nextBatchSize %s", valuePosition, nextBatchSize);

        return makeColumnChunk(field, totalNonNullCount, nextBatchSize, isNull);
    }

    // Read a column batch, does not support nulls, requires ranges
    private ColumnChunk readNonullsRanges(Field field)
    {
        int remainingInBatch = nextBatchSize;
        int valuePosition = 0;

        while (remainingInBatch > 0) {
            if (!checkValidPage()) {
                break;
            }

            RowRangeIterator.RowRange range = rowRangeIterator.getCurrentRange();
            if (range.terminal()) {
                break;
            }

            // Compute how many skips or run length
            long targetRow = range.start();
            boolean doingSkip = currentRow < targetRow;

            int stepCount;
            int countLimit = Math.min(remainingValueCountInPage, remainingInBatch);
            if (doingSkip) {
                stepCount = (int) Math.min(targetRow - currentRow, countLimit);
                valuesReader.skip(stepCount);
            }
            else {
                stepCount = (int) Math.min(range.count(), countLimit);
                readValues(valuePosition, stepCount);
                valuePosition += stepCount;
                rowRangeIterator.advance(stepCount);
                remainingInBatch -= stepCount;
            }

            currentRow += stepCount;
            remainingValueCountInPage -= stepCount;
        }

        checkArgument(valuePosition == nextBatchSize, "valuePosition %s must be equal to nextBatchSize %s", valuePosition, nextBatchSize);

        return makeColumnChunk(field, nextBatchSize, nextBatchSize, null);
    }

    // Read a column batch, does not support nulls or ranges
    private ColumnChunk readNonullsNoranges(Field field)
    {
        int remainingInBatch = nextBatchSize;
        int valuePosition = 0;

        while (remainingInBatch > 0) {
            if (!checkValidPage()) {
                break;
            }

            int chunkSize = Math.min(remainingInBatch, remainingValueCountInPage);
            readValues(valuePosition, chunkSize);
            valuePosition += chunkSize;
            remainingValueCountInPage -= chunkSize;
            remainingInBatch -= chunkSize;
        }

        checkArgument(valuePosition == nextBatchSize, "valuePosition %s must be equal to nextBatchSize %s", valuePosition, nextBatchSize);

        return makeColumnChunk(field, nextBatchSize, nextBatchSize, null);
    }

    /* Abstract methods to be provided by implementation */

    protected abstract void allocateValuesArray(int batchSize);

    protected abstract void readValues(int valuePosition, int chunkSize);

    protected abstract ColumnChunk makeColumnChunk(Field field, int totalNonNullCount, int batchSize, boolean[] isNull);

    protected abstract void relocateNonnulls(int valuePosition, int chunkSize, int nonnullCount, boolean[] isNull);
}
