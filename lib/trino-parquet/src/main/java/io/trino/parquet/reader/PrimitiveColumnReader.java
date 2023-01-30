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

import io.airlift.slice.Slice;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.ParquetTypeUtils;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.dictionary.Dictionary;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.parquet.ParquetReaderUtils.toInputStream;
import static io.trino.parquet.ValuesType.DEFINITION_LEVEL;
import static io.trino.parquet.ValuesType.REPETITION_LEVEL;
import static io.trino.parquet.ValuesType.VALUES;
import static java.util.Objects.requireNonNull;

public abstract class PrimitiveColumnReader
        implements ColumnReader
{
    private static final int EMPTY_LEVEL_VALUE = -1;
    protected final PrimitiveField field;

    protected int definitionLevel = EMPTY_LEVEL_VALUE;
    protected int repetitionLevel = EMPTY_LEVEL_VALUE;
    protected ValuesReader valuesReader;

    private int nextBatchSize;
    private LevelReader repetitionReader;
    private LevelReader definitionReader;
    private PageReader pageReader;
    private Dictionary dictionary;
    private DataPage page;
    private int remainingValueCountInPage;
    private int readOffset;
    @Nullable
    private PrimitiveIterator.OfLong indexIterator;
    private long currentRow;
    private long targetRow;

    protected abstract void readValue(BlockBuilder blockBuilder, Type type);

    private void skipSingleValue()
    {
        if (definitionLevel == field.getDescriptor().getMaxDefinitionLevel()) {
            valuesReader.skip();
        }
    }

    protected boolean isValueNull()
    {
        return ParquetTypeUtils.isValueNull(field.isRequired(), definitionLevel, field.getDefinitionLevel());
    }

    public PrimitiveColumnReader(PrimitiveField field)
    {
        this.field = requireNonNull(field, "columnDescriptor");
        pageReader = null;
        this.targetRow = 0;
        this.indexIterator = null;
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
        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();

        if (dictionaryPage != null) {
            try {
                dictionary = dictionaryPage.getEncoding().initDictionary(field.getDescriptor(), dictionaryPage);
            }
            catch (IOException e) {
                throw new ParquetDecodingException("could not decode the dictionary for " + field.getDescriptor(), e);
            }
        }
        else {
            dictionary = null;
        }
        if (rowRanges.isPresent()) {
            indexIterator = rowRanges.get().getParquetRowRanges().iterator();
            // If rowRanges is empty for a row-group, then no page needs to be read, and we should not reach here
            checkArgument(indexIterator.hasNext(), "rowRanges is empty");
            targetRow = indexIterator.next();
        }
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset = readOffset + nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public ColumnChunk readPrimitive()
    {
        // Pre-allocate these arrays to the necessary size. This saves a substantial amount of
        // CPU time by avoiding container resizing.
        IntList definitionLevels = new IntArrayList(nextBatchSize);
        IntList repetitionLevels = new IntArrayList(nextBatchSize);
        seek();
        BlockBuilder blockBuilder = field.getType().createBlockBuilder(null, nextBatchSize);
        int valueCount = 0;
        while (valueCount < nextBatchSize) {
            if (page == null) {
                readNextPage();
            }
            int valuesToRead = Math.min(remainingValueCountInPage, nextBatchSize - valueCount);
            if (valuesToRead == 0) {
                // When we break here, we could end up with valueCount < nextBatchSize, but that is OK.
                break;
            }
            readValues(blockBuilder, valuesToRead, field.getType(), definitionLevels, repetitionLevels);
            valueCount += valuesToRead;
        }

        readOffset = 0;
        nextBatchSize = 0;
        return new ColumnChunk(blockBuilder.build(), definitionLevels.toIntArray(), repetitionLevels.toIntArray());
    }

    private void readValues(BlockBuilder blockBuilder, int valuesToRead, Type type, IntList definitionLevels, IntList repetitionLevels)
    {
        processValues(valuesToRead, () -> {
            if (definitionLevel == field.getDefinitionLevel()) {
                readValue(blockBuilder, type);
            }
            else if (isValueNull()) {
                blockBuilder.appendNull();
            }
            definitionLevels.add(definitionLevel);
            repetitionLevels.add(repetitionLevel);
        });
    }

    private void skipValues(long valuesToRead)
    {
        processValues(valuesToRead, this::skipSingleValue);
    }

    /**
     * When filtering using column indexes we might skip reading some pages for different columns. Because the rows are
     * not aligned between the pages of the different columns it might be required to skip some values. The values (and the
     * related rl and dl) are skipped based on the iterator of the required row indexes and the first row index of each
     * page.
     * For example:
     *
     * <pre>
     * rows   col1   col2   col3
     *      ┌──────┬──────┬──────┐
     *   0  │  p0  │      │      │
     *      ╞══════╡  p0  │  p0  │
     *  20  │ p1(X)│------│------│
     *      ╞══════╪══════╡      │
     *  40  │ p2(X)│      │------│
     *      ╞══════╡ p1(X)╞══════╡
     *  60  │ p3(X)│      │------│
     *      ╞══════╪══════╡      │
     *  80  │  p4  │      │  p1  │
     *      ╞══════╡  p2  │      │
     * 100  │  p5  │      │      │
     *      └──────┴──────┴──────┘
     * </pre>
     * <p>
     * The pages 1, 2, 3 in col1 are skipped so we have to skip the rows [20, 79]. Because page 1 in col2 contains values
     * only for the rows [40, 79] we skip this entire page as well. To synchronize the row reading we have to skip the
     * values (and the related rl and dl) for the rows [20, 39] in the end of the page 0 for col2. Similarly, we have to
     * skip values while reading page0 and page1 for col3.
     */
    private void processValues(long valuesToRead, Runnable valueReader)
    {
        if (definitionLevel == EMPTY_LEVEL_VALUE && repetitionLevel == EMPTY_LEVEL_VALUE) {
            definitionLevel = definitionReader.readLevel();
            repetitionLevel = repetitionReader.readLevel();
        }
        int valueCount = 0;
        int skipCount = 0;
        for (int i = 0; i < valuesToRead; ) {
            boolean consumed;
            do {
                if (incrementRowAndTestIfTargetReached(repetitionLevel)) {
                    valueReader.run();
                    valueCount++;
                    consumed = true;
                }
                else {
                    skipSingleValue();
                    skipCount++;
                    consumed = false;
                }

                if (valueCount + skipCount == remainingValueCountInPage) {
                    updateValueCounts(valueCount, skipCount);
                    if (!readNextPage()) {
                        return;
                    }
                    valueCount = 0;
                    skipCount = 0;
                }

                repetitionLevel = repetitionReader.readLevel();
                definitionLevel = definitionReader.readLevel();
            }
            while (repetitionLevel != 0);

            if (consumed) {
                i++;
            }
        }
        updateValueCounts(valueCount, skipCount);
    }

    private void seek()
    {
        if (readOffset == 0) {
            return;
        }
        int readOffset = this.readOffset;
        int valuePosition = 0;
        while (valuePosition < readOffset) {
            if (page == null) {
                if (!readNextPage()) {
                    break;
                }
            }
            int offset = Math.min(remainingValueCountInPage, readOffset - valuePosition);
            skipValues(offset);
            valuePosition = valuePosition + offset;
        }
        checkArgument(valuePosition == readOffset, "valuePosition %s must be equal to readOffset %s", valuePosition, readOffset);
    }

    private boolean readNextPage()
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

    private void updateValueCounts(int valuesRead, int skipCount)
    {
        int totalCount = valuesRead + skipCount;
        if (totalCount == remainingValueCountInPage) {
            page = null;
            valuesReader = null;
        }
        remainingValueCountInPage -= totalCount;
    }

    private ValuesReader readPageV1(DataPageV1 page)
    {
        ValuesReader rlReader = page.getRepetitionLevelEncoding().getValuesReader(field.getDescriptor(), REPETITION_LEVEL);
        ValuesReader dlReader = page.getDefinitionLevelEncoding().getValuesReader(field.getDescriptor(), DEFINITION_LEVEL);
        repetitionReader = new LevelValuesReader(rlReader);
        definitionReader = new LevelValuesReader(dlReader);
        try {
            ByteBufferInputStream in = toInputStream(page.getSlice());
            rlReader.initFromPage(page.getValueCount(), in);
            dlReader.initFromPage(page.getValueCount(), in);
            return initDataReader(page.getValueEncoding(), page.getValueCount(), in, page.getFirstRowIndex());
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page " + page + " in column " + field.getDescriptor(), e);
        }
    }

    private ValuesReader readPageV2(DataPageV2 page)
    {
        repetitionReader = buildLevelRLEReader(field.getDescriptor().getMaxRepetitionLevel(), page.getRepetitionLevels());
        definitionReader = buildLevelRLEReader(field.getDescriptor().getMaxDefinitionLevel(), page.getDefinitionLevels());
        return initDataReader(page.getDataEncoding(), page.getValueCount(), toInputStream(page.getSlice()), page.getFirstRowIndex());
    }

    private LevelReader buildLevelRLEReader(int maxLevel, Slice slice)
    {
        if (maxLevel == 0) {
            return new LevelNullReader();
        }
        return new LevelRLEReader(new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), slice.getInput()));
    }

    private ValuesReader initDataReader(ParquetEncoding dataEncoding, int valueCount, ByteBufferInputStream in, OptionalLong firstRowIndex)
    {
        ValuesReader valuesReader;
        if (dataEncoding.usesDictionary()) {
            if (dictionary == null) {
                throw new ParquetDecodingException("Dictionary is missing for Page");
            }
            valuesReader = dataEncoding.getDictionaryBasedValuesReader(field.getDescriptor(), VALUES, dictionary);
        }
        else {
            valuesReader = dataEncoding.getValuesReader(field.getDescriptor(), VALUES);
        }

        try {
            valuesReader.initFromPage(valueCount, in);
            if (firstRowIndex.isPresent()) {
                currentRow = firstRowIndex.getAsLong();
            }
            return valuesReader;
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page in column " + field.getDescriptor(), e);
        }
    }

    // Increment currentRow and return true if at or after targetRow
    private boolean incrementRowAndTestIfTargetReached(int repetitionLevel)
    {
        if (indexIterator == null) {
            return true;
        }

        if (repetitionLevel == 0) {
            if (currentRow > targetRow) {
                targetRow = indexIterator.hasNext() ? indexIterator.next() : Long.MAX_VALUE;
            }
            boolean isAtTargetRow = currentRow == targetRow;
            currentRow++;
            return isAtTargetRow;
        }

        // currentRow was incremented at repetitionLevel 0
        return currentRow - 1 == targetRow;
    }
}
