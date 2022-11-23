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
import io.trino.spi.block.RunLengthEncodedBlock;
import org.apache.parquet.io.ParquetDecodingException;

import javax.annotation.Nullable;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.decoders.ValueDecoder.ValueDecodersProvider;
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
    private PageReader pageReader;
    private RowRangesIterator rowRanges;

    private int readOffset;
    private int remainingPageValueCount;
    @Nullable
    private DictionaryDecoder<BufferType> dictionaryDecoder;
    private FlatDefinitionLevelDecoder definitionLevelDecoder;
    private ValueDecoder<BufferType> valueDecoder;

    private int nextBatchSize;

    public FlatColumnReader(PrimitiveField field, ValueDecodersProvider<BufferType> decodersProvider, ColumnAdapter<BufferType> columnAdapter)
    {
        this.field = requireNonNull(field, "field is null");
        this.decodersProvider = requireNonNull(decodersProvider, "decoders is null");
        this.columnAdapter = requireNonNull(columnAdapter, "columnAdapter is null");
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
        BufferType values = columnAdapter.createBuffer(nextBatchSize);
        boolean[] isNull = new boolean[nextBatchSize];

        int totalNonNullCount = 0;
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
            totalNonNullCount += nonNullCount;

            // Only nulls
            if (nonNullCount == 0) {
                // Unpack empty null table. This is almost always a no-op. However, in binary type
                // the last position offset needs to be propagated
                BufferType tmpBuffer = columnAdapter.createTemporaryBuffer(offset, 0, values);
                columnAdapter.unpackNullValues(tmpBuffer, values, isNull, offset, 0, chunkSize);
            }
            // No nulls
            else if (nonNullCount == chunkSize) {
                valueDecoder.read(values, offset, nonNullCount);
            }
            else {
                // Read to a temporary array and unpack the nulls to the actual destination
                BufferType tmpBuffer = columnAdapter.createTemporaryBuffer(offset, nonNullCount, values);
                valueDecoder.read(tmpBuffer, 0, nonNullCount);
                columnAdapter.unpackNullValues(tmpBuffer, values, isNull, offset, nonNullCount, chunkSize);
            }

            offset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingPageValueCount -= chunkSize;
        }

        if (totalNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, nextBatchSize);
            return new ColumnChunk(block, EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }

        boolean hasNoNulls = totalNonNullCount == nextBatchSize;
        Block block;
        if (hasNoNulls) {
            block = columnAdapter.createNonNullBlock(values);
        }
        else {
            block = columnAdapter.createNullableBlock(isNull, values);
        }
        return new ColumnChunk(block, EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
    }

    @VisibleForTesting
    ColumnChunk readNoNull()
    {
        BufferType values = columnAdapter.createBuffer(nextBatchSize);
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

            valueDecoder.read(values, offset, chunkSize);
            offset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingPageValueCount -= chunkSize;
        }

        Block block = columnAdapter.createNonNullBlock(values);
        return new ColumnChunk(block, EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
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
            int size = dictionaryPage.getDictionarySize();
            BufferType dictionary = columnAdapter.createBuffer(size);
            ValueDecoder<BufferType> plainValuesDecoder = decodersProvider.create(PLAIN, field);
            plainValuesDecoder.init(new SimpleSliceInputStream(dictionaryPage.getSlice()));
            plainValuesDecoder.read(dictionary, 0, size);
            dictionaryDecoder = new DictionaryDecoder<>(dictionary, columnAdapter);
        }
        checkArgument(pageReader.getTotalValueCount() > 0, "page is empty");
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
}
