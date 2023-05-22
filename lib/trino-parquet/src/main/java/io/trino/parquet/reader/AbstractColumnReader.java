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
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.decoders.ValueDecoder;
import io.trino.parquet.reader.flat.ColumnAdapter;
import io.trino.parquet.reader.flat.DictionaryDecoder;
import io.trino.parquet.reader.flat.RowRangesIterator;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.type.AbstractVariableWidthType;
import org.apache.parquet.io.ParquetDecodingException;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.decoders.ValueDecoder.ValueDecodersProvider;
import static io.trino.parquet.reader.flat.DictionaryDecoder.DictionaryDecoderProvider;
import static io.trino.parquet.reader.flat.RowRangesIterator.createRowRangesIterator;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractColumnReader<BufferType>
        implements ColumnReader
{
    private static final Logger log = Logger.get(AbstractColumnReader.class);

    protected final PrimitiveField field;
    protected final ValueDecodersProvider<BufferType> decodersProvider;
    protected final ColumnAdapter<BufferType> columnAdapter;
    private final DictionaryDecoderProvider<BufferType> dictionaryDecoderProvider;

    protected PageReader pageReader;
    protected RowRangesIterator rowRanges;
    @Nullable
    protected DictionaryDecoder<BufferType> dictionaryDecoder;
    private boolean produceDictionaryBlock;

    public AbstractColumnReader(
            PrimitiveField field,
            ValueDecodersProvider<BufferType> decodersProvider,
            DictionaryDecoderProvider<BufferType> dictionaryDecoderProvider,
            ColumnAdapter<BufferType> columnAdapter)
    {
        this.field = requireNonNull(field, "field is null");
        this.decodersProvider = requireNonNull(decodersProvider, "decoders is null");
        this.dictionaryDecoderProvider = requireNonNull(dictionaryDecoderProvider, "dictionaryDecoderProvider is null");
        this.columnAdapter = requireNonNull(columnAdapter, "columnAdapter is null");
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
            log.debug("field %s, readDictionaryPage %s", field, dictionaryPage);
            dictionaryDecoder = dictionaryDecoderProvider.create(dictionaryPage, isNonNull());
            produceDictionaryBlock = shouldProduceDictionaryBlock(rowRanges);
        }
        this.rowRanges = createRowRangesIterator(rowRanges);
    }

    protected abstract boolean isNonNull();

    protected boolean produceDictionaryBlock()
    {
        return produceDictionaryBlock;
    }

    protected ValueDecoder<BufferType> createValueDecoder(ValueDecodersProvider<BufferType> decodersProvider, ParquetEncoding encoding, Slice data)
    {
        ValueDecoder<BufferType> valueDecoder;
        if (encoding == PLAIN_DICTIONARY || encoding == RLE_DICTIONARY) {
            if (dictionaryDecoder == null) {
                throw new ParquetDecodingException(format("Dictionary is missing for %s", field));
            }
            valueDecoder = dictionaryDecoder;
        }
        else {
            valueDecoder = decodersProvider.create(encoding);
        }
        valueDecoder.init(new SimpleSliceInputStream(data));
        return valueDecoder;
    }

    protected static void throwEndOfBatchException(int remainingInBatch)
    {
        throw new ParquetDecodingException(format("Corrupted Parquet file: extra %d values to be consumed when scanning current batch", remainingInBatch));
    }

    protected static void unpackDictionaryNullId(
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

    protected static ColumnChunk createDictionaryBlock(int[] dictionaryIds, Block dictionary, int[] definitions, int[] repetitions)
    {
        int positionsCount = dictionaryIds.length;
        return new ColumnChunk(
                DictionaryBlock.create(positionsCount, dictionary, dictionaryIds),
                definitions,
                repetitions,
                OptionalLong.of(getMaxDictionaryBlockSize(dictionary, positionsCount)));
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

    private static long getMaxDictionaryBlockSize(Block dictionary, long batchSize)
    {
        // An approximate upper bound on size of DictionaryBlock is derived here instead of using
        // DictionaryBlock#getSizeInBytes directly because that method is expensive
        double maxDictionaryFractionUsed = Math.min((double) batchSize / dictionary.getPositionCount(), 1.0);
        return (long) (batchSize * Integer.BYTES + dictionary.getSizeInBytes() * maxDictionaryFractionUsed);
    }
}
