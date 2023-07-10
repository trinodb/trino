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

import io.trino.parquet.DictionaryPage;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.decoders.RleBitPackingHybridDecoder;
import io.trino.parquet.reader.decoders.ValueDecoder;
import io.trino.spi.block.Block;
import jakarta.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public final class DictionaryDecoder<T>
        implements ValueDecoder<T>
{
    private final T dictionary;
    private final ColumnAdapter<T> columnAdapter;
    private final int dictionarySize;
    private final boolean isNonNull;
    private final long retainedSizeInBytes;

    private ValueDecoder<int[]> dictionaryIdsReader;
    @Nullable
    private Block dictionaryBlock;

    public DictionaryDecoder(T dictionary, ColumnAdapter<T> columnAdapter, int dictionarySize, boolean isNonNull)
    {
        this.columnAdapter = requireNonNull(columnAdapter, "columnAdapter is null");
        this.dictionary = requireNonNull(dictionary, "dictionary is null");
        this.dictionarySize = dictionarySize;
        this.isNonNull = isNonNull;
        this.retainedSizeInBytes = columnAdapter.getSizeInBytes(dictionary);
    }

    @Override
    public void init(SimpleSliceInputStream input)
    {
        int bitWidth = input.readByte();
        dictionaryIdsReader = new RleBitPackingHybridDecoder(bitWidth);
        dictionaryIdsReader.init(input);
    }

    @Override
    public void read(T values, int offset, int length)
    {
        int[] ids = new int[length];
        dictionaryIdsReader.read(ids, 0, length);
        columnAdapter.decodeDictionaryIds(values, offset, length, ids, dictionary);
    }

    @Override
    public void skip(int n)
    {
        dictionaryIdsReader.skip(n);
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    public void readDictionaryIds(int[] ids, int offset, int length)
    {
        dictionaryIdsReader.read(ids, offset, length);
    }

    public Block getDictionaryBlock()
    {
        if (dictionaryBlock == null) {
            if (isNonNull) {
                dictionaryBlock = columnAdapter.createNonNullBlock(dictionary);
            }
            else {
                dictionaryBlock = columnAdapter.createNullableDictionaryBlock(dictionary, dictionarySize);
            }
        }
        // Avoid creation of new Block objects for dictionary, since the engine currently
        // uses identity equality to test if dictionaries are the same
        return dictionaryBlock;
    }

    public int getDictionarySize()
    {
        return dictionarySize;
    }

    public interface DictionaryDecoderProvider<T>
    {
        DictionaryDecoder<T> create(DictionaryPage dictionaryPage, boolean isNonNull);
    }

    public static <BufferType> DictionaryDecoder<BufferType> getDictionaryDecoder(
            DictionaryPage dictionaryPage,
            ColumnAdapter<BufferType> columnAdapter,
            ValueDecoder<BufferType> plainValuesDecoder,
            boolean isNonNull)
    {
        int size = dictionaryPage.getDictionarySize();
        // Extra value is added to the end of the dictionary for nullable columns because
        // parquet dictionary page does not include null but Trino DictionaryBlock's dictionary does
        BufferType dictionary = columnAdapter.createBuffer(size + (isNonNull ? 0 : 1));
        plainValuesDecoder.init(new SimpleSliceInputStream(dictionaryPage.getSlice()));
        plainValuesDecoder.read(dictionary, 0, size);
        return new DictionaryDecoder<>(dictionary, columnAdapter, size, isNonNull);
    }
}
