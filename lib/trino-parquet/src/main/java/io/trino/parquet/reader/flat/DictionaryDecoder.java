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

import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.decoders.RleBitPackingHybridDecoder;
import io.trino.parquet.reader.decoders.ValueDecoder;

import static java.util.Objects.requireNonNull;

public final class DictionaryDecoder<T>
        implements ValueDecoder<T>
{
    private final T dictionary;
    private final ColumnAdapter<T> columnAdapter;

    private ValueDecoder<int[]> dictionaryIdsReader;

    public DictionaryDecoder(T dictionary, ColumnAdapter<T> columnAdapter)
    {
        this.columnAdapter = requireNonNull(columnAdapter, "columnAdapter is null");
        this.dictionary = requireNonNull(dictionary, "dictionary is null");
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
}
