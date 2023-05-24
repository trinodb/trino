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

import com.google.common.primitives.Longs;
import io.trino.spi.block.Block;
import io.trino.spi.block.Int128ArrayBlock;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;

public class Int128ColumnAdapter
        implements ColumnAdapter<long[]>
{
    public static final Int128ColumnAdapter INT128_ADAPTER = new Int128ColumnAdapter();

    @Override
    public long[] createBuffer(int size)
    {
        return new long[size * 2];
    }

    @Override
    public Block createNonNullBlock(long[] values)
    {
        return new Int128ArrayBlock(values.length / 2, Optional.empty(), values);
    }

    @Override
    public Block createNullableBlock(boolean[] nulls, long[] values)
    {
        return new Int128ArrayBlock(values.length / 2, Optional.of(nulls), values);
    }

    @Override
    public void copyValue(long[] source, int sourceIndex, long[] destination, int destinationIndex)
    {
        destination[destinationIndex * 2] = source[sourceIndex * 2];
        destination[(destinationIndex * 2) + 1] = source[(sourceIndex * 2) + 1];
    }

    @Override
    public void decodeDictionaryIds(long[] values, int offset, int length, int[] ids, long[] dictionary)
    {
        for (int i = 0; i < length; i++) {
            int id = 2 * ids[i];
            int destinationIndex = 2 * (offset + i);
            values[destinationIndex] = dictionary[id];
            values[destinationIndex + 1] = dictionary[id + 1];
        }
    }

    @Override
    public long getSizeInBytes(long[] values)
    {
        return sizeOf(values);
    }

    @Override
    public long[] merge(List<long[]> buffers)
    {
        return Longs.concat(buffers.toArray(long[][]::new));
    }
}
