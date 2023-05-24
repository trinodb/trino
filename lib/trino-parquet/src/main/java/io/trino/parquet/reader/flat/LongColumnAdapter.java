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
import io.trino.spi.block.LongArrayBlock;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;

public class LongColumnAdapter
        implements ColumnAdapter<long[]>
{
    public static final LongColumnAdapter LONG_ADAPTER = new LongColumnAdapter();

    @Override
    public long[] createBuffer(int size)
    {
        return new long[size];
    }

    @Override
    public Block createNonNullBlock(long[] values)
    {
        return new LongArrayBlock(values.length, Optional.empty(), values);
    }

    @Override
    public Block createNullableBlock(boolean[] nulls, long[] values)
    {
        return new LongArrayBlock(values.length, Optional.of(nulls), values);
    }

    @Override
    public void copyValue(long[] source, int sourceIndex, long[] destination, int destinationIndex)
    {
        destination[destinationIndex] = source[sourceIndex];
    }

    @Override
    public void decodeDictionaryIds(long[] values, int offset, int length, int[] ids, long[] dictionary)
    {
        for (int i = 0; i < length; i++) {
            values[offset + i] = dictionary[ids[i]];
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
