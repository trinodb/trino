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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.trino.spi.block.Block;
import io.trino.spi.block.Int96ArrayBlock;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;

public class Int96ColumnAdapter
        implements ColumnAdapter<Int96ColumnAdapter.Int96Buffer>
{
    public static final Int96ColumnAdapter INT96_ADAPTER = new Int96ColumnAdapter();

    @Override
    public Int96Buffer createBuffer(int size)
    {
        return new Int96Buffer(size);
    }

    @Override
    public void copyValue(Int96Buffer source, int sourceIndex, Int96Buffer destination, int destinationIndex)
    {
        destination.longs[destinationIndex] = source.longs[sourceIndex];
        destination.ints[destinationIndex] = source.ints[sourceIndex];
    }

    @Override
    public Block createNullableBlock(boolean[] nulls, Int96Buffer values)
    {
        return new Int96ArrayBlock(values.size(), Optional.of(nulls), values.longs, values.ints);
    }

    @Override
    public Block createNonNullBlock(Int96Buffer values)
    {
        return new Int96ArrayBlock(values.size(), Optional.empty(), values.longs, values.ints);
    }

    @Override
    public void decodeDictionaryIds(Int96Buffer values, int offset, int length, int[] ids, Int96Buffer dictionary)
    {
        for (int i = 0; i < length; i++) {
            values.longs[offset + i] = dictionary.longs[ids[i]];
            values.ints[offset + i] = dictionary.ints[ids[i]];
        }
    }

    @Override
    public long getSizeInBytes(Int96Buffer values)
    {
        return sizeOf(values.longs) + sizeOf(values.ints);
    }

    @Override
    public Int96Buffer merge(List<Int96Buffer> buffers)
    {
        return new Int96Buffer(
                Longs.concat(buffers.stream()
                        .map(buffer -> buffer.longs)
                        .toArray(long[][]::new)),
                Ints.concat(buffers.stream()
                        .map(buffer -> buffer.ints)
                        .toArray(int[][]::new)));
    }

    public static class Int96Buffer
    {
        public final long[] longs;
        public final int[] ints;

        public Int96Buffer(int size)
        {
            this(new long[size], new int[size]);
        }

        private Int96Buffer(long[] longs, int[] ints)
        {
            checkArgument(
                    longs.length == ints.length,
                    "Length of longs %s does not match length of ints %s",
                    longs.length,
                    ints.length);
            this.longs = longs;
            this.ints = ints;
        }

        public int size()
        {
            return longs.length;
        }
    }
}
