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
import io.trino.spi.block.Block;
import io.trino.spi.block.Fixed12Block;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;

public class Fixed12ColumnAdapter
        implements ColumnAdapter<int[]>
{
    public static final Fixed12ColumnAdapter FIXED12_ADAPTER = new Fixed12ColumnAdapter();

    @Override
    public int[] createBuffer(int size)
    {
        return new int[size * 3];
    }

    @Override
    public Block createNonNullBlock(int[] values)
    {
        return new Fixed12Block(values.length / 3, Optional.empty(), values);
    }

    @Override
    public Block createNullableBlock(boolean[] nulls, int[] values)
    {
        return new Fixed12Block(values.length / 3, Optional.of(nulls), values);
    }

    @Override
    public void copyValue(int[] source, int sourceIndex, int[] destination, int destinationIndex)
    {
        destination[destinationIndex * 3] = source[sourceIndex * 3];
        destination[(destinationIndex * 3) + 1] = source[(sourceIndex * 3) + 1];
        destination[(destinationIndex * 3) + 2] = source[(sourceIndex * 3) + 2];
    }

    @Override
    public void decodeDictionaryIds(int[] values, int offset, int length, int[] ids, int[] dictionary)
    {
        for (int i = 0; i < length; i++) {
            int id = 3 * ids[i];
            int destinationIndex = 3 * (offset + i);
            values[destinationIndex] = dictionary[id];
            values[destinationIndex + 1] = dictionary[id + 1];
            values[destinationIndex + 2] = dictionary[id + 2];
        }
    }

    @Override
    public long getSizeInBytes(int[] values)
    {
        return sizeOf(values);
    }

    @Override
    public int[] merge(List<int[]> buffers)
    {
        return Ints.concat(buffers.toArray(int[][]::new));
    }
}
