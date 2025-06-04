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

import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;

public class IntColumnAdapter
        implements ColumnAdapter<int[]>
{
    public static final IntColumnAdapter INT_ADAPTER = new IntColumnAdapter();

    @Override
    public int[] createBuffer(int size)
    {
        return new int[size];
    }

    @Override
    public Block createNonNullBlock(int[] values)
    {
        return new IntArrayBlock(values.length, Optional.empty(), values);
    }

    @Override
    public Block createNullableBlock(boolean[] nulls, int[] values)
    {
        return new IntArrayBlock(values.length, Optional.of(nulls), values);
    }

    @Override
    public void copyValue(int[] source, int sourceIndex, int[] destination, int destinationIndex)
    {
        destination[destinationIndex] = source[sourceIndex];
    }

    @Override
    public void decodeDictionaryIds(int[] values, int offset, int length, int[] ids, int[] dictionary)
    {
        for (int i = 0; i < length; i++) {
            values[offset + i] = dictionary[ids[i]];
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
        return concatIntArrays(buffers);
    }

    static int[] concatIntArrays(List<int[]> buffers)
    {
        long resultSize = 0;
        for (int[] buffer : buffers) {
            resultSize += buffer.length;
        }
        int[] result = new int[toIntExact(resultSize)];
        int offset = 0;
        for (int[] buffer : buffers) {
            System.arraycopy(buffer, 0, result, offset, buffer.length);
            offset += buffer.length;
        }
        return result;
    }
}
