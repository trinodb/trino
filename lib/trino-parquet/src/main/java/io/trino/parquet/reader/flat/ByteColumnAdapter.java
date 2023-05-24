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

import com.google.common.primitives.Bytes;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;

public class ByteColumnAdapter
        implements ColumnAdapter<byte[]>
{
    public static final ByteColumnAdapter BYTE_ADAPTER = new ByteColumnAdapter();

    @Override
    public byte[] createBuffer(int size)
    {
        return new byte[size];
    }

    @Override
    public Block createNonNullBlock(byte[] values)
    {
        return new ByteArrayBlock(values.length, Optional.empty(), values);
    }

    @Override
    public Block createNullableBlock(boolean[] nulls, byte[] values)
    {
        return new ByteArrayBlock(values.length, Optional.of(nulls), values);
    }

    @Override
    public void copyValue(byte[] source, int sourceIndex, byte[] destination, int destinationIndex)
    {
        destination[destinationIndex] = source[sourceIndex];
    }

    @Override
    public void decodeDictionaryIds(byte[] values, int offset, int length, int[] ids, byte[] dictionary)
    {
        for (int i = 0; i < length; i++) {
            values[offset + i] = dictionary[ids[i]];
        }
    }

    @Override
    public long getSizeInBytes(byte[] values)
    {
        return sizeOf(values);
    }

    @Override
    public byte[] merge(List<byte[]> buffers)
    {
        return Bytes.concat(buffers.toArray(byte[][]::new));
    }
}
