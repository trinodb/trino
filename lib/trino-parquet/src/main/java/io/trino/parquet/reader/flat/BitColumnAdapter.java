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

import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.Block;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.copyBits;
import static io.trino.spi.block.Bitmap.expandBits;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.set;
import static java.lang.Math.toIntExact;

public class BitColumnAdapter
        implements ColumnAdapter<BitBuffer>
{
    public static final BitColumnAdapter BIT_ADAPTER = new BitColumnAdapter();

    @Override
    public BitBuffer createBuffer(int size)
    {
        return new BitBuffer(size);
    }

    @Override
    public Block createNonNullBlock(BitBuffer values)
    {
        return new BitArrayBlock(values.getSize(), Optional.empty(), values.getValues());
    }

    @Override
    public Block createNullableBlock(long[] valueIsValid, BitBuffer values)
    {
        return new BitArrayBlock(values.getSize(), Optional.of(valueIsValid), values.getValues());
    }

    @Override
    public void copyValue(BitBuffer source, int sourceIndex, BitBuffer destination, int destinationIndex)
    {
        if (isSet(source.getValues(), 0, sourceIndex)) {
            set(destination.getValues(), 0, destinationIndex);
        }
        else {
            clear(destination.getValues(), 0, destinationIndex);
        }
    }

    @Override
    public void copyValues(BitBuffer source, int sourceIndex, BitBuffer destination, int destinationIndex, int length)
    {
        copyBits(source.getValues(), sourceIndex, destination.getValues(), destinationIndex, length);
    }

    @Override
    public void unpackNullValues(BitBuffer source, BitBuffer destination, long[] valueIsValid, int destOffset, int nonNullCount, int totalValuesCount)
    {
        // Bit buffers are zero-initialized, so an all-null range needs no value writes.
        if (nonNullCount == 0) {
            return;
        }
        expandBits(source.getValues(), 0, valueIsValid, destOffset, destination.getValues(), destOffset, totalValuesCount);
    }

    @Override
    public void decodeDictionaryIds(BitBuffer values, int offset, int length, int[] ids, BitBuffer dictionary)
    {
        for (int i = 0; i < length; i++) {
            if (isSet(dictionary.getValues(), 0, ids[i])) {
                set(values.getValues(), 0, offset + i);
            }
            else {
                clear(values.getValues(), 0, offset + i);
            }
        }
    }

    @Override
    public long getSizeInBytes(BitBuffer values)
    {
        return sizeOf(values.getValues());
    }

    @Override
    public BitBuffer merge(List<BitBuffer> buffers)
    {
        long resultSize = 0;
        for (BitBuffer buffer : buffers) {
            resultSize += buffer.getSize();
        }
        BitBuffer result = new BitBuffer(toIntExact(resultSize));
        int offset = 0;
        for (BitBuffer buffer : buffers) {
            copyBits(buffer.getValues(), 0, result.getValues(), offset, buffer.getSize());
            offset += buffer.getSize();
        }
        return result;
    }
}
