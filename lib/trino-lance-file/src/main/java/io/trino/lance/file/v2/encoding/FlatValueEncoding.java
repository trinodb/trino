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
package io.trino.lance.file.v2.encoding;

import io.airlift.slice.Slice;
import io.trino.lance.file.v2.reader.BufferAdapter;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.ValueBlock;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.lance.file.v2.reader.ByteArrayBufferAdapter.BYTE_ARRAY_BUFFER_ADAPTER;
import static io.trino.lance.file.v2.reader.IntArrayBufferAdapter.INT_ARRAY_BUFFER_ADAPTER;
import static io.trino.lance.file.v2.reader.LongArrayBufferAdapter.LONG_ARRAY_BUFFER_ADAPTER;
import static io.trino.lance.file.v2.reader.ShortArrayBufferAdapter.SHORT_ARRAY_BUFFER_ADAPTER;

public class FlatValueEncoding
        implements LanceEncoding
{
    private final int bytesPerValue;

    public FlatValueEncoding(int bytesPerValue)
    {
        checkArgument(bytesPerValue > 0, "bytesPerValue must be greater than 0");
        this.bytesPerValue = bytesPerValue;
    }

    public int getBytesPerValue()
    {
        return bytesPerValue;
    }

    @Override
    public ValueBlock decodeBlock(Slice slice, int count)
    {
        return decode(slice, count);
    }

    @Override
    public BufferAdapter getBufferAdapter()
    {
        return switch (bytesPerValue) {
            case 1 -> BYTE_ARRAY_BUFFER_ADAPTER;
            case 2 -> SHORT_ARRAY_BUFFER_ADAPTER;
            case 4 -> INT_ARRAY_BUFFER_ADAPTER;
            case 8 -> LONG_ARRAY_BUFFER_ADAPTER;
            default -> throw new IllegalArgumentException("Invalid bytesPerValue: " + bytesPerValue);
        };
    }

    @Override
    public MiniBlockDecoder getMiniBlockDecoder()
    {
        return switch (bytesPerValue) {
            case 1 -> new ByteMiniBlockDecoder();
            case 2 -> new ShortMiniBlockDecoder();
            case 4 -> new IntMiniBlockDecoder();
            case 8 -> new LongMiniBlockDecoder();
            default -> throw new IllegalArgumentException("Invalid bytesPerValue: " + bytesPerValue);
        };
    }

    @Override
    public BlockDecoder getBlockDecoder()
    {
        return switch (bytesPerValue) {
            case 1 -> new ByteBlockDecoder();
            case 2 -> new ShortBlockDecoder();
            case 4 -> new IntBlockDecoder();
            case 8 -> new LongBlockDecoder();
            default -> throw new IllegalArgumentException("Invalid bytesPerValue: " + bytesPerValue);
        };
    }

    private ValueBlock decode(Slice slice, int count)
    {
        return switch (bytesPerValue) {
            case 1 -> new ByteArrayBlock(count, Optional.empty(), slice.getBytes(0, count));
            case 2 -> new ShortArrayBlock(count, Optional.empty(), slice.getShorts(0, count));
            case 4 -> new IntArrayBlock(count, Optional.empty(), slice.getInts(0, count));
            case 8 -> new LongArrayBlock(count, Optional.empty(), slice.getLongs(0, count));
            default -> throw new IllegalArgumentException("Invalid bytesPerValue: " + bytesPerValue);
        };
    }

    public class ByteBlockDecoder
            implements BlockDecoder<byte[]>
    {
        private Slice slice;
        private int numValues;

        @Override
        public void init(Slice slice, int numValues)
        {
            checkArgument(bytesPerValue == 1);
            this.slice = slice;
            this.numValues = numValues;
        }

        @Override
        public void read(int sourceIndex, byte[] destination, int destinationIndex, int length)
        {
            checkArgument(sourceIndex + length <= numValues);
            slice.getBytes(sourceIndex, destination, destinationIndex, length);
        }
    }

    public class ShortBlockDecoder
            implements BlockDecoder<short[]>
    {
        private Slice slice;
        private int numValues;

        @Override
        public void init(Slice slice, int numValues)
        {
            checkArgument(bytesPerValue == 2);
            this.slice = slice;
            this.numValues = numValues;
        }

        @Override
        public void read(int sourceIndex, short[] destination, int destinationIndex, int length)
        {
            checkArgument(sourceIndex + length <= numValues);
            slice.getShorts(sourceIndex * Short.BYTES, destination, destinationIndex, length);
        }
    }

    public class IntBlockDecoder
            implements BlockDecoder<int[]>
    {
        private Slice slice;
        private int numValues;

        @Override
        public void init(Slice slice, int numValues)
        {
            checkArgument(bytesPerValue == 4);
            this.slice = slice;
            this.numValues = numValues;
        }

        @Override
        public void read(int sourceIndex, int[] destination, int destinationIndex, int length)
        {
            checkArgument(sourceIndex + length <= numValues);
            slice.getInts(sourceIndex * Integer.BYTES, destination, destinationIndex, length);
        }
    }

    public class LongBlockDecoder
            implements BlockDecoder<long[]>
    {
        private Slice slice;
        private int numValues;

        @Override
        public void init(Slice slice, int numValues)
        {
            checkArgument(bytesPerValue == 8);
            this.slice = slice;
            this.numValues = numValues;
        }

        @Override
        public void read(int sourceIndex, long[] destination, int destinationIndex, int length)
        {
            checkArgument(sourceIndex + length <= numValues);
            slice.getLongs(sourceIndex * Long.BYTES, destination, destinationIndex, length);
        }
    }

    public class ByteMiniBlockDecoder
            implements MiniBlockDecoder<byte[]>
    {
        private final ByteBlockDecoder blockDecoder = new ByteBlockDecoder();

        @Override
        public void init(List<Slice> slices, int numValues)
        {
            checkArgument(slices.size() == 1);
            blockDecoder.init(slices.getFirst(), numValues);
        }

        @Override
        public void read(int sourceIndex, byte[] destination, int destinationIndex, int length)
        {
            blockDecoder.read(sourceIndex, destination, destinationIndex, length);
        }
    }

    public class ShortMiniBlockDecoder
            implements MiniBlockDecoder<short[]>
    {
        private final ShortBlockDecoder blockDecoder = new ShortBlockDecoder();

        @Override
        public void init(List<Slice> slices, int numValues)
        {
            checkArgument(slices.size() == 1);
            blockDecoder.init(slices.getFirst(), numValues);
        }

        @Override
        public void read(int sourceIndex, short[] destination, int destinationIndex, int length)
        {
            blockDecoder.read(sourceIndex, destination, destinationIndex, length);
        }
    }

    public class IntMiniBlockDecoder
            implements MiniBlockDecoder<int[]>
    {
        private final IntBlockDecoder blockDecoder = new IntBlockDecoder();

        @Override
        public void init(List<Slice> slices, int numValues)
        {
            checkArgument(slices.size() == 1);
            blockDecoder.init(slices.getFirst(), numValues);
        }

        @Override
        public void read(int sourceIndex, int[] destination, int destinationIndex, int length)
        {
            blockDecoder.read(sourceIndex, destination, destinationIndex, length);
        }
    }

    public class LongMiniBlockDecoder
            implements MiniBlockDecoder<long[]>
    {
        private final LongBlockDecoder blockDecoder = new LongBlockDecoder();

        @Override
        public void init(List<Slice> slices, int numValues)
        {
            checkArgument(slices.size() == 1);
            blockDecoder.init(slices.getFirst(), numValues);
        }

        @Override
        public void read(int sourceIndex, long[] destination, int destinationIndex, int length)
        {
            blockDecoder.read(sourceIndex, destination, destinationIndex, length);
        }
    }
}
