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

import com.github.luohao.fastlanes.bitpack.VectorBytePacker;
import com.github.luohao.fastlanes.bitpack.VectorIntegerPacker;
import com.github.luohao.fastlanes.bitpack.VectorLongPacker;
import com.github.luohao.fastlanes.bitpack.VectorShortPacker;
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
import static java.lang.Math.toIntExact;

public class InlineBitpackingEncoding
        implements LanceEncoding
{
    public static final int MAX_ELEMENTS_PER_CHUNK = 1024;
    private final int uncompressedBitWidth;

    public InlineBitpackingEncoding(int uncompressedBitWidth)
    {
        this.uncompressedBitWidth = uncompressedBitWidth;
    }

    @Override
    public ValueBlock decodeBlock(Slice slice, int count)
    {
        return decode(slice, count);
    }

    @Override
    public MiniBlockDecoder getMiniBlockDecoder()
    {
        return switch (uncompressedBitWidth) {
            case 8 -> new ByteMiniBlockDecoder();
            case 16 -> new ShortMiniBlockDecoder();
            case 32 -> new IntMiniBlockDecoder();
            case 64 -> new LongMiniBlockDecoder();
            default -> throw new IllegalStateException("Unexpected uncompressedBitWidth: " + uncompressedBitWidth);
        };
    }

    @Override
    public BlockDecoder getBlockDecoder()
    {
        return switch (uncompressedBitWidth) {
            case 8 -> new ByteBlockDecoder();
            case 16 -> new ShortBlockDecoder();
            case 32 -> new IntBlockDecoder();
            case 64 -> new LongBlockDecoder();
            default -> throw new IllegalStateException("Unexpected uncompressedBitWidth: " + uncompressedBitWidth);
        };
    }

    @Override
    public BufferAdapter getBufferAdapter()
    {
        return switch (uncompressedBitWidth) {
            case 8 -> BYTE_ARRAY_BUFFER_ADAPTER;
            case 16 -> SHORT_ARRAY_BUFFER_ADAPTER;
            case 32 -> INT_ARRAY_BUFFER_ADAPTER;
            case 64 -> LONG_ARRAY_BUFFER_ADAPTER;
            default -> throw new IllegalStateException("Unexpected uncompressedBitWidth: " + uncompressedBitWidth);
        };
    }

    public ValueBlock decode(Slice slice, int count)
    {
        return switch (uncompressedBitWidth) {
            case 8 -> {
                int bitWidth = slice.getUnsignedByte(0);
                byte[] input = slice.getBytes(1, MAX_ELEMENTS_PER_CHUNK * bitWidth / uncompressedBitWidth);
                byte[] output = new byte[MAX_ELEMENTS_PER_CHUNK];
                VectorBytePacker.unpack(input, bitWidth, output);
                yield new ByteArrayBlock(count, Optional.empty(), output);
            }
            case 16 -> {
                int bitWidth = slice.getUnsignedShort(0);
                short[] input = slice.getShorts(2, MAX_ELEMENTS_PER_CHUNK * bitWidth / uncompressedBitWidth);
                short[] output = new short[MAX_ELEMENTS_PER_CHUNK];
                VectorShortPacker.unpack(input, bitWidth, output);
                yield new ShortArrayBlock(count, Optional.empty(), output);
            }
            case 32 -> {
                long bitWidth = slice.getUnsignedInt(0);
                int[] input = slice.getInts(4, toIntExact(MAX_ELEMENTS_PER_CHUNK * bitWidth / uncompressedBitWidth));
                int[] output = new int[MAX_ELEMENTS_PER_CHUNK];
                VectorIntegerPacker.unpack(input, toIntExact(bitWidth), output);
                yield new IntArrayBlock(count, Optional.empty(), output);
            }
            case 64 -> {
                long bitWidth = slice.getLong(0);
                long[] input = slice.getLongs(8, toIntExact(MAX_ELEMENTS_PER_CHUNK * bitWidth / uncompressedBitWidth));
                long[] output = new long[MAX_ELEMENTS_PER_CHUNK];
                VectorLongPacker.unpack(input, toIntExact(bitWidth), output);
                yield new LongArrayBlock(count, Optional.empty(), output);
            }
            default -> throw new IllegalStateException("Unexpected uncompressedBitWidth: " + uncompressedBitWidth);
        };
    }

    public int getUncompressedBitWidth()
    {
        return uncompressedBitWidth;
    }

    public class ByteBlockDecoder
            implements BlockDecoder<byte[]>
    {
        private int numValues;
        private final byte[] data = new byte[MAX_ELEMENTS_PER_CHUNK];

        @Override
        public void init(Slice slice, int numValues)
        {
            this.numValues = numValues;
            int bitWidth = slice.getUnsignedByte(0);
            VectorBytePacker.unpack(slice.getBytes(1, MAX_ELEMENTS_PER_CHUNK * bitWidth / uncompressedBitWidth), bitWidth, data);
        }

        @Override
        public void read(int sourceIndex, byte[] destination, int destinationIndex, int length)
        {
            checkArgument(sourceIndex + length <= numValues);
            System.arraycopy(data, sourceIndex, destination, destinationIndex, length);
        }
    }

    public class ShortBlockDecoder
            implements BlockDecoder<short[]>
    {
        private int numValues;
        private final short[] data = new short[MAX_ELEMENTS_PER_CHUNK];

        @Override
        public void init(Slice slice, int numValues)
        {
            this.numValues = numValues;
            int bitWidth = slice.getUnsignedByte(0);
            VectorShortPacker.unpack(slice.getShorts(2, MAX_ELEMENTS_PER_CHUNK * bitWidth / uncompressedBitWidth), bitWidth, data);
        }

        @Override
        public void read(int sourceIndex, short[] destination, int destinationIndex, int length)
        {
            checkArgument(sourceIndex + length <= numValues);
            System.arraycopy(data, sourceIndex, destination, destinationIndex, length);
        }
    }

    public class IntBlockDecoder
            implements BlockDecoder<int[]>
    {
        private int numValues;
        private final int[] data = new int[MAX_ELEMENTS_PER_CHUNK];

        @Override
        public void init(Slice slice, int numValues)
        {
            this.numValues = numValues;
            int bitWidth = slice.getUnsignedByte(0);
            VectorIntegerPacker.unpack(slice.getInts(4, MAX_ELEMENTS_PER_CHUNK * bitWidth / uncompressedBitWidth), bitWidth, data);
        }

        @Override
        public void read(int sourceIndex, int[] destination, int destinationIndex, int length)
        {
            checkArgument(sourceIndex + length <= numValues);
            System.arraycopy(data, sourceIndex, destination, destinationIndex, length);
        }
    }

    public class LongBlockDecoder
            implements BlockDecoder<long[]>
    {
        private int numValues;
        private final long[] data = new long[MAX_ELEMENTS_PER_CHUNK];

        @Override
        public void init(Slice slice, int numValues)
        {
            this.numValues = numValues;
            int bitWidth = slice.getUnsignedByte(0);
            VectorLongPacker.unpack(slice.getLongs(8, MAX_ELEMENTS_PER_CHUNK * bitWidth / uncompressedBitWidth), bitWidth, data);
        }

        @Override
        public void read(int sourceIndex, long[] destination, int destinationIndex, int length)
        {
            checkArgument(sourceIndex + length <= numValues);
            System.arraycopy(data, sourceIndex, destination, destinationIndex, length);
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
            checkArgument(numValues <= MAX_ELEMENTS_PER_CHUNK);
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
            checkArgument(numValues <= MAX_ELEMENTS_PER_CHUNK);
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
            checkArgument(numValues <= MAX_ELEMENTS_PER_CHUNK);
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
            checkArgument(numValues <= MAX_ELEMENTS_PER_CHUNK);
            blockDecoder.init(slices.getFirst(), numValues);
        }

        @Override
        public void read(int sourceIndex, long[] destination, int destinationIndex, int length)
        {
            blockDecoder.read(sourceIndex, destination, destinationIndex, length);
        }
    }
}
