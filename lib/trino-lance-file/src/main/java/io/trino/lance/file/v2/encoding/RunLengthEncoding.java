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

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.lance.file.v2.reader.ByteArrayBufferAdapter.BYTE_ARRAY_BUFFER_ADAPTER;
import static io.trino.lance.file.v2.reader.IntArrayBufferAdapter.INT_ARRAY_BUFFER_ADAPTER;
import static io.trino.lance.file.v2.reader.LongArrayBufferAdapter.LONG_ARRAY_BUFFER_ADAPTER;
import static io.trino.lance.file.v2.reader.ShortArrayBufferAdapter.SHORT_ARRAY_BUFFER_ADAPTER;
import static java.lang.Math.toIntExact;

public class RunLengthEncoding
        implements LanceEncoding
{
    private final int bitsPerValue;

    public RunLengthEncoding(int bitsPerValue)
    {
        this.bitsPerValue = bitsPerValue;
    }

    @Override
    public BufferAdapter getBufferAdapter()
    {
        return switch (bitsPerValue) {
            case 8 -> BYTE_ARRAY_BUFFER_ADAPTER;
            case 16 -> SHORT_ARRAY_BUFFER_ADAPTER;
            case 32 -> INT_ARRAY_BUFFER_ADAPTER;
            case 64 -> LONG_ARRAY_BUFFER_ADAPTER;
            default -> throw new IllegalStateException("Unexpected uncompressedBitWidth: " + bitsPerValue);
        };
    }

    @Override
    public MiniBlockDecoder getMiniBlockDecoder()
    {
        return switch (bitsPerValue) {
            case 8 -> new ByteRunLengthDecoder(bitsPerValue);
            case 16 -> new ShortRunLengthDecoder(bitsPerValue);
            case 32 -> new IntegerRunLengthDecoder(bitsPerValue);
            case 64 -> new LongRunLengthDecoder(bitsPerValue);
            default -> throw new IllegalStateException("Unexpected bitsPerValue: " + bitsPerValue);
        };
    }

    public static RunLengthEncoding from(build.buf.gen.lance.encodings21.Rle proto)
    {
        checkArgument(proto.getValues().hasFlat(), "value buffer only supports flat encoding");
        checkArgument(proto.getRunLengths().hasFlat(), "length buffer only supports flat encoding");
        build.buf.gen.lance.encodings21.Flat valueEncoding = proto.getValues().getFlat();
        return new RunLengthEncoding(toIntExact(valueEncoding.getBitsPerValue()));
    }

    public abstract static class RunLengthDecoder<T>
            implements MiniBlockDecoder<T>
    {
        private final int bitsPerValue;
        protected Slice valueSlice;
        protected Slice lengthSlice;

        protected RunLengthDecoder(int bitsPerValue)
        {
            this.bitsPerValue = bitsPerValue;
        }

        @Override
        public void init(List<Slice> slices, int numValues)
        {
            checkArgument(slices.size() == 2, "RLE miniblock has exact 2 buffers");
            checkArgument(slices.get(0).length() / (bitsPerValue / Byte.SIZE) == slices.get(1).length(), "values buffer and length buffer do not match");
            this.valueSlice = slices.get(0);
            this.lengthSlice = slices.get(1);
        }

        @Override
        public void read(int sourceIndex, T destination, int destinationIndex, int length)
        {
            int remainingSkip = sourceIndex;
            int runOffset = 0;
            int runLength = lengthSlice.getUnsignedByte(runOffset);
            while (remainingSkip > 0) {
                if (remainingSkip >= runLength) {
                    remainingSkip -= runLength;
                    runOffset++;
                    runLength = lengthSlice.getUnsignedByte(runOffset);
                }
                else {
                    runLength -= remainingSkip;
                    remainingSkip = 0;
                }
            }
            int destinationPosition = destinationIndex;
            int remaining = length;

            while (remaining > 0) {
                if (runLength == 0) {
                    runOffset++;
                    runLength = lengthSlice.getUnsignedByte(runOffset);
                }

                int n = Math.min(runLength, remaining);
                fill(runOffset, destination, destinationPosition, n);

                runLength -= n;
                remaining -= n;
                destinationPosition += n;
            }
        }

        protected abstract void fill(int runOffset, T destination, int offset, int length);
    }

    public static class ByteRunLengthDecoder
            extends RunLengthDecoder<byte[]>
    {
        protected ByteRunLengthDecoder(int bitsPerValue)
        {
            super(bitsPerValue);
        }

        @Override
        protected void fill(int runOffset, byte[] destination, int offset, int length)
        {
            Arrays.fill(destination, offset, offset + length, valueSlice.getByte(runOffset));
        }
    }

    public static class ShortRunLengthDecoder
            extends RunLengthDecoder<short[]>
    {
        protected ShortRunLengthDecoder(int bitsPerValue)
        {
            super(bitsPerValue);
        }

        @Override
        protected void fill(int runOffset, short[] destination, int offset, int length)
        {
            Arrays.fill(destination, offset, offset + length, valueSlice.getShort(runOffset * Short.BYTES));
        }
    }

    public static class IntegerRunLengthDecoder
            extends RunLengthDecoder<int[]>
    {
        protected IntegerRunLengthDecoder(int bitsPerValue)
        {
            super(bitsPerValue);
        }

        @Override
        protected void fill(int runOffset, int[] destination, int offset, int length)
        {
            Arrays.fill(destination, offset, offset + length, valueSlice.getInt(runOffset * Integer.BYTES));
        }
    }

    public static class LongRunLengthDecoder
            extends RunLengthDecoder<long[]>
    {
        protected LongRunLengthDecoder(int bitsPerValue)
        {
            super(bitsPerValue);
        }

        @Override
        protected void fill(int runOffset, long[] destination, int offset, int length)
        {
            Arrays.fill(destination, offset, offset + length, valueSlice.getLong(runOffset * Long.BYTES));
        }
    }
}
