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

import build.buf.gen.lance.encodings21.OutOfLineBitpacking;
import com.github.luohao.fastlanes.bitpack.VectorShortPacker;
import io.airlift.slice.Slice;
import io.trino.lance.file.v2.reader.BufferAdapter;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;

public class OutOfLineBitpackingEncoding
        implements LanceEncoding
{
    public static final int ELEMENTS_PER_CHUNK = 1024;
    private final int uncompressedBitsPerValue;
    private final int compressedBitsPerValue;
    private final int chunkSize;

    public OutOfLineBitpackingEncoding(int uncompressedBitsPerValue, int compressedBitsPerValue)
    {
        this.uncompressedBitsPerValue = uncompressedBitsPerValue;
        this.compressedBitsPerValue = compressedBitsPerValue;
        this.chunkSize = (ELEMENTS_PER_CHUNK * compressedBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    }

    @Override
    public BufferAdapter getBufferAdapter()
    {
        throw new UnsupportedOperationException("getBufferAdapter not supported for OutOfLineBitpacking");
    }

    @Override
    public <T> MiniBlockDecoder<T> getMiniBlockDecoder()
    {
        throw new UnsupportedOperationException("getMiniBlockDecoder not supported for OutOfLineBitpacking");
    }

    @Override
    public BlockDecoder getBlockDecoder()
    {
        return switch (uncompressedBitsPerValue) {
            case 16 -> new ShortBlockDecoder();
            default -> throw new IllegalStateException("Unexpected value: " + uncompressedBitsPerValue);
        };
    }

    public class ShortBlockDecoder
            implements BlockDecoder<short[]>
    {
        private short[] values;

        @Override
        public void init(Slice slice, int count)
        {
            this.values = new short[count];
            int numFullChunks = count / ELEMENTS_PER_CHUNK;
            int numTailValues = count % ELEMENTS_PER_CHUNK;
            int currentOffset = 0;
            short[] buffer = new short[ELEMENTS_PER_CHUNK];

            for (int chunk = 0; chunk < numFullChunks; chunk++) {
                Slice chunkSlice = slice.slice(chunkSize * chunk, chunkSize);
                VectorShortPacker.unpack(chunkSlice.getShorts(0, chunkSize / Short.BYTES), compressedBitsPerValue, buffer);
                System.arraycopy(buffer, 0, values, currentOffset, ELEMENTS_PER_CHUNK);
                currentOffset += ELEMENTS_PER_CHUNK;
            }
            if (numTailValues > 0) {
                int tailByteOffset = chunkSize * numFullChunks;
                int tailSize = slice.length() - tailByteOffset;
                Slice tailSlice = slice.slice(tailByteOffset, tailSize);
                if (tailSize * Byte.SIZE == numTailValues * uncompressedBitsPerValue) {
                    tailSlice.getShorts(0, values, currentOffset, numTailValues);
                }
                else {
                    checkArgument(tailSize == chunkSize, "tail chunk size must be equal to full chunk size if bitpacked");
                    VectorShortPacker.unpack(tailSlice.getShorts(0, chunkSize / Short.BYTES), compressedBitsPerValue, buffer);
                    System.arraycopy(buffer, 0, values, currentOffset, numTailValues);
                }
            }
        }

        @Override
        public void read(int sourceIndex, short[] destination, int destinationIndex, int length)
        {
            System.arraycopy(values, sourceIndex, destination, destinationIndex, length);
        }
    }

    public static OutOfLineBitpackingEncoding fromProto(OutOfLineBitpacking proto)
    {
        checkArgument(proto.hasValues());
        checkArgument(proto.getValues().hasFlat());
        int uncompressedBitsPerValue = toIntExact(proto.getUncompressedBitsPerValue());
        int compressedBitsPerValue = toIntExact(proto.getValues().getFlat().getBitsPerValue());
        return new OutOfLineBitpackingEncoding(uncompressedBitsPerValue, compressedBitsPerValue);
    }
}
