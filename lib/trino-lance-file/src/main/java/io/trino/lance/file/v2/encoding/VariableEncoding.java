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
import io.trino.lance.file.v2.reader.BinaryBuffer;
import io.trino.lance.file.v2.reader.BufferAdapter;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.lance.file.v2.reader.BinaryBufferAdapter.VARIABLE_BINARY_BUFFER_ADAPTER;
import static java.lang.Math.toIntExact;

public class VariableEncoding
        implements LanceEncoding
{
    @Override
    public ValueBlock decodeBlock(Slice slice, int count)
    {
        // caller verifies count is smaller than the number of values in the buffer
        return decode(slice, count);
    }

    @Override
    public MiniBlockDecoder getMiniBlockDecoder()
    {
        return new VariableBinaryDecoder();
    }

    @Override
    public BufferAdapter getBufferAdapter()
    {
        return VARIABLE_BINARY_BUFFER_ADAPTER;
    }

    private ValueBlock decode(Slice slice, int count)
    {
        if (count == 0) {
            return new VariableWidthBlock(0, EMPTY_SLICE, new int[1], Optional.empty());
        }

        int bitsPerOffset = toIntExact(slice.getUnsignedInt(0));
        if (bitsPerOffset == 32) {
            int offsetStart = 8;
            long bytesStartOffset = slice.getUnsignedInt(4);
            int[] offsets = slice.getInts(offsetStart, count + 1);
            Slice data = slice.slice(toIntExact(bytesStartOffset), toIntExact(slice.length() - bytesStartOffset));
            return new VariableWidthBlock(count, data, offsets, Optional.empty());
        }
        else {
            throw new UnsupportedOperationException("Unsupported bits per offset: " + bitsPerOffset);
        }
    }

    public static class VariableBinaryDecoder
            implements MiniBlockDecoder<BinaryBuffer>
    {
        private Slice slice;
        private int[] offsets;

        @Override
        public void init(List<Slice> slices, int numValues)
        {
            checkArgument(slices.size() == 1);
            slice = slices.get(0);
            offsets = slice.getInts(0, numValues + 1);
        }

        @Override
        public void read(int sourceIndex, BinaryBuffer destination, int destinationIndex, int length)
        {
            for (int i = 0; i < length; i++) {
                destination.add(slice.slice(offsets[sourceIndex + i], offsets[sourceIndex + i + 1] - offsets[sourceIndex + i]), destinationIndex + i);
            }
        }
    }
}
