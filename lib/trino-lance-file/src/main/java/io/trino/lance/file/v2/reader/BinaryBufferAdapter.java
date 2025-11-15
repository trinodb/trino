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
package io.trino.lance.file.v2.reader;

import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;

import java.util.List;
import java.util.Optional;

public class BinaryBufferAdapter
        implements BufferAdapter<BinaryBuffer>
{
    public static final BinaryBufferAdapter VARIABLE_BINARY_BUFFER_ADAPTER = new BinaryBufferAdapter();

    @Override
    public BinaryBuffer createBuffer(int size)
    {
        return new BinaryBuffer(size);
    }

    @Override
    public void copy(BinaryBuffer source, int sourceIndex, BinaryBuffer destination, int destinationIndex, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BinaryBuffer merge(List<BinaryBuffer> buffers)
    {
        if (buffers.isEmpty()) {
            return new BinaryBuffer(0);
        }

        int valueCount = 0;
        for (BinaryBuffer binaryBuffer : buffers) {
            valueCount += binaryBuffer.getValueCount();
        }
        BinaryBuffer result = new BinaryBuffer(valueCount);
        for (BinaryBuffer binaryBuffer : buffers) {
            result.addChunk(binaryBuffer.asSlice());
        }
        int[] resultOffsets = result.getOffsets();
        int[] firstOffsets = buffers.get(0).getOffsets();
        System.arraycopy(firstOffsets, 0, resultOffsets, 0, firstOffsets.length);

        int dataOffset = firstOffsets[firstOffsets.length - 1];
        int outputArrayOffset = firstOffsets.length;
        for (int i = 1; i < buffers.size(); i++) {
            int[] currentOffsets = buffers.get(i).getOffsets();
            for (int j = 1; j < currentOffsets.length; j++) {
                resultOffsets[outputArrayOffset + j - 1] = dataOffset + currentOffsets[j];
            }
            outputArrayOffset += currentOffsets.length - 1;
            dataOffset = resultOffsets[outputArrayOffset - 1];
        }

        return result;
    }

    @Override
    public Block createBlock(BinaryBuffer buffer, Optional<boolean[]> valueIsNull)
    {
        return new VariableWidthBlock(buffer.getValueCount(), buffer.getSlice(), buffer.getOffsets(), valueIsNull);
    }

    @Override
    public long getRetainedBytes(BinaryBuffer buffer)
    {
        return buffer.getRetainedSize();
    }
}
