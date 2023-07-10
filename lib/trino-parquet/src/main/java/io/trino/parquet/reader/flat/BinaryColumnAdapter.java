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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetReaderUtils.castToByteNegate;

public class BinaryColumnAdapter
        implements ColumnAdapter<BinaryBuffer>
{
    public static final BinaryColumnAdapter BINARY_ADAPTER = new BinaryColumnAdapter();

    @Override
    public BinaryBuffer createBuffer(int batchSize)
    {
        return new BinaryBuffer(batchSize);
    }

    @Override
    public BinaryBuffer createTemporaryBuffer(int currentOffset, int size, BinaryBuffer buffer)
    {
        return buffer.withTemporaryOffsets(currentOffset, size);
    }

    @Override
    public void copyValue(BinaryBuffer source, int sourceIndex, BinaryBuffer destination, int destinationIndex)
    {
        // ignore as unpackNullValues is overridden
        throw new UnsupportedOperationException();
    }

    @Override
    public Block createNullableBlock(boolean[] nulls, BinaryBuffer values)
    {
        return new VariableWidthBlock(values.getValueCount(), values.asSlice(), values.getOffsets(), Optional.of(nulls));
    }

    @Override
    public Block createNullableDictionaryBlock(BinaryBuffer dictionary, int nonNullsCount)
    {
        checkArgument(
                dictionary.getValueCount() == nonNullsCount + 1,
                "Dictionary buffer size %s did not match the expected value of %s",
                dictionary.getValueCount(),
                nonNullsCount + 1);
        boolean[] nulls = new boolean[nonNullsCount + 1];
        nulls[nonNullsCount] = true;
        // Overwrite the next after last position with an empty value. This will be used as null.
        int[] offsets = dictionary.getOffsets();
        offsets[nonNullsCount + 1] = offsets[nonNullsCount];
        return new VariableWidthBlock(dictionary.getValueCount(), dictionary.asSlice(), offsets, Optional.of(nulls));
    }

    @Override
    public Block createNonNullBlock(BinaryBuffer values)
    {
        return new VariableWidthBlock(values.getValueCount(), values.asSlice(), values.getOffsets(), Optional.empty());
    }

    @Override
    public void unpackNullValues(BinaryBuffer sourceBuffer, BinaryBuffer destinationBuffer, boolean[] isNull, int destOffset, int nonNullCount, int totalValuesCount)
    {
        int endOffset = destOffset + totalValuesCount;
        int srcOffset = 0;
        int[] destination = destinationBuffer.getOffsets();
        int[] source = sourceBuffer.getOffsets();

        while (srcOffset < nonNullCount) {
            destination[destOffset] = source[srcOffset];
            srcOffset += castToByteNegate(isNull[destOffset]);
            destOffset++;
        }
        // The last+1 offset is always a sentinel value equal to last offset + last position length.
        // In case of null values at the end, the last offset value needs to be repeated for every null position
        while (destOffset <= endOffset) {
            destination[destOffset++] = source[nonNullCount];
        }
    }

    @Override
    public void decodeDictionaryIds(BinaryBuffer values, int offset, int length, int[] ids, BinaryBuffer dictionary)
    {
        Slice dictionarySlice = dictionary.asSlice();
        int[] outputOffsets = values.getOffsets();
        int[] dictionaryOffsets = dictionary.getOffsets();
        int outputLength = 0;
        for (int i = 0; i < length; i++) {
            int id = ids[i];
            int positionLength = dictionaryOffsets[id + 1] - dictionaryOffsets[id];
            outputLength += positionLength;
            outputOffsets[offset + i + 1] = outputOffsets[offset + i] + positionLength;
        }
        byte[] outputChunk = new byte[outputLength];
        int outputIndex = 0;
        for (int i = 0; i < length; i++) {
            int id = ids[i];
            int startIndex = dictionaryOffsets[id];
            int endIndex = dictionaryOffsets[id + 1];
            int positionLength = endIndex - startIndex;
            dictionarySlice.getBytes(startIndex, outputChunk, outputIndex, positionLength);
            outputIndex += positionLength;
        }

        values.addChunk(Slices.wrappedBuffer(outputChunk));
    }

    @Override
    public long getSizeInBytes(BinaryBuffer values)
    {
        return values.getRetainedSize();
    }

    @Override
    public BinaryBuffer merge(List<BinaryBuffer> buffers)
    {
        if (buffers.size() == 0) {
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
}
